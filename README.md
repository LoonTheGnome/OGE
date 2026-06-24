# Materialzeugnis-Extraktion nach Excel fuer Databricks Serverless

Dieses Paket enthaelt ein Databricks-Notebook im Source-Format zur parallelen Auswertung von Materialzeugnis-PDFs.

## Dateien

- `01_materialzeugnis_serverless_excel_export.py`  
  Databricks Notebook Source. Fuehrt die komplette Pipeline aus:
  PDF-Fund, Orientierungserkennung, Vision-Extraktion, Re-Run-Konsolidierung und Excel-Export pro Dokument.

- `03_materialzeugnis_xlsx_export.py`  
  Excel-Export (.xlsx) pro Dokument, dokumentweit ueber alle Laeufe. Zwei getrennte
  Ablagestrukturen: `mit_run_info/<ordnernummer>/<stem>.xlsx` (alle Felder inkl.
  Modellname/Run-IDs) und `ohne_run_info/<ordnernummer>/<stem>.xlsx` (nur fachliche
  Felder). Der Ordnername ist die Nummer des Herkunftsordners unter `Projektdaten/`.

- `04_materialzeugnis_reflatten.py`  
  Berechnet `identifiers` und `datapoints_runs` aus dem vorhandenen
  `parsed_json` aller Laeufe neu (mit korrigierter Confidence-Logik) und baut
  `datapoints` neu auf. Keine Modell-Calls. Damit lassen sich bereits erfasste
  Daten korrigieren, ohne erneut zu extrahieren. Quelle der Wahrheit ist
  `materialzeugnisse_raw_page_extractions.parsed_json` (wird nur gelesen).

- `05_materialzeugnis_parquet_export.py` + `local_xlsx_converter.py`  
  **Empfohlener, OOM-sicherer Weg fuer die xlsx-Erzeugung.** Das Notebook schreibt
  die konsolidierten Tabellen verteilt als Parquet ins Volume (Spark `write`, kein
  Treiber-Speicher -> kann nicht OOMen). Die eigentlichen Excel-Dateien (zwei
  Versionen pro Dokument, nummerierte Ordner, alle Sheets) erzeugt man danach
  **lokal** mit `local_xlsx_converter.py` (Laptop/VS Code, `pip install pandas
  pyarrow xlsxwriter`). Der lokale Lauf ist resumebar (ueberspringt vorhandene
  Dateien). Hintergrund: xlsx fuer 20-31k-Zeilen-Dokumente auf einem kleinen
  Serverless-Treiber zu bauen ist nicht zuverlaessig moeglich; Notebook 03 bleibt
  als reiner Serverless-Versuch erhalten.

- `GENIE_PROMPT.md`  
  Korrigierter Prompt fuer Databricks Genie / Databricks Assistant. Ziel ist nicht Q&A, sondern die Erzeugung eines Export-Notebooks.

## Standardpfade

Input:

```text
/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten/
```

Excel-Export:

```text
/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export/<RUN_ID>/
```

## Modelle

Primaer:

```text
databricks-claude-fable-5
```

Fallback:

```text
databricks-claude-opus-4-8
```

## Orientierung

Gedrehte Seiten werden nicht dem Zufall ueberlassen. Das Notebook erzeugt pro Seite eine 2x2-Montage aus 0/90/180/270 Grad, laesst das Vision-Modell die beste Rotation bestimmen und rendert danach die Seite in dieser Rotation fuer die eigentliche Extraktion.

## Konfidenz und Re-Runs

Jeder Wert erhaelt:

- `confidence_model`
- `confidence_rule_adjustment`
- `confidence_final_pre_verification`
- `confidence_final`
- `verification_status`
- `needs_human_review`

Re-Runs werden in `materialzeugnisse_datapoints_runs` historisiert. Die aktuelle konsolidierte Sicht steht in `materialzeugnisse_datapoints`.

## Re-Run-Modi (Fehlerseiten erneut extrahieren)

Der Lauf wird ueber das Widget `RUN_MODE` gesteuert:

| RUN_MODE | Wirkung |
|----------|---------|
| `full` (Default) | Erstlauf: alle Seiten frisch extrahieren. |
| `retry_errors` | NUR die Seiten erneut verarbeiten, die im Quelllauf keinen `status='ok'` haben (Timeouts, unleserliche Scans, abgebrochene Seiten). Genau das Szenario "57 Errors nachziehen". |
| `reverify` | ALLE Seiten der Dokumente des Quelllaufs erneut lesen, um per Re-Run-Abgleich die Confidence anzuheben bzw. Konflikte aufzudecken. |

Weitere Widgets:

- `SOURCE_RUN_ID` — der Lauf, auf den sich `retry_errors` / `reverify` bezieht. Leer lassen = automatisch der letzte Lauf.
- `RUN_ID` — leer lassen; jeder Re-Run erhaelt automatisch eine **neue** `RUN_ID`, damit die Historisierung (und damit die Confidence-Anhebung) funktioniert. Eine Kollision mit `SOURCE_RUN_ID` wird automatisch verhindert.

### Ablauf fuer die Fehlerseiten

1. Notebook mit `RUN_MODE = retry_errors` starten (`SOURCE_RUN_ID` leer = letzter Lauf).
2. Es werden ausschliesslich die Seiten ohne erfolgreiche Extraktion erneut verarbeitet.
3. Neue Datenpunkte fliessen in `materialzeugnisse_datapoints_runs`; die Konsolidierung baut `materialzeugnisse_datapoints` neu auf und integriert die korrigierten Werte.
4. Die "Re-Run-Bilanz" im Notebook zeigt, wie viele zuvor fehlgeschlagene Seiten jetzt erfolgreich gelesen wurden.
5. Optional anschliessend `RUN_MODE = reverify`, damit gleiche Werte aus zwei Erfassungen als `confirmed_by_rerun` markiert und die `confidence_final` (bis max. 0.99) angehoben werden.

Hinweis: Bei einem Restart desselben Laufs gelten nur Seiten mit `status='ok'` als erledigt; Fehlerseiten werden automatisch erneut versucht.

## Modellwahl und fehlende Bilder

Weitere Widgets:

- `FORCE_MODEL` — wenn gesetzt (z. B. `databricks-claude-opus-4-8`), wird **ausschliesslich** dieses Modell genutzt, kein Fallback. Leer = Kaskade: pro Seite zuerst Opus, nur bei echtem Fehler Sonnet. Ein einmal flaky Verfuegbarkeits-Test legt den Lauf damit nicht mehr dauerhaft auf den Fallback fest.
- `RENDER_MISSING_IMAGES` (`true`/`false`) — fehlt ein vorgerendertes Seitenbild im Volume, wird die Seite bei Bedarf aus dem PDF gerendert (PyMuPDF) und als PNG ins Volume geschrieben, sodass Folgelaeufe es als Cache finden.

Robustheit gegen `400 Bad Request` (zu grosse Bilder): Seitenbilder werden vor dem Senden auf `MAX_IMAGE_SIDE_PX` herunterskaliert und als JPEG kodiert; bei einem groessenbedingten 400 verkleinert der Aufruf das Bild zusaetzlich und versucht es erneut.

## Tuning

Startwerte:

```python
THREADS_PER_PARTITION = 6
MAX_SPARK_PARTITIONS = 512
DPI = 230
MAX_IMAGE_SIDE_PX = 2200
```

Bei Rate Limits zuerst reduzieren:

```python
THREADS_PER_PARTITION = 3
MAX_SPARK_PARTITIONS = 256
```

Bei stabiler Verarbeitung ohne Rate Limits kann erhoeht werden:

```python
THREADS_PER_PARTITION = 8
MAX_SPARK_PARTITIONS = 768
```
