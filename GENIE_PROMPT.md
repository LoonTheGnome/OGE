# Prompt fuer Databricks Genie / Databricks Assistant

Erstelle ein Databricks Python Notebook fuer Serverless, das Materialzeugnis-PDFs auswertet und pro PDF-Dokument eine Excel-Datei exportiert.

## Datenquelle

Die PDF-Dateien liegen rekursiv in nummerierten Unterordnern unter:

```text
/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten/
```

Es werden 52 PDF-Dateien erwartet.

## Ziel

Nicht nur Fragen beantworten, sondern einen reproduzierbaren Export erzeugen:

1. Alle PDFs rekursiv finden.
2. Jede PDF-Seite als eigene serverless-parallele Arbeitseinheit verarbeiten.
3. Seiten mit anderer Orientierung erkennen und vor der Extraktion richtig drehen.
4. Schlechte Scanqualitaet beruecksichtigen.
5. Materialzeugnisdaten je Wert extrahieren.
6. Jeden Wert mit Herkunft und Konfidenz speichern.
7. Re-Runs als Historie speichern und Werte konsolidiert verifizieren.
8. Pro PDF-Dokument eine Excel-Datei mit strukturierten Listen exportieren.

## Modelle

Nutze als Primaermodell:

```text
databricks-claude-fable-5
```

Falls nicht verfuegbar oder nicht bildfaehig, verwende automatisch:

```text
databricks-claude-opus-4-8
```

## Serverless-Parallelisierung

Implementiere maximale praktikable Serverless-Parallelisierung ohne RDD-APIs:

- Nutze Spark DataFrames.
- Nutze `mapInPandas` fuer seitenweise Verarbeitung.
- Partitioniere auf Seitenebene, z.B. nach `page_id`, nicht nur nach Dokument.
- Nutze pro Spark-Partition einen `ThreadPoolExecutor`, um mehrere Model-Serving-Requests parallel abzusetzen.
- Parameter:
  - `THREADS_PER_PARTITION = 6`
  - `MAX_SPARK_PARTITIONS = 512`
  - `TARGET_RECORDS_PER_PARTITION = THREADS_PER_PARTITION`
- Bei 429/Rate-Limits muessen die Werte reduzierbar sein.

## Orientierung

Das Modell kann gedrehte Seiten oft lesen, aber bei schlechten Scans und Tabellenwerten soll das Notebook nicht darauf vertrauen.

Implementiere deshalb vor der Extraktion:

1. Pro Seite eine kleine 2x2-Montage aus vier Rotationen erzeugen:
   - 0 Grad
   - 90 Grad
   - 180 Grad
   - 270 Grad
2. Das Vision-Modell waehlt die beste Rotation als JSON:
   - `best_rotation_degrees`
   - `confidence`
   - `reason`
3. Danach wird die Seite hochaufloesend mit dieser Rotation gerendert und extrahiert.
4. Die Rotationsentscheidung wird pro Seite gespeichert.

## Extraktion

Jede Seite als hochaufloesendes JPEG rendern, ca. 230 DPI, maximale Kantenlaenge ca. 2200 px.

Optional darf die Vorseite als Kontextbild mitgegeben werden. Diese Vorseite darf nur fuer Identifier-Kontext verwendet werden; Messwerte duerfen ausschliesslich aus der aktuellen Seite extrahiert werden.

Extrahiere insbesondere:

- Allgemeine Produkt-/Bauteildaten:
  - Bauteilart
  - Rohr, Bogen, Flansch, Fitting
  - Dimension
  - Aussendurchmesser
  - Wanddicke
  - Laenge
  - Werkstoff
  - Norm
  - Hersteller
  - Besteller
  - Projekt
- Chemische Analyse:
  - C, Si, Mn, P, S, Cr, Ni, Mo, Cu, Al, Nb, V, Ti, N, B, CEV, PCM usw.
- Zugversuche:
  - Rm, Rp0.2, ReH, ReL, A, A5, Z usw.
- Kerbschlag:
  - KV, KV2, Charpy V
  - Temperatur
  - Einzelwerte
  - Mittelwert
- Haerte:
  - HV, HB, HRC
  - Positionen
  - Einzelwerte
  - Mittelwerte
- Waermebehandlung
- Dimensionen
- NDE:
  - UT, RT, MT, PT, VT
- Druckpruefung
- Konformitaet / Abnahme

## Identifier-Logik

Jeder Messwert muss soweit moeglich den passenden Identifiern zugeordnet werden.

Besonders wichtige Identifier:

- Rohrnummer / pipe number / tube number
- Schmelzennummer / Heat No. / Melt No. / Cast No.
- Chargennummer / Charge No. / Batch No. / Lot No.
- Zeugnisnummer / certificate number
- Bauteilnummer / item number / position number
- Werkstoffnummer / material number
- Proben- und Specimen-Nummern
- Coils, plates, heats, samples, specimens, test pieces

Wenn ein Identifier nur von der Vorseite ableitbar ist:

- `scope = inferred_from_previous_page`
- `source_page_number` der Vorseite setzen
- Konfidenz reduzieren
- `uncertainty_note` fuellen

## Herkunft je Datenpunkt

Jeder Datenpunkt muss enthalten:

- `pdf_path`
- `file_name`
- `page_number`
- `evidence_text`
- optional `source_page_number` fuer uebernommene Identifier

## Konfidenz je Wert

Jeder einzelne Wert braucht eine eigene Konfidenz.

Speichere mindestens:

- `confidence_model`: direkte Modellsicherheit fuer diesen Wert
- `confidence_rule_adjustment`: regelbasierte Anpassung, z.B. wegen schlechter Seite, fehlendem Identifier, uebernommenem Identifier oder Unsicherheit
- `confidence_final_pre_verification`: Konfidenz vor Re-Run-Abgleich
- `confidence_final`: finale Konfidenz nach Re-Run-Abgleich
- `needs_human_review`: Boolean
- `verification_status`

## Re-Run-Logik

Bei einem Re-Run duerfen Werte nicht einfach ueberschrieben werden.

Implementiere:

1. Eine Historientabelle fuer alle Run-Ergebnisse:
   - `materialzeugnisse_datapoints_runs`
2. Eine konsolidierte Current-Tabelle:
   - `materialzeugnisse_datapoints`
3. Ein stabiler Wertslot:
   - `stable_datapoint_key`
4. Ein Wert-Fingerprint:
   - `value_fingerprint`

Wenn derselbe `stable_datapoint_key` in einem Re-Run denselben `value_fingerprint` liefert:

- `verification_status = confirmed_by_rerun`
- `confidence_final` erhoehen, maximal 0.99

Wenn derselbe `stable_datapoint_key` in einem Re-Run einen anderen `value_fingerprint` liefert:

- `verification_status = changed_or_conflicting`
- `confidence_final` reduzieren
- `needs_human_review = true`
- Varianten im Feld `value_variants_json` dokumentieren

Wenn ein Wertslot erstmals auftaucht:

- `verification_status = new`

## Delta-Tabellen

Lege diese Tabellen bzw. Views an:

```text
playground.u_daniel_bick.materialzeugnisse_page_manifest
playground.u_daniel_bick.materialzeugnisse_raw_page_extractions
playground.u_daniel_bick.materialzeugnisse_identifiers
playground.u_daniel_bick.materialzeugnisse_datapoints_runs
playground.u_daniel_bick.materialzeugnisse_datapoints
playground.u_daniel_bick.materialzeugnisse_excel_exports
playground.u_daniel_bick.materialzeugnisse_quality_checks
```

## Excel-Export

Exportiere pro PDF-Dokument genau eine `.xlsx`-Datei in:

```text
/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export/<RUN_ID>/
```

Der Dateiname soll den PDF-Namen und die `document_id` enthalten.

Jede Excel-Datei soll mindestens diese Sheets enthalten:

1. `00_Overview`
   - run_id
   - document_id
   - file_name
   - pdf_path
   - Anzahl extrahierter Datenpunkte
   - Anzahl Review-Zeilen
   - Anzahl Seiten

2. `01_All_Datapoints`
   - alle konsolidierten Werte des Dokuments

3. `02_Review`
   - alle Werte mit:
     - `confidence_final < 0.7`
     - `needs_human_review = true`
     - `verification_status = changed_or_conflicting`

4. `03_Identifiers`
   - alle erkannten Identifier des Dokuments

5. `04_Page_Status`
   - Seitenstatus
   - Rotationsentscheidung
   - Fehler
   - Seitenqualitaet

6. Fachliche Sheets:
   - `10_Chemistry`
   - `11_Tensile`
   - `12_Impact`
   - `13_Hardness`
   - `14_Dimensions`
   - `15_Heat_Treatment`
   - `16_NDE`
   - `17_Pressure`
   - `18_Product_Info`
   - `19_Certificate`
   - `20_Compliance`
   - `21_Other`

Fuer Chemistry, Tensile, Impact und Hardness optional zusaetzlich Wide-Sheets erzeugen, bei denen `property_name` als Spalte gepivotet wird.

Alle Excel-Sheets sollen Filter, Freeze-Panes und sinnvolle Spaltenbreiten erhalten.

## Quality Checks

Die View `materialzeugnisse_quality_checks` soll mindestens ausweisen:

- fehlgeschlagene Seiten im aktuellen Run
- Datenpunkte ohne Identifier
- Datenpunkte mit niedriger Konfidenz
- Datenpunkte mit Re-Run-Konflikt
- Datenpunkte mit Review-Flag
- erfolgreich verarbeitete Seiten ohne extrahierte Datenpunkte

## Wichtig

Keine Werte erfinden.
Jede Zahl nur extrahieren, wenn sie sichtbar ist.
Jeder Datenpunkt muss eindeutig auf PDF und Seite zurueckfuehrbar sein.
Das Ergebnis des Notebooks sind Delta-Tabellen und Excel-Dateien pro PDF-Dokument.
