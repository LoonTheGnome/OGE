# Projektkontext fuer Claude

Materialzeugnis-Extraktion (PDFs/Bilder -> Delta-Tabellen -> Excel) auf Databricks.

## Umgebung / Setup (wichtig fuer Loesungsentscheidungen)

### Databricks
- **Nur Serverless Compute.** Der Nutzer hat **keine Berechtigung, Cluster zu
  provisionieren/buchen** (kein Classic-/Single-Node-Cluster, keine
  Cluster-Libraries/Init-Scripts).
- Daraus folgende Einschraenkungen, die schon Probleme verursacht haben:
  - `.cache()` / `.persist()` sind **nicht erlaubt** (`NOT_SUPPORTED_WITH_SERVERLESS`).
  - Der Treiber ist **klein und nicht einstellbar** -> schwere Single-Driver-Arbeit
    (z.B. xlsx grosser Tabellen via openpyxl/`toPandas`) laeuft kumulativ in **OOM**.
  - `%pip install ...` braucht anschliessend `dbutils.library.restartPython()`,
    sonst ist das Paket im laufenden Prozess nicht importierbar.
- **Konsequenz / bevorzugtes Muster:** Auf Databricks nur **Daten exportieren**
  (Parquet/CSV via `spark.write`, laeuft verteilt, kein Treiber-RAM, kann nicht
  OOMen). Speicherintensive Weiterverarbeitung (xlsx-Erzeugung) **lokal** machen.

### Lokaler Rechner (fuer Post-Processing)
- Laptop: **Intel Core Ultra 5 235H, 14 Kerne, 64 GB RAM**, Intel Arc 140T GPU + NPU.
- GPU/NPU sind fuer pandas/xlsx **irrelevant** (reine CPU-Arbeit) - nicht einplanen.
- Genug RAM/Kerne fuer parallele lokale Konvertierung (siehe `local_xlsx_converter.py`,
  `--workers` Default = Kerne - 2).

## Sonstiges
- Kommunikation auf **Deutsch**.
- Entwicklung/Push auf den vom Task vorgegebenen Branch.

## Notebooks / Skripte (Kurzueberblick)
- `01_materialzeugnis_serverless_excel_export.py` — Extraktion (Vision-Modell),
  Re-Run-Modi (`full|retry_errors|reverify`), Konsolidierung, Confidence.
- `04_materialzeugnis_reflatten.py` — rechnet Confidence aus vorhandenem
  `parsed_json` neu (ohne Modell-Calls); muss laufen, bevor exportiert wird.
- `05_materialzeugnis_parquet_export.py` (+ `local_xlsx_converter.py`) —
  **empfohlener xlsx-Weg**: Parquet auf Databricks raus, xlsx lokal bauen.
- `02_/03_*` — aeltere/serverless-Exportversuche (03 OOMt bei grossen Dokumenten).
