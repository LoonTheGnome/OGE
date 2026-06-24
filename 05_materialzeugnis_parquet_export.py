# Databricks notebook source
# DBTITLE 1,Parquet-Export (Daten raus, xlsx lokal bauen)
# MAGIC %md
# MAGIC # Parquet-Export der konsolidierten Daten
# MAGIC
# MAGIC Schreibt die fuer den Excel-Export noetigen Tabellen als **Parquet** ins Volume.
# MAGIC Das laeuft verteilt auf den Executors (Spark `write`), braucht **keinen
# MAGIC Treiber-Speicher** und kann daher **nicht OOMen** - im Gegensatz zur
# MAGIC xlsx-Erzeugung auf dem Treiber.
# MAGIC
# MAGIC Die eigentliche xlsx-Erzeugung (zwei Versionen pro Dokument, nummerierte
# MAGIC Ordner, alle Sheets) passiert anschliessend **lokal** mit
# MAGIC `local_xlsx_converter.py` auf einem Rechner mit genug RAM (z.B. Laptop).
# MAGIC
# MAGIC Ablauf:
# MAGIC 1. (einmalig) `04_materialzeugnis_reflatten` ausfuehren, damit die Confidence
# MAGIC    in `materialzeugnisse_datapoints` korrekt ist.
# MAGIC 2. Dieses Notebook ausfuehren -> Parquet liegt unter `PARQUET_ROOT`.
# MAGIC 3. Den Parquet-Ordner herunterladen und lokal `local_xlsx_converter.py` laufen lassen.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

CATALOG = "playground"
SCHEMA = "u_daniel_bick"

DATAPOINT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints"
IDENTIFIER_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_identifiers"
RAW_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_raw_page_extractions"

DEFAULT_PARQUET_ROOT = "/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export/_export_parquet"
try:
    dbutils.widgets.text("PARQUET_ROOT", DEFAULT_PARQUET_ROOT)
    PARQUET_ROOT = dbutils.widgets.get("PARQUET_ROOT").strip() or DEFAULT_PARQUET_ROOT
except Exception:
    PARQUET_ROOT = DEFAULT_PARQUET_ROOT

print(f"PARQUET_ROOT: {PARQUET_ROOT}", flush=True)

# COMMAND ----------

# DBTITLE 1,Vorbedingungen pruefen
if not spark.catalog.tableExists(DATAPOINT_TABLE):
    raise RuntimeError(f"{DATAPOINT_TABLE} existiert nicht. Zuerst Extraktion bzw. 04_reflatten ausfuehren.")

dp_count = spark.table(DATAPOINT_TABLE).count()
print(f"Zeilen in {DATAPOINT_TABLE}: {dp_count}", flush=True)
if dp_count == 0:
    raise RuntimeError(f"{DATAPOINT_TABLE} ist LEER. Zuerst 04_reflatten (mit OK-Seiten) ausfuehren.")

# Kurzer Confidence-Check, damit klar ist ob 04 gelaufen ist:
display(
    spark.table(DATAPOINT_TABLE)
    .agg(
        F.round(F.avg("confidence_final"), 3).alias("avg_confidence_final"),
        F.round(F.avg("confidence_rule_adjustment"), 3).alias("avg_rule_adjustment"),
        F.sum(F.col("needs_human_review").cast("int")).alias("needs_review"),
    )
)

# COMMAND ----------

# DBTITLE 1,Datenpunkte -> Parquet (alle Felder)
(
    spark.table(DATAPOINT_TABLE)
    .write.mode("overwrite")
    .parquet(f"{PARQUET_ROOT}/datapoints")
)
print(f"OK: {PARQUET_ROOT}/datapoints", flush=True)

# COMMAND ----------

# DBTITLE 1,Identifier -> Parquet (dokumentweit dedupliziert)
(
    spark.table(IDENTIFIER_TABLE)
    .dropDuplicates(["document_id", "page_number", "identifier_type", "identifier_value"])
    .write.mode("overwrite")
    .parquet(f"{PARQUET_ROOT}/identifiers")
)
print(f"OK: {PARQUET_ROOT}/identifiers", flush=True)

# COMMAND ----------

# DBTITLE 1,Seitenstatus -> Parquet (aktueller Status je Seite ueber alle Laeufe)
_page_w = Window.partitionBy("page_id").orderBy(
    F.when(F.col("status") == "ok", F.lit(1)).otherwise(F.lit(0)).desc(),
    F.col("created_at_utc").desc(),
)
(
    spark.table(RAW_TABLE)
    .select("document_id", "page_id", "page_number", "file_name", "status",
            "run_id", "model_name", "duration_s", "error_message", "created_at_utc")
    .withColumn("_rn", F.row_number().over(_page_w))
    .where(F.col("_rn") == 1)
    .drop("_rn")
    .write.mode("overwrite")
    .parquet(f"{PARQUET_ROOT}/page_status")
)
print(f"OK: {PARQUET_ROOT}/page_status", flush=True)

# COMMAND ----------

# DBTITLE 1,Fertig
print("=" * 60)
print("Parquet-Export abgeschlossen.")
print(f"Verzeichnis: {PARQUET_ROOT}")
print("  - datapoints/   (alle Felder der konsolidierten Tabelle)")
print("  - identifiers/")
print("  - page_status/")
print()
print("Naechster Schritt LOKAL (Laptop, VS Code):")
print("  1. Ordner herunterladen (Databricks UI/CLI).")
print("  2. pip install pandas pyarrow xlsxwriter")
print("  3. python local_xlsx_converter.py --input <ordner> --output ./xlsx")
