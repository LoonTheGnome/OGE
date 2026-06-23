# Databricks notebook source
# DBTITLE 1,Materialzeugnis Excel-Export
# MAGIC %md
# MAGIC # Materialzeugnis CSV-Export
# MAGIC
# MAGIC Liest die extrahierten Daten aus den Delta-Tabellen und exportiert pro PDF-Dokument einen Ordner mit CSV-Dateien (eine pro Sheet).
# MAGIC
# MAGIC Keine externen Pakete noetig – laeuft stabil auf Serverless.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# Kein pip install noetig - nur pandas + pyspark (vorinstalliert)
print("Keine externen Pakete benoetigt.")

# COMMAND ----------

# DBTITLE 1,Imports und Parameter
from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

CATALOG = "playground"
SCHEMA = "u_daniel_bick"

DEFAULT_EXPORT_ROOT_PATH = "/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export"
CONFIDENCE_THRESHOLD_REVIEW = 0.7

PAGE_MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_page_manifest"
RAW_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_raw_page_extractions"
IDENTIFIER_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_identifiers"
DATAPOINT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints"
EXPORT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_excel_exports"

# Widgets
try:
    dbutils.widgets.text("EXPORT_ROOT_PATH", DEFAULT_EXPORT_ROOT_PATH)
    dbutils.widgets.text("RUN_ID", "")
    EXPORT_ROOT_PATH = dbutils.widgets.get("EXPORT_ROOT_PATH").strip() or DEFAULT_EXPORT_ROOT_PATH
    RUN_ID = dbutils.widgets.get("RUN_ID").strip()
except Exception:
    EXPORT_ROOT_PATH = DEFAULT_EXPORT_ROOT_PATH
    RUN_ID = ""

# Wenn keine RUN_ID angegeben: letzte RUN_ID aus dem Manifest nehmen
if not RUN_ID:
    latest = spark.table(PAGE_MANIFEST_TABLE).agg(F.max("run_id").alias("r")).collect()
    RUN_ID = latest[0]["r"] if latest and latest[0]["r"] else ""

if not RUN_ID:
    raise RuntimeError("Keine RUN_ID gefunden. Bitte zuerst die Extraktion ausfuehren.")

print(f"RUN_ID: {RUN_ID}")
print(f"EXPORT_ROOT_PATH: {EXPORT_ROOT_PATH}")

# COMMAND ----------

# DBTITLE 1,Hilfsfunktionen
def utcnow_naive() -> datetime:
    return datetime.utcnow()


def sanitize_filename(name: str) -> str:
    return re.sub(r'[^\w\-. ()]+', '_', name).strip('_')[:100]


def reorder_columns(pdf_df: pd.DataFrame, preferred: List[str]) -> pd.DataFrame:
    if pdf_df is None or pdf_df.empty:
        return pdf_df
    cols = [c for c in preferred if c in pdf_df.columns] + [c for c in pdf_df.columns if c not in preferred]
    return pdf_df[cols]


def make_wide_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    index_cols = [
        c for c in [
            "file_name", "page_number", "identifier_keys", "record_type",
            "section", "table_name", "specimen_or_sample", "orientation",
            "test_temperature_c", "verification_status",
        ]
        if c in df.columns
    ]

    if "property_name" not in df.columns or "value_raw" not in df.columns:
        return pd.DataFrame()

    tmp = df.copy()
    tmp["property_name"] = tmp["property_name"].fillna("unknown_property").astype(str)

    try:
        wide = (
            tmp.pivot_table(
                index=index_cols,
                columns="property_name",
                values="value_raw",
                aggfunc=lambda x: " | ".join([str(v) for v in x if pd.notna(v)]),
                dropna=False,
            )
            .reset_index()
        )
        wide.columns = [str(c) for c in wide.columns]
        return wide
    except Exception:
        return pd.DataFrame()


def save_csv(df: pd.DataFrame, path: str) -> None:
    """Speichert DataFrame als CSV mit UTF-8-BOM (Excel-kompatibel)."""
    df.to_csv(path, index=False, encoding="utf-8-sig")


def append_or_create_delta(df, table_name: str) -> None:
    try:
        table_exists = spark.catalog.tableExists(table_name)
    except Exception:
        table_exists = False
    if table_exists:
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(table_name)
    else:
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# COMMAND ----------

# DBTITLE 1,Excel-Export pro Dokument
preferred_dp_cols = [
    "file_name", "page_number", "record_type", "identifier_keys",
    "property_name", "value_slot_key", "value_raw", "value_num", "unit",
    "operator", "limit_min_raw", "limit_max_raw", "result_or_conformity",
    "confidence_final", "confidence_model", "verification_status",
    "needs_human_review", "section", "table_name", "specimen_or_sample",
    "orientation", "test_temperature_c", "test_standard", "evidence_text",
    "uncertainty_note", "pdf_path", "stable_datapoint_key",
    "value_fingerprint", "seen_run_ids", "value_variants_json",
]


def write_document_csv(
    document_id: str,
    file_name: str,
    pdf_path: str,
    export_dir: str,
) -> Dict[str, Any]:
    """Exportiert ein Dokument als CSV-Ordner (eine CSV pro Sheet)."""
    doc_folder = Path(export_dir) / f"{sanitize_filename(Path(file_name).stem)}__{document_id}"
    doc_folder.mkdir(parents=True, exist_ok=True)

    dp_pdf = (
        spark.table(DATAPOINT_TABLE)
        .where(F.col("document_id") == document_id)
        .orderBy("page_number", "record_type", "identifier_keys", "property_name")
        .toPandas()
    )

    # Identifier dokumentweit ueber ALLE Laeufe (dedupliziert), damit die Liste
    # nach einem Re-Run einzelner Seiten vollstaendig bleibt.
    id_pdf = (
        spark.table(IDENTIFIER_TABLE)
        .where(F.col("document_id") == document_id)
        .dropDuplicates(["page_number", "identifier_type", "identifier_value"])
        .orderBy("page_number", "identifier_type", "identifier_value")
        .toPandas()
    )

    # Seitenstatus: pro Seite der aktuelle (beste) Status ueber alle Laeufe.
    _page_w = Window.partitionBy("page_id").orderBy(
        F.when(F.col("status") == "ok", F.lit(1)).otherwise(F.lit(0)).desc(),
        F.col("created_at_utc").desc(),
    )
    raw_pdf = (
        spark.table(RAW_TABLE)
        .where(F.col("document_id") == document_id)
        .withColumn("_rn", F.row_number().over(_page_w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .orderBy("page_number")
        .toPandas()
    )

    review_pdf = pd.DataFrame()
    if not dp_pdf.empty:
        review_pdf = dp_pdf[
            (dp_pdf.get("needs_human_review", False) == True)
            | (dp_pdf.get("confidence_final", 1.0).fillna(0.0) < CONFIDENCE_THRESHOLD_REVIEW)
            | (dp_pdf.get("verification_status", "") == "changed_or_conflicting")
        ].copy()

    overview = pd.DataFrame([
        {"field": "run_id", "value": RUN_ID},
        {"field": "document_id", "value": document_id},
        {"field": "file_name", "value": file_name},
        {"field": "pdf_path", "value": pdf_path},
        {"field": "export_created_at_utc", "value": utcnow_naive().isoformat()},
        {"field": "datapoint_rows_current", "value": int(len(dp_pdf))},
        {"field": "review_rows", "value": int(len(review_pdf))},
        {"field": "identifier_rows_this_run", "value": int(len(id_pdf))},
        {"field": "page_rows_this_run", "value": int(len(raw_pdf))},
    ])

    dp_pdf = reorder_columns(dp_pdf, preferred_dp_cols)
    review_pdf = reorder_columns(review_pdf, preferred_dp_cols)

    # CSVs schreiben
    save_csv(overview, str(doc_folder / "00_Overview.csv"))
    save_csv(dp_pdf, str(doc_folder / "01_All_Datapoints.csv"))
    save_csv(review_pdf, str(doc_folder / "02_Review.csv"))
    save_csv(id_pdf, str(doc_folder / "03_Identifiers.csv"))
    save_csv(raw_pdf, str(doc_folder / "04_Page_Status.csv"))

    record_sheets = [
        ("chemical_composition", "10_Chemistry"),
        ("tensile_test", "11_Tensile"),
        ("impact_test", "12_Impact"),
        ("hardness_test", "13_Hardness"),
        ("dimensional_check", "14_Dimensions"),
        ("heat_treatment", "15_Heat_Treatment"),
        ("nde", "16_NDE"),
        ("pressure_test", "17_Pressure"),
        ("product_info", "18_Product_Info"),
        ("certificate_metadata", "19_Certificate"),
        ("compliance", "20_Compliance"),
        ("other", "21_Other"),
    ]

    for record_type, sname in record_sheets:
        sub = dp_pdf[dp_pdf["record_type"] == record_type].copy() if not dp_pdf.empty and "record_type" in dp_pdf.columns else pd.DataFrame()
        if not sub.empty:
            save_csv(sub, str(doc_folder / f"{sname}.csv"))
            wide = make_wide_sheet(sub)
            if not wide.empty and record_type in {"chemical_composition", "tensile_test", "impact_test", "hardness_test"}:
                save_csv(wide, str(doc_folder / f"{sname}_Wide.csv"))

    return {
        "run_id": RUN_ID,
        "document_id": document_id,
        "pdf_path": pdf_path,
        "file_name": file_name,
        "export_path": str(doc_folder),
        "datapoint_rows": int(len(dp_pdf)),
        "review_rows": int(len(review_pdf)),
        "identifier_rows": int(len(id_pdf)),
        "page_rows": int(len(raw_pdf)),
        "created_at_utc": utcnow_naive(),
    }


# --- Export ausfuehren ---
export_dir = str(Path(EXPORT_ROOT_PATH) / RUN_ID)
documents_pdf = (
    spark.table(PAGE_MANIFEST_TABLE)
    .where(F.col("run_id") == RUN_ID)
    .select("document_id", "pdf_path", "file_name")
    .distinct()
    .orderBy("file_name")
    .toPandas()
)

print(f"Dokumente zu exportieren: {len(documents_pdf)}")

export_rows = []
for rec in documents_pdf.to_dict("records"):
    try:
        result = write_document_csv(
            document_id=rec["document_id"],
            file_name=rec["file_name"],
            pdf_path=rec["pdf_path"],
            export_dir=export_dir,
        )
        export_rows.append(result)
        print(f"  OK: {rec['file_name']} -> {result['export_path']}")
    except Exception as exc:
        print(f"  FEHLER: {rec['file_name']}: {repr(exc)}")

if export_rows:
    export_df = spark.createDataFrame(export_rows)
    append_or_create_delta(export_df, EXPORT_TABLE)
    display(spark.table(EXPORT_TABLE).where(F.col("run_id") == RUN_ID).orderBy("file_name"))

print(f"\nCSV-Exportverzeichnis: {export_dir}")

# COMMAND ----------

# DBTITLE 1,Master-Index
# Master-Index CSV
index_pdf = (
    spark.table(EXPORT_TABLE)
    .where(F.col("run_id") == RUN_ID)
    .orderBy("file_name")
    .toPandas()
)

index_path = str(Path(export_dir) / f"00_MASTER_INDEX__{RUN_ID}.csv")
save_csv(index_pdf, index_path)

print(f"Master-Index: {index_path}")
print(f"Gesamt exportierte Dokumente: {len(export_rows)}")

# COMMAND ----------

