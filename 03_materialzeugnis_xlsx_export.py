# Databricks notebook source
# DBTITLE 1,Materialzeugnis Excel-Export (.xlsx, zwei Versionen)
# MAGIC %md
# MAGIC # Materialzeugnis Excel-Export (.xlsx)
# MAGIC
# MAGIC Exportiert die konsolidierten Daten pro PDF-Dokument als **echte Excel-Datei**.
# MAGIC
# MAGIC - Zwei getrennte Ablagestrukturen (je Version ein eigener Baum):
# MAGIC   - `<ROOT>/mit_run_info/<ordnernummer>/<stem>.xlsx`  -> **mit** Run-Infos
# MAGIC     (Modellname, Run-IDs, Confidence-Zwischenwerte, interne Keys)
# MAGIC   - `<ROOT>/ohne_run_info/<ordnernummer>/<stem>.xlsx` -> **ohne** Run-Infos
# MAGIC     (nur die fachlichen Ergebnisfelder)
# MAGIC - In beiden Baeumen gibt es pro Dokument einen Unterordner, benannt mit der
# MAGIC   **Nummer des Herkunftsordners** (der nummerierte Unterordner unter `Projektdaten/`).
# MAGIC - Es werden ALLE Felder der konsolidierten Tabelle uebernommen.
# MAGIC
# MAGIC Liest aus den Delta-Tabellen der Extraktions-Pipeline; dokumentweit ueber alle Laeufe.

# COMMAND ----------

# MAGIC %pip install xlsxwriter --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Imports und Parameter
from __future__ import annotations

import gc
import os
import re
import shutil
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
try:
    import xlsxwriter
except ImportError:
    # Fallback, falls der %pip-Install im laufenden Prozess noch nicht greift.
    import importlib
    import subprocess
    import sys as _sys
    subprocess.check_call([_sys.executable, "-m", "pip", "install", "xlsxwriter", "--quiet"])
    importlib.invalidate_caches()
    import xlsxwriter
from pyspark.sql import Window
from pyspark.sql import functions as F

# Frueh und LAUT scheitern, wenn der xlsx-Writer fehlt - statt spaeter 53x kryptisch.
print(f"xlsxwriter Version: {xlsxwriter.__version__}", flush=True)

CATALOG = "playground"
SCHEMA = "u_daniel_bick"

DEFAULT_XLSX_EXPORT_ROOT = "/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export/xlsx"
CONFIDENCE_THRESHOLD_REVIEW = 0.70

# Getrennte Ablagestruktur je Version: jeder Baum enthaelt die nummerierten
# Dokument-Unterordner.
#   <ROOT>/mit_run_info/<ordnernummer>/<stem>.xlsx
#   <ROOT>/ohne_run_info/<ordnernummer>/<stem>.xlsx
FULL_SUBDIR = "mit_run_info"
CLEAN_SUBDIR = "ohne_run_info"

DATAPOINT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints"
IDENTIFIER_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_identifiers"
RAW_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_raw_page_extractions"
PAGE_MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_page_manifest"

# Widgets
try:
    dbutils.widgets.text("XLSX_EXPORT_ROOT", DEFAULT_XLSX_EXPORT_ROOT)
    # Leer = ALLE Dokumente der konsolidierten Tabelle exportieren.
    # Gesetzt = nur Dokumente, die in diesem Run verarbeitet wurden.
    dbutils.widgets.text("RUN_ID", "")
    XLSX_EXPORT_ROOT = dbutils.widgets.get("XLSX_EXPORT_ROOT").strip() or DEFAULT_XLSX_EXPORT_ROOT
    RUN_ID_FILTER = dbutils.widgets.get("RUN_ID").strip()
except Exception:
    XLSX_EXPORT_ROOT = DEFAULT_XLSX_EXPORT_ROOT
    RUN_ID_FILTER = ""

print(f"XLSX_EXPORT_ROOT: {XLSX_EXPORT_ROOT}")
print(f"RUN_ID-Filter:    {RUN_ID_FILTER or '(alle Dokumente)'}")

# COMMAND ----------

# DBTITLE 1,Spalten- und Sheet-Definitionen
# Spalten, die NUR in der "full"-Version erscheinen (Run-/Technik-Metadaten).
# Sie werden in der "clean"-Version weggelassen.
RUN_META_COLUMNS = {
    "confidence_model",
    "confidence_rule_adjustment",
    "confidence_final_pre_verification",
    "model_name",
    "orientation_rotation_degrees",
    "last_seen_run_id",
    "first_seen_run_id",
    "seen_run_ids",
    "extraction_count",
    "run_count",
    "distinct_value_count",
    "first_seen_at_utc",
    "last_seen_at_utc",
    "document_id",
    "page_id",
    "datapoint_id",
    "stable_datapoint_key",
    "value_fingerprint",
    "value_slot_key",
    "identifiers_json",
    "value_variants_json",
}

# Bevorzugte Spaltenreihenfolge: fachliche Felder zuerst, Technik hinten.
PREFERRED_DP_COLS = [
    "file_name", "page_number", "record_type", "section", "table_name",
    "identifier_keys", "group_id", "specimen_or_sample", "orientation",
    "test_temperature_c", "property_name", "property_label_raw",
    "value_raw", "value_num", "unit", "operator",
    "limit_min_raw", "limit_min_num", "limit_max_raw", "limit_max_num",
    "result_or_conformity", "test_standard",
    "confidence_final", "verification_status", "needs_human_review",
    "evidence_text", "uncertainty_note", "pdf_path",
    # ab hier Run-/Technik-Felder (nur full):
    "confidence_model", "confidence_rule_adjustment", "confidence_final_pre_verification",
    "model_name", "orientation_rotation_degrees",
    "last_seen_run_id", "first_seen_run_id", "seen_run_ids",
    "extraction_count", "run_count", "distinct_value_count",
    "first_seen_at_utc", "last_seen_at_utc",
    "document_id", "page_id", "datapoint_id",
    "stable_datapoint_key", "value_fingerprint", "value_slot_key",
    "identifiers_json", "value_variants_json",
]

# record_type-Werte wie sie das Extraktionsmodell vergibt -> Sheetname.
RECORD_SHEETS = [
    ("chemical", "10_Chemistry"),
    ("tensile", "11_Tensile"),
    ("impact", "12_Impact"),
    ("hardness", "13_Hardness"),
    ("dimensional", "14_Dimensions"),
    ("heat_treatment", "15_Heat_Treatment"),
    ("nde", "16_NDE"),
    ("pressure", "17_Pressure"),
    ("product_info", "18_Product_Info"),
    ("certificate", "19_Certificate"),
    ("compliance", "20_Compliance"),
    ("other", "21_Other"),
]
WIDE_RECORD_TYPES = {"chemical", "tensile", "impact", "hardness"}
# Wide-Pivot-Sheets nur fuer Dokumente bis zu dieser Datenpunktzahl erzeugen
# (der Pivot verdoppelt kurzzeitig den RAM-Bedarf).
WIDE_MAX_DATAPOINTS = 8000

# Spalten, die in der clean-Version je Sheet zusaetzlich entfernt werden.
IDENTIFIER_DROP_CLEAN = {"run_id", "identifier_id", "document_id", "page_id", "created_at_utc"}
PAGESTATUS_DROP_CLEAN = {"run_id", "model_name", "duration_s"}

# COMMAND ----------

# DBTITLE 1,Hilfsfunktionen
def origin_folder_number(pdf_path: str) -> str:
    """Leitet die Nummer des Herkunftsordners aus dem PDF-Pfad ab.

    Beispiele:
      .../Projektdaten/12_ABS/datei.pdf            -> "12"
      .../Projektdaten/DHB_03.06.01.02.04/x.pdf    -> "03.06.01.02.04"
      .../Projektdaten/Sonderfall/x.pdf            -> "Sonderfall" (keine Ziffern)
    """
    folder = Path(pdf_path).parent.name
    match = re.search(r"\d+(?:[._]\d+)*", folder)
    raw = match.group(0) if match else folder
    return sanitize_name(raw) or "unbekannt"


def sanitize_name(name: str, max_len: int = 120) -> str:
    base = re.sub(r"[^\w\-. ()]+", "_", str(name), flags=re.UNICODE).strip(" _.")
    return base[:max_len]


def safe_sheet_name(name: str, used: set) -> str:
    """Excel-Sheetnamen: max. 31 Zeichen, eindeutig, ohne verbotene Zeichen."""
    clean = re.sub(r"[\[\]\:\*\?\/\\]", "_", str(name))[:31] or "Sheet"
    candidate = clean
    i = 1
    while candidate.lower() in used:
        suffix = f"_{i}"
        candidate = clean[: 31 - len(suffix)] + suffix
        i += 1
    used.add(candidate.lower())
    return candidate


def reorder_columns(df: pd.DataFrame, preferred: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    return df[cols]


def drop_columns(df: pd.DataFrame, drop: set) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    keep = [c for c in df.columns if c not in drop]
    return df[keep]


def make_wide_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Pivotiert property_name zu Spalten (eine Zeile je Identifier-/Proben-Kombi)."""
    if df is None or df.empty or "property_name" not in df.columns or "value_raw" not in df.columns:
        return pd.DataFrame()
    index_cols = [
        c for c in [
            "file_name", "page_number", "identifier_keys", "record_type",
            "section", "table_name", "specimen_or_sample", "orientation",
            "test_temperature_c",
        ]
        if c in df.columns
    ]
    if not index_cols:
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


def _coerce_cell(v: Any) -> Any:
    """xlsxwriter-taugliche Zelle: None/komplexe Typen zu str/"" wandeln."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float, str)):
        return v
    return str(v)


def open_workbook(target_path: str):
    """xlsxwriter-Workbook ueber lokale Temp-Datei (constant_memory streamt auf Disk)."""
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    wb = xlsxwriter.Workbook(tmp_path, {"constant_memory": True, "in_memory": False})
    return wb, tmp_path


def finalize_workbook(wb, tmp_path: str, target_path: str) -> None:
    wb.close()
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(tmp_path, target_path)
    try:
        os.remove(tmp_path)
    except Exception:
        pass


def write_sheet_rows(wb, sheet_name: str, header: List[str], row_iter, used_names: set) -> int:
    """Schreibt ein Sheet zeilenweise aus einem Iterator (haelt nie das ganze Sheet im RAM)."""
    name = safe_sheet_name(sheet_name, used_names)
    ws = wb.add_worksheet(name)
    ncols = len(header)
    if header:
        ws.write_row(0, 0, [str(h) for h in header])
        ws.freeze_panes(1, 0)
    r = 1
    for vals in row_iter:
        ws.write_row(r, 0, [_coerce_cell(x) for x in vals])
        r += 1
    if header and r > 1:
        ws.autofilter(0, 0, r - 1, max(0, ncols - 1))
    return r - 1


def spark_row_iter(sdf, columns: List[str]):
    """Streamt Zeilen einer Spark-DataFrame partitionsweise in den Treiber."""
    for row in sdf.select(*columns).toLocalIterator():
        yield [row[c] for c in columns]

# COMMAND ----------

# DBTITLE 1,Dokumente bestimmen
# Vorbedingung: die konsolidierte Tabelle muss existieren und Zeilen haben.
if not spark.catalog.tableExists(DATAPOINT_TABLE):
    raise RuntimeError(
        f"Tabelle {DATAPOINT_TABLE} existiert nicht. Zuerst die Extraktion bzw. "
        f"das Re-Flatten-Notebook (04) ausfuehren."
    )

dp_total_rows = spark.table(DATAPOINT_TABLE).count()
print(f"Zeilen in {DATAPOINT_TABLE}: {dp_total_rows}", flush=True)
if dp_total_rows == 0:
    raise RuntimeError(
        f"{DATAPOINT_TABLE} ist LEER. Vermutlich wurde das Re-Flatten (04) ohne "
        f"OK-Seiten ausgefuehrt oder die Extraktion fehlt. Export abgebrochen."
    )

# Basis: konsolidierte Datenpunkte (dokumentweit, ueber alle Laeufe).
docs_df = (
    spark.table(DATAPOINT_TABLE)
    .select("document_id", "pdf_path", "file_name")
    .distinct()
)

# Optional auf einen Run einschraenken (Dokumente, die in dem Run verarbeitet wurden).
# Wichtig: ein STALE RUN_ID-Widget darf den Export nicht still auf 0 Dokumente
# reduzieren. Wenn der Filter nichts trifft, wird er ignoriert (alle Dokumente).
if RUN_ID_FILTER:
    print(f"RUN_ID-Filter aktiv: {RUN_ID_FILTER}", flush=True)
    run_docs = (
        spark.table(PAGE_MANIFEST_TABLE)
        .where(F.col("run_id") == RUN_ID_FILTER)
        .select("document_id")
        .distinct()
    )
    filtered = docs_df.join(run_docs, "document_id", "inner")
    if filtered.limit(1).count() == 0:
        print(
            f"WARNUNG: RUN_ID-Filter '{RUN_ID_FILTER}' trifft keine Dokumente "
            f"(stale Widget?). Filter wird ignoriert -> ALLE Dokumente exportiert.",
            flush=True,
        )
    else:
        docs_df = filtered

documents = [r.asDict() for r in docs_df.orderBy("file_name").collect()]
print(f"Zu exportierende Dokumente: {len(documents)}", flush=True)
if not documents:
    raise RuntimeError("Keine Dokumente zu exportieren - Export abgebrochen (siehe Meldungen oben).")

# COMMAND ----------

# DBTITLE 1,Export pro Dokument (full + clean)
def _overview_rows(meta, dp_total, review_n, ids_n, pages_n, models, run_ids, include_run_info):
    rows = [
        ["file_name", meta["file_name"]],
        ["origin_folder_number", meta["origin_folder"]],
        ["pdf_path", meta["pdf_path"]],
        ["datapoints_total", dp_total],
        ["review_rows", review_n],
        ["identifier_rows", ids_n],
        ["page_rows", pages_n],
        ["export_created_at_utc", datetime.utcnow().isoformat()],
        ["version", "full (mit Run-Infos)" if include_run_info else "clean (ohne Run-Infos)"],
    ]
    if include_run_info:
        rows += [
            ["document_id", meta["document_id"]],
            ["model_names", ", ".join(map(str, models))],
            ["seen_run_ids", " ; ".join(map(str, run_ids))],
        ]
    return rows


def export_document(document_id: str, meta: Dict[str, Any], full_path: str, clean_path: str) -> int:
    """Exportiert ein Dokument als zwei xlsx (full/clean), zeilenweise gestreamt.

    Es wird NICHTS pro Dokument komplett nach pandas materialisiert: jede Zeile
    fliesst per toLocalIterator (partitionsweise) direkt in xlsxwriter
    (constant_memory). So bleibt der Treiber-RAM unabhaengig von der Dokumentgroesse.
    """
    # Datenpunkte (sortiert, gecached -> viele Sheets lesen denselben Cache)
    dp_sdf = (
        spark.table(DATAPOINT_TABLE)
        .where(F.col("document_id") == document_id)
        .orderBy("page_number", "record_type", "identifier_keys", "property_name")
        .cache()
    )
    dp_total = dp_sdf.count()  # materialisiert den Cache

    dp_cols_all = [c for c in PREFERRED_DP_COLS if c in dp_sdf.columns] + [c for c in dp_sdf.columns if c not in PREFERRED_DP_COLS]
    dp_cols_clean = [c for c in dp_cols_all if c not in RUN_META_COLUMNS]

    ids_sdf = (
        spark.table(IDENTIFIER_TABLE)
        .where(F.col("document_id") == document_id)
        .dropDuplicates(["page_number", "identifier_type", "identifier_value"])
        .orderBy("page_number", "identifier_type", "identifier_value")
    )
    ids_cols_all = list(ids_sdf.columns)
    ids_cols_clean = [c for c in ids_cols_all if c not in IDENTIFIER_DROP_CLEAN]

    # Seitenstatus: nur noetige Spalten VOR dem Window auswaehlen, damit die
    # riesigen Textspalten (parsed_json/response_text) nicht durch den Shuffle laufen.
    page_w = Window.partitionBy("page_id").orderBy(
        F.when(F.col("status") == "ok", F.lit(1)).otherwise(F.lit(0)).desc(),
        F.col("created_at_utc").desc(),
    )
    pages_sdf = (
        spark.table(RAW_TABLE)
        .where(F.col("document_id") == document_id)
        .select("page_id", "page_number", "file_name", "status", "run_id",
                "model_name", "duration_s", "error_message", "created_at_utc")
        .withColumn("_rn", F.row_number().over(page_w))
        .where(F.col("_rn") == 1)
        .drop("_rn", "created_at_utc")
        .orderBy("page_number")
    )
    pages_cols_all = ["page_id", "page_number", "file_name", "status", "run_id", "model_name", "duration_s", "error_message"]
    pages_cols_clean = [c for c in pages_cols_all if c not in PAGESTATUS_DROP_CLEAN]

    review_cond = (
        (F.col("needs_human_review") == True)
        | (F.coalesce(F.col("confidence_final"), F.lit(0.0)) < F.lit(CONFIDENCE_THRESHOLD_REVIEW))
        | (F.col("verification_status") == "changed_or_conflicting")
    )
    review_sdf = dp_sdf.where(review_cond)

    # Kennzahlen + Record-Types einmalig bestimmen (guenstig auf dem Cache)
    review_n = review_sdf.count()
    ids_n = ids_sdf.count()
    pages_n = pages_sdf.count()
    models = sorted({r[0] for r in dp_sdf.select("model_name").distinct().collect() if r[0] is not None})
    run_ids = sorted({r[0] for r in dp_sdf.select("seen_run_ids").distinct().collect() if r[0] is not None})
    present_record_types = {r[0] for r in dp_sdf.select("record_type").distinct().collect()}
    allow_wide = dp_total <= WIDE_MAX_DATAPOINTS

    try:
        for include_run_info, target in ((True, full_path), (False, clean_path)):
            dp_cols = dp_cols_all if include_run_info else dp_cols_clean
            ids_cols = ids_cols_all if include_run_info else ids_cols_clean
            pg_cols = pages_cols_all if include_run_info else pages_cols_clean

            wb, tmp_path = open_workbook(target)
            used: set = set()
            try:
                write_sheet_rows(wb, "00_Overview", ["field", "value"],
                                 iter(_overview_rows(meta, dp_total, review_n, ids_n, pages_n, models, run_ids, include_run_info)),
                                 used)
                write_sheet_rows(wb, "01_All_Datapoints", dp_cols, spark_row_iter(dp_sdf, dp_cols), used)
                write_sheet_rows(wb, "02_Review", dp_cols, spark_row_iter(review_sdf, dp_cols), used)
                write_sheet_rows(wb, "03_Identifiers", ids_cols, spark_row_iter(ids_sdf, ids_cols), used)
                write_sheet_rows(wb, "04_Page_Status", pg_cols, spark_row_iter(pages_sdf, pg_cols), used)

                for record_type, sheet_name in RECORD_SHEETS:
                    if record_type not in present_record_types:
                        continue
                    sub = dp_sdf.where(F.col("record_type") == record_type)
                    write_sheet_rows(wb, sheet_name, dp_cols, spark_row_iter(sub, dp_cols), used)
                    if allow_wide and record_type in WIDE_RECORD_TYPES:
                        wide_df = make_wide_sheet(sub.toPandas())
                        if not wide_df.empty:
                            write_sheet_rows(
                                wb, f"{sheet_name}_Wide", list(wide_df.columns),
                                (list(t) for t in wide_df.itertuples(index=False, name=None)), used,
                            )
                finalize_workbook(wb, tmp_path, target)
            except Exception:
                try:
                    wb.close()
                    os.remove(tmp_path)
                except Exception:
                    pass
                raise
        return dp_total
    finally:
        dp_sdf.unpersist()



export_rows = []
errors = []
total_docs = len(documents)

print(f"Starte Export von {total_docs} Dokumenten ...", flush=True)

for idx, doc in enumerate(documents, start=1):
    document_id = doc["document_id"]
    pdf_path = doc["pdf_path"]
    file_name = doc["file_name"]
    folder_number = origin_folder_number(pdf_path)
    stem = sanitize_name(Path(file_name).stem) or document_id

    meta = {
        "document_id": document_id,
        "pdf_path": pdf_path,
        "file_name": file_name,
        "origin_folder": folder_number,
    }

    # Getrennte Ablagestruktur je Version, jeweils mit nummerierter Unterstruktur.
    full_path = str(Path(XLSX_EXPORT_ROOT) / FULL_SUBDIR / folder_number / f"{stem}.xlsx")
    clean_path = str(Path(XLSX_EXPORT_ROOT) / CLEAN_SUBDIR / folder_number / f"{stem}.xlsx")

    print(f"[{idx}/{total_docs}] start [{folder_number}] {file_name} ...", flush=True)
    try:
        n_dp = export_document(document_id, meta, full_path, clean_path)
        export_rows.append({
            "folder_number": folder_number,
            "file_name": file_name,
            "datapoints": n_dp,
            "full_xlsx": full_path,
            "clean_xlsx": clean_path,
        })
        print(f"[{idx}/{total_docs}] OK [{folder_number}] {file_name} ({n_dp} Datenpunkte)", flush=True)
    except Exception as exc:
        errors.append({"file_name": file_name, "error": repr(exc)})
        # Beim ERSTEN Fehler den vollen Traceback zeigen, damit die Ursache nicht
        # in der Schleife verschluckt wird.
        if len(errors) == 1:
            print(f"[{idx}/{total_docs}] ERSTER FEHLER bei {file_name}:\n{traceback.format_exc()}", flush=True)
        else:
            print(f"[{idx}/{total_docs}] FEHLER [{folder_number}] {file_name}: {repr(exc)}", flush=True)
    finally:
        gc.collect()

print(f"\nFertig: {len(export_rows)} Dokumente exportiert, {len(errors)} Fehler.", flush=True)
print(f"Zielverzeichnis: {XLSX_EXPORT_ROOT}")

# Kein stiller "Erfolg": wenn keine einzige Datei geschrieben wurde, hart fehlschlagen.
if not export_rows:
    first_error = errors[0]["error"] if errors else "unbekannt"
    raise RuntimeError(
        f"Es wurde KEINE xlsx geschrieben (von {total_docs} Dokumenten, {len(errors)} Fehler). "
        f"Erster Fehler: {first_error}"
    )

# COMMAND ----------

# DBTITLE 1,Index-Datei
if export_rows:
    index_df = pd.DataFrame(export_rows).sort_values(["folder_number", "file_name"])
    index_path = str(Path(XLSX_EXPORT_ROOT) / "00_INDEX.xlsx")
    wb, tmp_path = open_workbook(index_path)
    write_sheet_rows(
        wb, "Index", list(index_df.columns),
        (list(t) for t in index_df.itertuples(index=False, name=None)), set(),
    )
    finalize_workbook(wb, tmp_path, index_path)
    print(f"Index: {index_path}")
    try:
        display(spark.createDataFrame(index_df))
    except Exception:
        print(index_df.to_string(index=False))

if errors:
    print("\nFehler:")
    for e in errors:
        print(f"  - {e['file_name']}: {e['error']}")
