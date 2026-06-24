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
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F

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


def write_volume_xlsx(sheets, target_path: str) -> None:
    """Schreibt mehrere DataFrames als Sheets in eine xlsx-Datei im Volume.

    Speicherschonend fuer Serverless:
    - Engine xlsxwriter mit constant_memory=True -> jede Zeile wird sofort auf die
      Platte geflusht, die Arbeitsmappe wird NICHT komplett im RAM gehalten
      (openpyxl tat genau das und sprengte bei 30k+ Zeilen den Speicher).
    - Schreibt in eine lokale Temp-Datei und kopiert sie danach ins Volume.
    - Teil-Frames werden nach dem Schreiben sofort freigegeben.

    sheets: Iterable von (sheet_name, DataFrame). Mit constant_memory muessen die
    Zeilen je Sheet in Reihenfolge geschrieben werden - pandas.to_excel tut das.
    """
    used_names: set = set()
    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    try:
        with pd.ExcelWriter(
            tmp_path,
            engine="xlsxwriter",
            engine_kwargs={"options": {"constant_memory": True}},
        ) as writer:
            wrote_any = False
            for raw_name, df in sheets:
                name = safe_sheet_name(raw_name, used_names)
                frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
                # freeze_panes fixiert die Kopfzeile (kein Zellen-Scan noetig)
                frame.to_excel(writer, sheet_name=name, index=False, freeze_panes=(1, 0))
                try:
                    n_rows, n_cols = frame.shape
                    if n_rows > 0 and n_cols > 0:
                        writer.sheets[name].autofilter(0, 0, n_rows, n_cols - 1)
                except Exception:
                    pass
                wrote_any = True
                del frame
                gc.collect()
            if not wrote_any:
                pd.DataFrame().to_excel(writer, sheet_name="leer", index=False)

        Path(target_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(tmp_path, target_path)
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass

# COMMAND ----------

# DBTITLE 1,Dokumente bestimmen
# Basis: konsolidierte Datenpunkte (dokumentweit, ueber alle Laeufe).
docs_df = (
    spark.table(DATAPOINT_TABLE)
    .select("document_id", "pdf_path", "file_name")
    .distinct()
)

# Optional auf einen Run einschraenken (Dokumente, die in dem Run verarbeitet wurden).
if RUN_ID_FILTER:
    run_docs = (
        spark.table(PAGE_MANIFEST_TABLE)
        .where(F.col("run_id") == RUN_ID_FILTER)
        .select("document_id")
        .distinct()
    )
    docs_df = docs_df.join(run_docs, "document_id", "inner")

documents = [r.asDict() for r in docs_df.orderBy("file_name").collect()]
print(f"Zu exportierende Dokumente: {len(documents)}")

# COMMAND ----------

# DBTITLE 1,Export pro Dokument (full + clean)
def load_document_frames(document_id: str) -> Dict[str, pd.DataFrame]:
    """Laedt alle relevanten Daten eines Dokuments als pandas-DataFrames."""
    dp = (
        spark.table(DATAPOINT_TABLE)
        .where(F.col("document_id") == document_id)
        .orderBy("page_number", "record_type", "identifier_keys", "property_name")
        .toPandas()
    )

    ids = (
        spark.table(IDENTIFIER_TABLE)
        .where(F.col("document_id") == document_id)
        .dropDuplicates(["page_number", "identifier_type", "identifier_value"])
        .orderBy("page_number", "identifier_type", "identifier_value")
        .toPandas()
    )

    page_w = Window.partitionBy("page_id").orderBy(
        F.when(F.col("status") == "ok", F.lit(1)).otherwise(F.lit(0)).desc(),
        F.col("created_at_utc").desc(),
    )
    pages = (
        spark.table(RAW_TABLE)
        .where(F.col("document_id") == document_id)
        .withColumn("_rn", F.row_number().over(page_w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
        .select("page_id", "page_number", "file_name", "status", "run_id",
                "model_name", "duration_s", "error_message")
        .orderBy("page_number")
        .toPandas()
    )
    return {"dp": dp, "ids": ids, "pages": pages}


def build_sheets(frames: Dict[str, pd.DataFrame], meta: Dict[str, Any], include_run_info: bool):
    """Generator: liefert (sheet_name, DataFrame) lazy.

    Lazy, damit jedes Teil-Frame erst beim Schreiben entsteht und danach (vom
    Writer) sofort freigegeben werden kann - wichtig fuer den RAM bei grossen
    Dokumenten.
    """
    dp = frames["dp"].copy()
    ids = frames["ids"].copy()
    pages = frames["pages"].copy()

    dp_total = int(len(dp))

    # Datenpunkte: alle Felder, ggf. Run-/Technik-Felder entfernen
    if not include_run_info and not dp.empty:
        dp = drop_columns(dp, RUN_META_COLUMNS)
    dp = reorder_columns(dp, PREFERRED_DP_COLS)

    # Review-Teilmenge
    if not dp.empty:
        needs_review = dp["needs_human_review"] == True if "needs_human_review" in dp.columns else False
        low_conf = dp["confidence_final"].fillna(0.0) < CONFIDENCE_THRESHOLD_REVIEW if "confidence_final" in dp.columns else False
        conflict = dp["verification_status"] == "changed_or_conflicting" if "verification_status" in dp.columns else False
        review = dp[needs_review | low_conf | conflict].copy()
    else:
        review = pd.DataFrame()

    # Identifier / Page-Status: clean-Version ohne Run-/ID-Spalten
    if not include_run_info:
        ids = drop_columns(ids, IDENTIFIER_DROP_CLEAN)
        pages = drop_columns(pages, PAGESTATUS_DROP_CLEAN)

    # Overview
    overview_rows = [
        {"field": "file_name", "value": meta["file_name"]},
        {"field": "origin_folder_number", "value": meta["origin_folder"]},
        {"field": "pdf_path", "value": meta["pdf_path"]},
        {"field": "datapoints_total", "value": dp_total},
        {"field": "review_rows", "value": int(len(review))},
        {"field": "identifier_rows", "value": int(len(ids))},
        {"field": "page_rows", "value": int(len(pages))},
        {"field": "export_created_at_utc", "value": datetime.utcnow().isoformat()},
        {"field": "version", "value": "full (mit Run-Infos)" if include_run_info else "clean (ohne Run-Infos)"},
    ]
    if include_run_info:
        src = frames["dp"]
        models = sorted({str(m) for m in src.get("model_name", pd.Series(dtype=str)).dropna().unique()}) if not src.empty else []
        run_ids = sorted({str(r) for r in src.get("seen_run_ids", pd.Series(dtype=str)).dropna().unique()}) if not src.empty else []
        overview_rows.extend([
            {"field": "document_id", "value": meta["document_id"]},
            {"field": "model_names", "value": ", ".join(models)},
            {"field": "seen_run_ids", "value": " ; ".join(run_ids)},
        ])

    yield ("00_Overview", pd.DataFrame(overview_rows))
    yield ("01_All_Datapoints", dp)
    yield ("02_Review", review)
    yield ("03_Identifiers", ids)
    yield ("04_Page_Status", pages)

    # Fachliche Sheets je record_type. Wide-Pivots nur fuer kleinere Dokumente
    # (der Pivot verdoppelt kurzzeitig den RAM-Bedarf).
    allow_wide = dp_total <= WIDE_MAX_DATAPOINTS
    if not dp.empty and "record_type" in dp.columns:
        for record_type, sheet_name in RECORD_SHEETS:
            sub = dp[dp["record_type"] == record_type].copy()
            if sub.empty:
                continue
            yield (sheet_name, sub)
            if allow_wide and record_type in WIDE_RECORD_TYPES:
                wide = make_wide_sheet(sub)
                if not wide.empty:
                    yield (f"{sheet_name}_Wide", wide)



export_rows = []
errors = []

for doc in documents:
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

    try:
        frames = load_document_frames(document_id)
        n_dp = int(len(frames["dp"]))
        write_volume_xlsx(build_sheets(frames, meta, include_run_info=True), full_path)
        write_volume_xlsx(build_sheets(frames, meta, include_run_info=False), clean_path)

        export_rows.append({
            "folder_number": folder_number,
            "file_name": file_name,
            "datapoints": n_dp,
            "full_xlsx": full_path,
            "clean_xlsx": clean_path,
        })
        print(f"  OK [{folder_number}] {file_name} ({n_dp} Datenpunkte)")
    except Exception as exc:
        errors.append({"file_name": file_name, "error": repr(exc)})
        print(f"  FEHLER [{folder_number}] {file_name}: {repr(exc)}")
    finally:
        # Speicher zwischen Dokumenten freigeben (grosse pandas-Frames)
        frames = None
        gc.collect()

print(f"\nFertig: {len(export_rows)} Dokumente exportiert, {len(errors)} Fehler.")
print(f"Zielverzeichnis: {XLSX_EXPORT_ROOT}")

# COMMAND ----------

# DBTITLE 1,Index-Datei
if export_rows:
    index_df = pd.DataFrame(export_rows).sort_values(["folder_number", "file_name"])
    write_volume_xlsx([("Index", index_df)], str(Path(XLSX_EXPORT_ROOT) / "00_INDEX.xlsx"))
    print(f"Index: {Path(XLSX_EXPORT_ROOT) / '00_INDEX.xlsx'}")
    try:
        display(spark.createDataFrame(index_df))
    except Exception:
        print(index_df.to_string(index=False))

if errors:
    print("\nFehler:")
    for e in errors:
        print(f"  - {e['file_name']}: {e['error']}")
