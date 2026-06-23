# Databricks notebook source
# MAGIC %md
# MAGIC # Materialzeugnis-Extraktion nach Excel, serverless-parallel
# MAGIC
# MAGIC Ziel:
# MAGIC - rekursiv alle PDF-Dateien unter ROOT_PATH finden
# MAGIC - jede PDF-Seite als eigene serverless-parallele Arbeitseinheit auswerten
# MAGIC - gedrehte Seiten vor der Extraktion erkennen und korrekt rendern
# MAGIC - alle Werte mit Identifiern, Seitenherkunft und Konfidenz extrahieren
# MAGIC - Re-Runs historisieren und konsolidierte Konfidenzen berechnen
# MAGIC - pro PDF-Dokument eine Excel-Datei mit fachlichen Listen exportieren

# COMMAND ----------

# MAGIC %pip install json-repair --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from __future__ import annotations

import base64
import gc
import hashlib
import io
import json
import math
import os
import re
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from json_repair import repair_json
from PIL import Image, ImageDraw
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# ============================================================
# Parameter
# ============================================================

DEFAULT_ROOT_PATH = "/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten"
DEFAULT_EXPORT_ROOT_PATH = "/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export"
DEFAULT_IMAGES_ROOT_PATH = "/Volumes/playground/u_daniel_bick/rohdaten/bilder"
IMAGE_FILE_PATTERN = "{stem}__seite_{page:04d}.png"

CATALOG = "playground"
SCHEMA = "u_daniel_bick"

PRIMARY_MODEL = "databricks-claude-opus-4-8"
FALLBACK_MODEL = "databricks-claude-sonnet-4-6"

EXPECTED_PDF_COUNT = 53

# Run-Modus steuert, welche Seiten ein Lauf verarbeitet:
#   "full"          -> alle Seiten frisch extrahieren (Erstlauf)
#   "retry_errors"  -> NUR Seiten erneut extrahieren, die in SOURCE_RUN_ID keinen
#                      erfolgreichen (status='ok') Treffer haben
#                      (Timeouts, unleserliche Scans, abgebrochene Seiten)
#   "reverify"      -> ALLE Seiten der Dokumente aus SOURCE_RUN_ID erneut lesen,
#                      um per Re-Run-Abgleich die Confidence anzuheben bzw.
#                      Konflikte aufzudecken
DEFAULT_RUN_MODE = "full"
VALID_RUN_MODES = ("full", "retry_errors", "reverify")

# Serverless-Parallelisierung:
# Die tatsächliche Parallelität wird durch Serverless-Scaling und Model-Serving-Rate-Limits begrenzt.
THREADS_PER_PARTITION = 6
MAX_SPARK_PARTITIONS = 16
TARGET_RECORDS_PER_PARTITION = 4

# Vision-/PDF-Rendering
ENABLE_ORIENTATION_DETECTION = False
INCLUDE_PREVIOUS_PAGE_AS_CONTEXT_IMAGE = False

DPI = 150
MAX_IMAGE_SIDE_PX = 1600
JPEG_QUALITY = 82

ORIENTATION_DPI = 95
ORIENTATION_MAX_SIDE_PX = 900
ORIENTATION_JPEG_QUALITY = 82

MAX_TOKENS = 16000
REQUEST_TIMEOUT_S = 300
MODEL_RETRIES = 8

CONFIDENCE_THRESHOLD_REVIEW = 0.70

RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

RAW_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_raw_page_extractions"
PAGE_MANIFEST_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_page_manifest"
IDENTIFIER_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_identifiers"
DATAPOINT_RUNS_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints_runs"
DATAPOINT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints"
EXPORT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_excel_exports"
QUALITY_VIEW = f"{CATALOG}.{SCHEMA}.materialzeugnisse_quality_checks"

EXTRACTION_PROMPT_VERSION = "materialzeugnis_v4_compact_complete_extraction"

# COMMAND ----------

# Optional: Widgets erlauben Job-Parameter.
RUN_MODE = DEFAULT_RUN_MODE
SOURCE_RUN_ID = ""
try:
    dbutils.widgets.text("ROOT_PATH", DEFAULT_ROOT_PATH)
    dbutils.widgets.text("EXPORT_ROOT_PATH", DEFAULT_EXPORT_ROOT_PATH)
    dbutils.widgets.text("IMAGES_ROOT_PATH", DEFAULT_IMAGES_ROOT_PATH)
    # RUN_ID bewusst mit leerem Default: so erhaelt JEDER Lauf ohne explizite
    # Vorgabe einen frischen Zeitstempel. (Ein vorbelegter Default wuerde im
    # Widget bestehen bleiben und bei einem Re-Run faelschlich den alten Lauf
    # ueberschreiben.)
    dbutils.widgets.text("RUN_ID", "")
    dbutils.widgets.dropdown("RUN_MODE", DEFAULT_RUN_MODE, list(VALID_RUN_MODES))
    dbutils.widgets.text("SOURCE_RUN_ID", "")
    dbutils.widgets.text("DOCUMENT_FILTER", "")
    ROOT_PATH = dbutils.widgets.get("ROOT_PATH").strip() or DEFAULT_ROOT_PATH
    EXPORT_ROOT_PATH = dbutils.widgets.get("EXPORT_ROOT_PATH").strip() or DEFAULT_EXPORT_ROOT_PATH
    IMAGES_ROOT_PATH = dbutils.widgets.get("IMAGES_ROOT_PATH").strip() or DEFAULT_IMAGES_ROOT_PATH
    RUN_ID_WIDGET = dbutils.widgets.get("RUN_ID").strip()
    if RUN_ID_WIDGET:
        RUN_ID = RUN_ID_WIDGET
    RUN_MODE = (dbutils.widgets.get("RUN_MODE").strip() or DEFAULT_RUN_MODE).lower()
    SOURCE_RUN_ID = dbutils.widgets.get("SOURCE_RUN_ID").strip()
    DOCUMENT_FILTER = dbutils.widgets.get("DOCUMENT_FILTER").strip()
except Exception:
    ROOT_PATH = DEFAULT_ROOT_PATH
    EXPORT_ROOT_PATH = DEFAULT_EXPORT_ROOT_PATH
    IMAGES_ROOT_PATH = DEFAULT_IMAGES_ROOT_PATH
    DOCUMENT_FILTER = ""

if RUN_MODE not in VALID_RUN_MODES:
    print(f"Warnung: unbekannter RUN_MODE '{RUN_MODE}', falle zurueck auf '{DEFAULT_RUN_MODE}'.")
    RUN_MODE = DEFAULT_RUN_MODE

print(f"RUN_ID: {RUN_ID}")
print(f"RUN_MODE: {RUN_MODE}")
print(f"SOURCE_RUN_ID: {SOURCE_RUN_ID or '(auto: letzter Lauf)'}")
print(f"ROOT_PATH: {ROOT_PATH}")
print(f"EXPORT_ROOT_PATH: {EXPORT_ROOT_PATH}")
print(f"IMAGES_ROOT_PATH: {IMAGES_ROOT_PATH}")
print(f"DOCUMENT_FILTER: {DOCUMENT_FILTER or '(alle)'}")

# COMMAND ----------

# ============================================================
# Databricks Auth für Model Serving
# ============================================================

def get_databricks_auth() -> Tuple[str, str]:
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")

    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        if not host:
            host = ctx.apiUrl().get()
        if not token:
            token = ctx.apiToken().get()
    except Exception:
        pass

    if not host:
        raise RuntimeError("DATABRICKS_HOST fehlt.")
    if not token:
        raise RuntimeError("DATABRICKS_TOKEN fehlt.")

    if not host.startswith("http"):
        host = "https://" + host

    return host.rstrip("/"), token


DB_HOST, DB_TOKEN = get_databricks_auth()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

try:
    spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", str(max(THREADS_PER_PARTITION * 2, 10)))
except Exception:
    pass

# COMMAND ----------

# ============================================================
# Schemas
# ============================================================

raw_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("document_id", T.StringType(), False),
    T.StructField("page_id", T.StringType(), False),
    T.StructField("pdf_path", T.StringType(), False),
    T.StructField("file_name", T.StringType(), False),
    T.StructField("page_number", T.IntegerType(), False),
    T.StructField("page_count", T.IntegerType(), False),
    T.StructField("model_name", T.StringType(), True),
    T.StructField("status", T.StringType(), False),
    T.StructField("duration_s", T.DoubleType(), True),
    T.StructField("image_width_px", T.IntegerType(), True),
    T.StructField("image_height_px", T.IntegerType(), True),
    T.StructField("orientation_rotation_degrees", T.IntegerType(), True),
    T.StructField("orientation_confidence", T.DoubleType(), True),
    T.StructField("orientation_reason", T.StringType(), True),
    T.StructField("response_text", T.StringType(), True),
    T.StructField("parsed_json", T.StringType(), True),
    T.StructField("error_message", T.StringType(), True),
    T.StructField("extraction_prompt_version", T.StringType(), True),
    T.StructField("created_at_utc", T.TimestampType(), False),
])

manifest_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("document_id", T.StringType(), False),
    T.StructField("page_id", T.StringType(), False),
    T.StructField("pdf_path", T.StringType(), False),
    T.StructField("file_name", T.StringType(), False),
    T.StructField("page_number", T.IntegerType(), False),
    T.StructField("page_count", T.IntegerType(), False),
])

identifier_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("document_id", T.StringType(), False),
    T.StructField("page_id", T.StringType(), False),
    T.StructField("pdf_path", T.StringType(), False),
    T.StructField("file_name", T.StringType(), False),
    T.StructField("page_number", T.IntegerType(), False),
    T.StructField("identifier_id", T.StringType(), False),
    T.StructField("identifier_type", T.StringType(), True),
    T.StructField("identifier_label_raw", T.StringType(), True),
    T.StructField("identifier_value", T.StringType(), True),
    T.StructField("group_id", T.StringType(), True),
    T.StructField("scope", T.StringType(), True),
    T.StructField("source_page_number", T.IntegerType(), True),
    T.StructField("evidence_text", T.StringType(), True),
    T.StructField("confidence_model", T.DoubleType(), True),
    T.StructField("created_at_utc", T.TimestampType(), False),
])

datapoint_runs_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("document_id", T.StringType(), False),
    T.StructField("page_id", T.StringType(), False),
    T.StructField("datapoint_id", T.StringType(), False),
    T.StructField("stable_datapoint_key", T.StringType(), False),
    T.StructField("value_fingerprint", T.StringType(), False),
    T.StructField("pdf_path", T.StringType(), False),
    T.StructField("file_name", T.StringType(), False),
    T.StructField("page_number", T.IntegerType(), False),
    T.StructField("model_name", T.StringType(), True),
    T.StructField("orientation_rotation_degrees", T.IntegerType(), True),
    T.StructField("record_type", T.StringType(), True),
    T.StructField("group_id", T.StringType(), True),
    T.StructField("identifier_keys", T.StringType(), True),
    T.StructField("identifiers_json", T.StringType(), True),
    T.StructField("section", T.StringType(), True),
    T.StructField("table_name", T.StringType(), True),
    T.StructField("property_name", T.StringType(), True),
    T.StructField("value_slot_key", T.StringType(), True),
    T.StructField("property_label_raw", T.StringType(), True),
    T.StructField("test_standard", T.StringType(), True),
    T.StructField("specimen_or_sample", T.StringType(), True),
    T.StructField("orientation", T.StringType(), True),
    T.StructField("test_temperature_c", T.DoubleType(), True),
    T.StructField("operator", T.StringType(), True),
    T.StructField("value_raw", T.StringType(), True),
    T.StructField("value_num", T.DoubleType(), True),
    T.StructField("unit", T.StringType(), True),
    T.StructField("limit_min_raw", T.StringType(), True),
    T.StructField("limit_min_num", T.DoubleType(), True),
    T.StructField("limit_max_raw", T.StringType(), True),
    T.StructField("limit_max_num", T.DoubleType(), True),
    T.StructField("result_or_conformity", T.StringType(), True),
    T.StructField("evidence_text", T.StringType(), True),
    T.StructField("uncertainty_note", T.StringType(), True),
    T.StructField("confidence_model", T.DoubleType(), True),
    T.StructField("confidence_rule_adjustment", T.DoubleType(), True),
    T.StructField("confidence_final_pre_verification", T.DoubleType(), True),
    T.StructField("confidence_final", T.DoubleType(), True),
    T.StructField("verification_status", T.StringType(), True),
    T.StructField("needs_human_review", T.BooleanType(), True),
    T.StructField("page_quality_json", T.StringType(), True),
    T.StructField("created_at_utc", T.TimestampType(), False),
])

export_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("document_id", T.StringType(), False),
    T.StructField("pdf_path", T.StringType(), False),
    T.StructField("file_name", T.StringType(), False),
    T.StructField("excel_path", T.StringType(), False),
    T.StructField("datapoint_rows", T.IntegerType(), True),
    T.StructField("review_rows", T.IntegerType(), True),
    T.StructField("identifier_rows", T.IntegerType(), True),
    T.StructField("page_rows", T.IntegerType(), True),
    T.StructField("created_at_utc", T.TimestampType(), False),
])

# COMMAND ----------

# ============================================================
# Hilfsfunktionen
# ============================================================

def utcnow_naive() -> datetime:
    return datetime.utcnow()


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        try:
            return len(spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA} LIKE '{table_name.split('.')[-1]}'").collect()) > 0
        except Exception:
            return False


def append_or_create_delta(df, table_name: str) -> None:
    if table_exists(table_name):
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(table_name)
        )
    else:
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )


def sha1_short(text: str, length: int = 16) -> str:
    return hashlib.sha1(str(text).encode("utf-8")).hexdigest()[:length]


def normalise_text(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def safe_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, default=str)


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return float(value)
        except Exception:
            return None

    s = str(value).strip()
    if not s:
        return None
    s = s.replace(" ", "").replace(",", ".")
    match = re.search(r"[-+]?\d+(?:\.\d+)?", s)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def clamp_confidence(value: Any, default: float = 0.50) -> float:
    f = parse_float(value)
    if f is None:
        f = default
    return max(0.0, min(1.0, float(f)))


def extract_json_from_model_text(text: str) -> Dict[str, Any]:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        return json.loads(cleaned)
    except Exception:
        pass

    try:
        repaired = repair_json(cleaned)
        return json.loads(repaired)
    except Exception:
        pass

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start >= 0 and end > start:
        candidate = cleaned[start : end + 1]
        repaired = repair_json(candidate)
        return json.loads(repaired)

    raise ValueError("Keine valide JSON-Antwort extrahierbar.")


def list_pdfs(root_path: str) -> List[str]:
    root = Path(root_path)
    pdfs = sorted(str(p) for p in root.rglob("*.pdf"))
    pdfs += sorted(str(p) for p in root.rglob("*.PDF"))
    return sorted(set(pdfs))


def sanitize_filename(name: str, max_len: int = 120) -> str:
    base = re.sub(r"[^\w\-. ]+", "_", str(name), flags=re.UNICODE).strip()
    base = re.sub(r"\s+", "_", base)
    return base[:max_len] or "document"


def identifier_keys(identifiers: List[Dict[str, Any]]) -> str:
    keys = []
    for ident in identifiers or []:
        t = ident.get("identifier_type")
        v = ident.get("identifier_value")
        if t and v:
            keys.append(f"{normalise_text(t)}:{str(v).strip()}")
    return " | ".join(sorted(set(keys)))


def value_fingerprint_from_dp(dp: Dict[str, Any]) -> str:
    payload = {
        "operator": normalise_text(dp.get("operator")),
        "value_raw": normalise_text(dp.get("value_raw")),
        "value_num": parse_float(dp.get("value_num")),
        "unit": normalise_text(dp.get("unit")),
        "limit_min_raw": normalise_text(dp.get("limit_min_raw")),
        "limit_min_num": parse_float(dp.get("limit_min_num")),
        "limit_max_raw": normalise_text(dp.get("limit_max_raw")),
        "limit_max_num": parse_float(dp.get("limit_max_num")),
        "result_or_conformity": normalise_text(dp.get("result_or_conformity")),
    }
    return sha1_short(safe_json(payload), 24)


def stable_key_from_dp(
    document_id: str,
    page_number: int,
    dp: Dict[str, Any],
    ids: List[Dict[str, Any]],
    fallback_index: int,
) -> str:
    value_slot_key = dp.get("value_slot_key")
    if not value_slot_key:
        value_slot_key = "|".join([
            normalise_text(dp.get("record_type")),
            normalise_text(dp.get("section")),
            normalise_text(dp.get("table_name")),
            normalise_text(dp.get("property_name")),
            normalise_text(dp.get("property_label_raw")),
            normalise_text(dp.get("specimen_or_sample")),
            normalise_text(dp.get("orientation")),
            str(parse_float(dp.get("test_temperature_c"))),
            normalise_text(dp.get("unit")),
            f"fallback_index:{fallback_index}",
        ])

    payload = {
        "document_id": document_id,
        "page_number": int(page_number),
        "record_type": normalise_text(dp.get("record_type")),
        "identifier_keys": identifier_keys(ids),
        "property_name": normalise_text(dp.get("property_name")),
        "value_slot_key": normalise_text(value_slot_key),
        "unit": normalise_text(dp.get("unit")),
    }
    return sha1_short(safe_json(payload), 32)


def calculate_preverification_confidence(dp: Dict[str, Any], page_quality: Dict[str, Any]) -> Tuple[float, float, bool]:
    base = clamp_confidence(dp.get("confidence_model", dp.get("confidence")), default=0.55)
    adjustment = 0.0

    ids = dp.get("identifiers", []) or []
    if not ids:
        adjustment -= 0.20

    scopes = {normalise_text(i.get("scope")) for i in ids}
    if "ambiguous" in scopes:
        adjustment -= 0.12
    if "inferred_from_previous_page" in scopes:
        adjustment -= 0.07

    scan_quality = normalise_text(page_quality.get("scan_quality"))
    if scan_quality == "poor":
        adjustment -= 0.15
    elif scan_quality == "unreadable":
        adjustment -= 0.35
    elif scan_quality == "medium":
        adjustment -= 0.04

    if dp.get("uncertainty_note"):
        adjustment -= 0.05

    final_pre = max(0.0, min(1.0, base + adjustment))
    needs_review = bool(
        final_pre < CONFIDENCE_THRESHOLD_REVIEW
        or page_quality.get("needs_human_review") is True
        or "ambiguous" in scopes
        or not ids
    )

    return base, adjustment, needs_review

# COMMAND ----------

# DBTITLE 1,Bildladen aus Volume
# ============================================================
# Bildladen aus Volume (ersetzt PDF-Rendering)
# ============================================================

def resolve_image_path(pdf_path: str, page_number: int) -> str:
    """Leitet aus dem PDF-Pfad den Volume-Bildpfad ab.
    Schema: <IMAGES_ROOT>/<ordner>/<pdf_stem>/<pdf_stem>__seite_NNNN.png
    """
    pdf_p = Path(pdf_path)
    stem = pdf_p.stem
    ordner = pdf_p.parent.name
    image_name = IMAGE_FILE_PATTERN.format(stem=stem, page=int(page_number))
    return str(Path(IMAGES_ROOT_PATH) / ordner / stem / image_name)


def load_page_image_bytes(pdf_path: str, page_number: int) -> Tuple[bytes, Tuple[int, int], float]:
    """Laedt ein vorgerendertes Seitenbild aus dem Volume.
    Returns: (image_bytes, (width, height), size_mb)
    """
    image_path = resolve_image_path(pdf_path, page_number)
    if not os.path.exists(image_path):
        raise FileNotFoundError(f"Seitenbild nicht gefunden: {image_path}")
    file_size = os.path.getsize(image_path)
    size_mb = file_size / (1024 * 1024)
    with open(image_path, "rb") as f:
        image_bytes = f.read()
    width, height = _png_dimensions(image_bytes)
    return image_bytes, (width, height), size_mb


def _png_dimensions(data: bytes) -> Tuple[int, int]:
    """Liest Breite/Hoehe aus dem PNG-IHDR-Chunk (Bytes 16-23)."""
    import struct
    if len(data) >= 24 and data[:8] == b'\x89PNG\r\n\x1a\n':
        w, h = struct.unpack('>II', data[16:24])
        return w, h
    img = Image.open(io.BytesIO(data))
    dims = img.size
    img.close()
    del img
    return dims


def count_page_images(pdf_path: str) -> int:
    """Zaehlt vorhandene Seitenbilder fuer ein PDF im Volume."""
    pdf_p = Path(pdf_path)
    stem = pdf_p.stem
    ordner = pdf_p.parent.name
    image_dir = Path(IMAGES_ROOT_PATH) / ordner / stem
    if not image_dir.exists():
        return 0
    return len([f for f in image_dir.iterdir() if f.suffix.lower() == ".png" and "__seite_" in f.name])


def make_orientation_collage(pdf_path: str, page_number: int) -> bytes:
    """Erzeugt 2x2-Montage aus 4 Rotationen des vorgerenderten Bildes."""
    image_bytes, _, _ = load_page_image_bytes(pdf_path, page_number)
    base_img = Image.open(io.BytesIO(image_bytes))
    del image_bytes
    max_side = ORIENTATION_MAX_SIDE_PX
    if max(base_img.size) > max_side:
        base_img.thumbnail((max_side, max_side), Image.Resampling.LANCZOS)
    rotations = [0, 90, 180, 270]
    cells = []
    for rot in rotations:
        rotated = base_img.rotate(-rot, expand=True) if rot != 0 else base_img.copy()
        canvas = Image.new("RGB", (rotated.width, rotated.height + 42), "white")
        canvas.paste(rotated, (0, 42))
        draw = ImageDraw.Draw(canvas)
        draw.text((12, 12), f"ROTATION_{rot}_DEGREES", fill="black")
        cells.append(canvas)
        if rot != 0:
            rotated.close()
            del rotated
    w = max(c.width for c in cells)
    h = max(c.height for c in cells)
    collage = Image.new("RGB", (2 * w, 2 * h), "white")
    collage.paste(cells[0], (0, 0))
    collage.paste(cells[1], (w, 0))
    collage.paste(cells[2], (0, h))
    collage.paste(cells[3], (w, h))
    out = io.BytesIO()
    collage.save(out, format="JPEG", quality=ORIENTATION_JPEG_QUALITY, optimize=True)
    result = out.getvalue()
    base_img.close()
    collage.close()
    for c in cells:
        c.close()
    del base_img, collage, cells
    gc.collect()
    return result


def make_test_image() -> bytes:
    """Minimales Testbild fuer Vision-Model-Check."""
    img = Image.new("RGB", (200, 60), "white")
    draw = ImageDraw.Draw(img)
    draw.text((10, 20), "TEST", fill="black")
    out = io.BytesIO()
    img.save(out, format="PNG")
    result = out.getvalue()
    img.close()
    del img
    return result

# COMMAND ----------

# ============================================================
# Model Serving
# ============================================================

SYSTEM_PROMPT = """
Du bist ein sehr genauer Extraktionsassistent fuer Materialzeugnisse, Abnahmepruefzeugnisse
und technische Pruefbescheinigungen fuer Pipelinebauteile. Du extrahierst ausschliesslich
sichtbare Informationen aus der bereitgestellten Seite. Du erfindest nichts.
Die Antwort muss ein einzelnes valides JSON-Objekt sein, ohne Markdown.
""".strip()


def response_text_from_serving_response(payload: Dict[str, Any]) -> str:
    choices = payload.get("choices", [])
    if not choices:
        raise ValueError(f"Keine choices in Model-Antwort: {payload}")

    message = choices[0].get("message", {})
    content = message.get("content", "")

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, dict):
                if "text" in item:
                    parts.append(str(item["text"]))
                elif item.get("type") in {"text", "output_text"} and "content" in item:
                    parts.append(str(item["content"]))
            else:
                parts.append(str(item))
        return "".join(parts)

    return str(content)


def call_model_images(
    model_name: str,
    prompt: str,
    labelled_images: List[Tuple[str, bytes]],
    max_tokens: int = MAX_TOKENS,
    use_json_response_format: bool = True,
) -> Tuple[str, Dict[str, Any]]:
    content: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]

    for label, image_bytes in labelled_images:
        image_b64 = base64.b64encode(image_bytes).decode("utf-8")
        mime = "image/png" if image_bytes[:8] == b'\x89PNG\r\n\x1a\n' else "image/jpeg"
        content.append({"type": "text", "text": f"IMAGE_LABEL: {label}"})
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:{mime};base64,{image_b64}",
                "detail": "high",
            },
        })

    request_payload: Dict[str, Any] = {
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": content},
        ],
        "stream": False,
        "max_tokens": max_tokens,
    }

    # AI Gateway Route (OpenAI-kompatibel)
    url = f"{DB_HOST}/ai-gateway/mlflow/v1/chat/completions"
    request_payload["model"] = model_name
    headers = {
        "Authorization": f"Bearer {DB_TOKEN}",
        "Content-Type": "application/json",
    }

    last_error: Optional[Exception] = None

    for attempt in range(1, MODEL_RETRIES + 1):
        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(request_payload),
                timeout=REQUEST_TIMEOUT_S,
            )

            if (
                response.status_code == 400
                and ("does not support" in response.text or "not supported" in response.text)
            ):
                # Unsupported parameter - retry without it
                error_text = response.text.lower()
                if "temperature" in error_text:
                    request_payload.pop("temperature", None)
                    continue
                if "response_format" in error_text or "json_object" in error_text:
                    request_payload.pop("response_format", None)
                    continue

            if response.status_code in {408, 409, 425, 429, 500, 502, 503, 504}:
                last_error = RuntimeError(f"{response.status_code}: {response.text[:1000]}")
                sleep_s = min(90, 2 ** attempt)
                time.sleep(sleep_s)
                continue

            response.raise_for_status()
            response_json = response.json()
            return response_text_from_serving_response(response_json), response_json

        except Exception as exc:
            last_error = exc
            sleep_s = min(90, 2 ** attempt)
            time.sleep(sleep_s)

    raise RuntimeError(f"Model call endgueltig fehlgeschlagen fuer {model_name}: {last_error}")


def select_available_vision_model() -> str:
    prompt = 'Lies das Bild. Antworte exakt als JSON: {"ok": true, "model_can_see_image": true}'
    image = make_test_image()

    for model_name in [PRIMARY_MODEL, FALLBACK_MODEL]:
        try:
            text, _ = call_model_images(
                model_name=model_name,
                prompt=prompt,
                labelled_images=[("CURRENT_PAGE_TO_EXTRACT", image)],
                max_tokens=200,
            )
            parsed = extract_json_from_model_text(text)
            if parsed.get("ok") is True:
                print(f"Gewaehlt: {model_name}")
                return model_name
        except Exception as exc:
            print(f"Modell nicht nutzbar oder nicht bildfaehig: {model_name} -> {exc}")

    raise RuntimeError("Keines der konfigurierten Modelle ist als Vision-Modell nutzbar.")


SELECTED_MODEL = select_available_vision_model()

# COMMAND ----------

# ============================================================
# Prompts
# ============================================================

def build_orientation_prompt(pdf_path: str, file_name: str, page_number: int) -> str:
    return f"""
Du siehst eine Montage derselben PDF-Seite in vier Rotationen: 0, 90, 180 und 270 Grad.
Waehle die Rotation, bei der Text und Tabellen am besten lesbar und aufrecht sind.

Dokument:
- pdf_path: {pdf_path}
- file_name: {file_name}
- page_number: {page_number}

Antworte ausschliesslich als valides JSON:
{{
  "best_rotation_degrees": 0,
  "confidence": 0.0,
  "reason": "kurze Begruendung"
}}

Zulaessige Werte fuer best_rotation_degrees: 0, 90, 180, 270.
""".strip()


def build_extraction_prompt(
    pdf_path: str,
    file_name: str,
    page_number: int,
    page_count: int,
    selected_rotation_degrees: int,
) -> str:
    return f"""
Du extrahierst ALLE Daten aus einer Seite eines Materialzeugnisses / Pruefbescheiningung.

Dokument: {file_name} | Seite {page_number}/{page_count} | Rotation: {selected_rotation_degrees}°

Bilder:
- PREVIOUS_CONTEXT_PAGE (falls vorhanden): NUR fuer Identifier-Kontext. Keine Werte daraus extrahieren.
- CURRENT_PAGE_TO_EXTRACT: Hieraus ALLE Werte extrahieren.

═══════════════════════════════════════════════════════════════
KRITISCHE REGEL: VOLLSTAENDIGKEIT
═══════════════════════════════════════════════════════════════

JEDE EINZELNE ZELLE in jeder Tabelle auf dieser Seite MUSS ein eigener Datenpunkt werden.
Wenn eine Tabelle 15 Zeilen x 8 Spalten hat, muessen 120 Datenpunkte entstehen.
Du darfst NICHTS auslassen, zusammenfassen oder ueberspringen.
Wenn Du unsicher bist, extrahiere trotzdem mit niedrigem confidence.

═══════════════════════════════════════════════════════════════
LOGIK: IDENTIFIER → WERTE
═══════════════════════════════════════════════════════════════

1. Identifiziere ALLE Identifier-Typen auf der Seite.
   Typische Identifier (variiert pro Dokument):
   - Heat No. / Schmelzen-Nr. / Cast No.
   - Pipe No. / Rohr-Nr. / Tube No.
   - Coil No.
   - Pipe Counter / lfd. Nr.
   - Specimen / Probe
   - Chargen-Nr. / Batch No. / Lot No.
   - Zeugnis-Nr. / Certificate No.
   - Item No. / Position
   Es koennen 1-6 verschiedene Identifier-Typen gleichzeitig vorkommen.

2. Fuer JEDE Zeile in jeder Tabelle:
   - Ordne die Zeile den richtigen Identifier-Werten zu
   - Extrahiere JEDEN Spaltenwert als eigenen Datenpunkt
   - Verwende den EXAKTEN Spaltenkopf als property_name

═══════════════════════════════════════════════════════════════
KOMPAKTES OUTPUT-FORMAT
═══════════════════════════════════════════════════════════════

Antwort als JSON:

{{
  "identifiers": [
    {{
      "type": "heat_number|pipe_number|coil_number|specimen_number|certificate_number|batch_number|item_number|other",
      "label": "sichtbare Beschriftung",
      "value": "Wert",
      "scope": "explicit|inferred|ambiguous"
    }}
  ],
  "datapoints": [
    {{
      "ids": {{"heat": "SQ31295", "pipe": "P-001", "coil": "C-42"}},
      "record_type": "chemical|tensile|impact|hardness|dimensional|heat_treatment|nde|pressure|product_info|certificate|compliance|other",
      "section": "Tabellenname oder Abschnitt",
      "prop": "EXAKTER Spaltenkopf oder Eigenschaftsname",
      "val": "Originalwert wie gedruckt",
      "num": null,
      "unit": "J|MPa|%|mm|degC|HV|HB|bar|text",
      "temp_c": null,
      "loc": "body|weld|HAZ|FL+2|unknown",
      "spec": "Proben-ID falls sichtbar",
      "lim_min": null,
      "lim_max": null,
      "conf": 0.95
    }}
  ],
  "page_info": {{
    "quality": "good|medium|poor",
    "tables_found": 0,
    "total_table_rows": 0
  }}
}}

WICHTIG zu "ids":
- Enthaelt ALLE Identifier die fuer diesen Datenpunkt gelten
- Keys sind Kurzformen: heat, pipe, coil, specimen, cert, batch, item, counter, other
- Wenn ein Identifier fuer ALLE Zeilen auf der Seite gilt, trotzdem in jedem Datenpunkt wiederholen

WICHTIG zu "prop":
- Verwende den EXAKTEN Text des Spaltenkopfes, z.B. "Absorbed Energy 1 (J)" nicht "KV_single_1"
- Wenn der Spaltenkopf mehrzeilig ist, kombiniere zu einem lesbaren Text
- Fuer Nicht-Tabellen-Werte: verwende die sichtbare Bezeichnung

WICHTIG zu "val":
- EXAKT wie gedruckt: "283", "0,42", "<0.01", "28-32", "ACCEPTABLE"
- Keine Interpretation oder Rundung

Beispiel - Impact Test Tabelle mit 3 Zeilen:
{{
  "datapoints": [
    {{"ids": {{"heat": "SQ31295", "pipe": "123"}}, "record_type": "impact", "section": "CHARPY IMPACT TEST", "prop": "Absorbed Energy 1 (J)", "val": "283", "num": 283, "unit": "J", "temp_c": -25, "loc": "body", "spec": "1", "lim_min": "45", "lim_max": null, "conf": 0.95}},
    {{"ids": {{"heat": "SQ31295", "pipe": "123"}}, "record_type": "impact", "section": "CHARPY IMPACT TEST", "prop": "Absorbed Energy 2 (J)", "val": "282", "num": 282, "unit": "J", "temp_c": -25, "loc": "body", "spec": "1", "lim_min": "45", "lim_max": null, "conf": 0.95}},
    {{"ids": {{"heat": "SQ31295", "pipe": "123"}}, "record_type": "impact", "section": "CHARPY IMPACT TEST", "prop": "Absorbed Energy 3 (J)", "val": "306", "num": 306, "unit": "J", "temp_c": -25, "loc": "body", "spec": "1", "lim_min": "45", "lim_max": null, "conf": 0.95}},
    {{"ids": {{"heat": "SQ31295", "pipe": "123"}}, "record_type": "impact", "section": "CHARPY IMPACT TEST", "prop": "Average (J)", "val": "290", "num": 290, "unit": "J", "temp_c": -25, "loc": "body", "spec": "1", "lim_min": "60", "lim_max": null, "conf": 0.95}},
    {{"ids": {{"heat": "SQ31295", "pipe": "123"}}, "record_type": "impact", "section": "CHARPY IMPACT TEST", "prop": "Shear (%)", "val": "100", "num": 100, "unit": "%", "temp_c": -25, "loc": "body", "spec": "1", "lim_min": "85", "lim_max": null, "conf": 0.95}},
    {{"ids": {{"heat": "SQ31641", "pipe": "124"}}, "record_type": "impact", "section": "CHARPY IMPACT TEST", "prop": "Absorbed Energy 1 (J)", "val": "198", "num": 198, "unit": "J", "temp_c": -25, "loc": "FL+2", "spec": "2", "lim_min": "45", "lim_max": null, "conf": 0.95}}
  ]
}}

Dieses Beispiel zeigt: JEDE Zelle = ein Datenpunkt. Nichts auslassen!

Antwort ausschliesslich als JSON. Kein Markdown, kein Text davor oder danach.
""".strip()

# COMMAND ----------

# ============================================================
# Orientierung und Seitenextraktion
# ============================================================

def detect_best_rotation(pdf_path: str, file_name: str, page_number: int) -> Tuple[int, float, str]:
    if not ENABLE_ORIENTATION_DETECTION:
        return 0, 1.0, "Orientation detection disabled."

    try:
        montage = make_orientation_collage(pdf_path, page_number)
        prompt = build_orientation_prompt(pdf_path, file_name, page_number)
        text, _ = call_model_images(
            model_name=SELECTED_MODEL,
            prompt=prompt,
            labelled_images=[("ORIENTATION_COLLAGE", montage)],
            max_tokens=600,
        )
        parsed = extract_json_from_model_text(text)
        rotation = int(parsed.get("best_rotation_degrees", 0))
        if rotation not in {0, 90, 180, 270}:
            rotation = 0
        confidence = clamp_confidence(parsed.get("confidence"), default=0.5)
        reason = str(parsed.get("reason", ""))
        return rotation, confidence, reason
    except Exception as exc:
        return 0, 0.0, f"Orientation detection failed; fallback 0 degrees: {repr(exc)}"


def process_single_page_parallel(row: Dict[str, Any]) -> Dict[str, Any]:
    created_at = utcnow_naive()
    t0 = time.time()

    response_text = None
    parsed = None
    error_message = None
    status = "ok"
    model_used = SELECTED_MODEL
    image_width_px = None
    image_height_px = None
    rotation = 0
    rotation_confidence = None
    rotation_reason = None

    try:
        rotation, rotation_confidence, rotation_reason = detect_best_rotation(
            pdf_path=row["pdf_path"],
            file_name=row["file_name"],
            page_number=int(row["page_number"]),
        )

        current_bytes, current_size, img_size_mb = load_page_image_bytes(
            pdf_path=row["pdf_path"],
            page_number=int(row["page_number"]),
        )
        image_width_px, image_height_px = current_size

        labelled_images: List[Tuple[str, bytes]] = []

        if INCLUDE_PREVIOUS_PAGE_AS_CONTEXT_IMAGE and int(row["page_number"]) > 1:
            try:
                previous_bytes, _, _ = load_page_image_bytes(
                    pdf_path=row["pdf_path"],
                    page_number=int(row["page_number"]) - 1,
                )
                labelled_images.append(("PREVIOUS_CONTEXT_PAGE", previous_bytes))
            except Exception:
                pass

        labelled_images.append(("CURRENT_PAGE_TO_EXTRACT", current_bytes))

        prompt = build_extraction_prompt(
            pdf_path=row["pdf_path"],
            file_name=row["file_name"],
            page_number=int(row["page_number"]),
            page_count=int(row["page_count"]),
            selected_rotation_degrees=rotation,
        )

        try:
            response_text, _ = call_model_images(
                model_name=SELECTED_MODEL,
                prompt=prompt,
                labelled_images=labelled_images,
                max_tokens=MAX_TOKENS,
            )
            model_used = SELECTED_MODEL
        except Exception as primary_exc:
            if SELECTED_MODEL != FALLBACK_MODEL:
                response_text, _ = call_model_images(
                    model_name=FALLBACK_MODEL,
                    prompt=prompt,
                    labelled_images=labelled_images,
                    max_tokens=MAX_TOKENS,
                )
                model_used = FALLBACK_MODEL
            else:
                raise primary_exc

        parsed = extract_json_from_model_text(response_text)

        # Speicher sofort freigeben
        del current_bytes
        del labelled_images
        gc.collect()

        for dp in parsed.get("datapoints", []) or []:
            dp["source"] = {
                "pdf_path": row["pdf_path"],
                "file_name": row["file_name"],
                "page_number": int(row["page_number"]),
            }

    except Exception as exc:
        status = "error"
        error_message = repr(exc) + "\n" + traceback.format_exc(limit=5)

    duration_s = time.time() - t0

    return {
        "run_id": row["run_id"],
        "document_id": row["document_id"],
        "page_id": row["page_id"],
        "pdf_path": row["pdf_path"],
        "file_name": row["file_name"],
        "page_number": int(row["page_number"]),
        "page_count": int(row["page_count"]),
        "model_name": model_used,
        "status": status,
        "duration_s": float(duration_s),
        "image_width_px": image_width_px,
        "image_height_px": image_height_px,
        "orientation_rotation_degrees": rotation,
        "orientation_confidence": rotation_confidence,
        "orientation_reason": rotation_reason,
        "response_text": response_text,
        "parsed_json": safe_json(parsed) if parsed is not None else None,
        "error_message": error_message,
        "extraction_prompt_version": EXTRACTION_PROMPT_VERSION,
        "created_at_utc": created_at,
    }


RAW_COLUMNS = [field.name for field in raw_schema.fields]


def process_pages_partition(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for pdf_batch in iterator:
        records = pdf_batch.to_dict("records")
        output_rows = []

        with ThreadPoolExecutor(max_workers=THREADS_PER_PARTITION) as executor:
            futures = [executor.submit(process_single_page_parallel, row) for row in records]
            for future in as_completed(futures):
                output_rows.append(future.result())

        gc.collect()
        yield pd.DataFrame(output_rows, columns=RAW_COLUMNS)

# COMMAND ----------

# ============================================================
# Manifest erstellen
# ============================================================

# ------------------------------------------------------------
# Re-Run-Steuerung: Quelllauf aufloesen und RUN_ID absichern
# ------------------------------------------------------------
SOURCE_RUN_ID_RESOLVED = ""
RETRY_OK_PAGE_IDS: set = set()          # Seiten mit status='ok' im Quelllauf
RETRY_SCOPE_DOCUMENT_IDS: set = set()   # Dokumente, die zum Quelllauf gehoeren

if RUN_MODE in ("retry_errors", "reverify"):
    src = SOURCE_RUN_ID
    if not src and table_exists(RAW_TABLE):
        try:
            r = (
                spark.table(RAW_TABLE)
                .where(F.col("run_id") != RUN_ID)
                .agg(F.max("run_id").alias("m"))
                .collect()
            )
            src = r[0]["m"] if r and r[0]["m"] else ""
        except Exception:
            src = ""

    if not src:
        print(f"Warnung: RUN_MODE='{RUN_MODE}', aber kein Quelllauf gefunden. Falle zurueck auf 'full'.")
        RUN_MODE = "full"
    else:
        SOURCE_RUN_ID_RESOLVED = src
        # RUN_ID darf NICHT mit dem Quelllauf kollidieren, sonst wuerde der
        # Re-Run den Originallauf ueberschreiben statt ihn zu historisieren.
        if RUN_ID == SOURCE_RUN_ID_RESOLVED:
            RUN_ID = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ") + "_rerun"
            print(f"RUN_ID kollidierte mit Quelllauf -> neue RUN_ID: {RUN_ID}")

        src_raw = spark.table(RAW_TABLE).where(F.col("run_id") == SOURCE_RUN_ID_RESOLVED)
        RETRY_SCOPE_DOCUMENT_IDS = {
            row["document_id"] for row in src_raw.select("document_id").distinct().collect()
        }
        RETRY_OK_PAGE_IDS = {
            row["page_id"]
            for row in src_raw.where(F.col("status") == "ok").select("page_id").distinct().collect()
        }
        print(
            f"Quelllauf {SOURCE_RUN_ID_RESOLVED}: "
            f"{len(RETRY_SCOPE_DOCUMENT_IDS)} Dokumente, "
            f"{len(RETRY_OK_PAGE_IDS)} bereits erfolgreiche Seiten."
        )

pdf_paths = list_pdfs(ROOT_PATH)
print(f"Gefundene PDF-Dateien: {len(pdf_paths)}")

# Optional: Nur bestimmte Unterordner verarbeiten (kommaseparierte Ordnernamen)
if DOCUMENT_FILTER:
    filter_folders = [f.strip() for f in DOCUMENT_FILTER.split(",")]
    pdf_paths = [p for p in pdf_paths if any(f"/{folder}/" in p or p.endswith(f"/{folder}") for folder in filter_folders)]
    print(f"Nach DOCUMENT_FILTER ({DOCUMENT_FILTER}): {len(pdf_paths)} PDFs")

if not DOCUMENT_FILTER and len(pdf_paths) != EXPECTED_PDF_COUNT:
    print(f"Warnung: Erwartet waren {EXPECTED_PDF_COUNT}, gefunden wurden {len(pdf_paths)}.")

manifest_rows: List[Dict[str, Any]] = []
pdf_open_error_rows: List[Dict[str, Any]] = []

for pdf_path in pdf_paths:
    file_name = Path(pdf_path).name
    document_id = sha1_short(pdf_path, 20)

    try:
        page_count = count_page_images(pdf_path)
        if page_count == 0:
            raise FileNotFoundError(f"Keine Seitenbilder fuer: {pdf_path}")

        for page_number in range(1, page_count + 1):
            page_id = f"{document_id}_p{page_number:04d}"
            manifest_rows.append({
                "run_id": RUN_ID,
                "document_id": document_id,
                "page_id": page_id,
                "pdf_path": pdf_path,
                "file_name": file_name,
                "page_number": page_number,
                "page_count": page_count,
            })

    except Exception as exc:
        pdf_open_error_rows.append({
            "run_id": RUN_ID,
            "document_id": document_id,
            "page_id": f"{document_id}_p0000",
            "pdf_path": pdf_path,
            "file_name": file_name,
            "page_number": 0,
            "page_count": 0,
            "model_name": SELECTED_MODEL,
            "status": "pdf_open_error",
            "duration_s": None,
            "image_width_px": None,
            "image_height_px": None,
            "orientation_rotation_degrees": None,
            "orientation_confidence": None,
            "orientation_reason": None,
            "response_text": None,
            "parsed_json": None,
            "error_message": repr(exc),
            "extraction_prompt_version": EXTRACTION_PROMPT_VERSION,
            "created_at_utc": utcnow_naive(),
        })

# ------------------------------------------------------------
# Im Re-Run nur die relevanten Seiten dieses Laufs behalten
# ------------------------------------------------------------
TARGET_PAGE_IDS: set = {m["page_id"] for m in manifest_rows}
if RUN_MODE in ("retry_errors", "reverify") and SOURCE_RUN_ID_RESOLVED:
    manifest_rows = [
        m for m in manifest_rows
        if m["document_id"] in RETRY_SCOPE_DOCUMENT_IDS
        and (RUN_MODE == "reverify" or m["page_id"] not in RETRY_OK_PAGE_IDS)
    ]
    pdf_open_error_rows = [
        r for r in pdf_open_error_rows
        if r["document_id"] in RETRY_SCOPE_DOCUMENT_IDS
        and (RUN_MODE == "reverify" or r["page_id"] not in RETRY_OK_PAGE_IDS)
    ]
    TARGET_PAGE_IDS = {m["page_id"] for m in manifest_rows}
    print(
        f"RUN_MODE={RUN_MODE}: {len(manifest_rows)} Seiten zur (Neu-)Verarbeitung "
        f"ausgewaehlt (aus {len(RETRY_SCOPE_DOCUMENT_IDS)} Dokumenten des Quelllaufs)."
    )
    if not manifest_rows and not pdf_open_error_rows:
        print("Hinweis: Keine offenen Seiten - im Quelllauf war bereits alles erfolgreich.")

manifest_df = spark.createDataFrame(manifest_rows, manifest_schema)

(
    manifest_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PAGE_MANIFEST_TABLE)
)

total_pages = manifest_df.count()
SPARK_PARTITIONS = min(
    MAX_SPARK_PARTITIONS,
    max(1, math.ceil(total_pages / TARGET_RECORDS_PER_PARTITION))
)

print(f"PDF-Seiten gesamt: {total_pages}")
print(f"Spark-Partitionen fuer Extraktion: {SPARK_PARTITIONS}")
print(f"Threads pro Partition: {THREADS_PER_PARTITION}")
print(f"Theoretische In-Flight-Obergrenze: ca. {SPARK_PARTITIONS * THREADS_PER_PARTITION}")
print("Hinweis: Effektiv begrenzt durch Serverless-Scaling und Model-Serving-Rate-Limits.")

display(manifest_df.groupBy("file_name").agg(F.count("*").alias("pages")).orderBy("file_name"))

# COMMAND ----------

# DBTITLE 1,Inkrementelle Extraktion (Dokument fuer Dokument)
# ============================================================
# Inkrementelle Extraktion (Dokument fuer Dokument)
# ============================================================
# Jedes Dokument wird einzeln verarbeitet und sofort zu Delta geschrieben.
# Bei Timeout/Restart: bereits verarbeitete Seiten werden uebersprungen.

from concurrent.futures import ThreadPoolExecutor, as_completed

# Bereits ERFOLGREICH verarbeitete Seiten dieses Runs pruefen.
# Wichtig: nur status='ok' zaehlt als erledigt. Fehlerseiten (Timeout,
# unleserlicher Scan) bleiben offen und werden bei einem Restart desselben
# Runs erneut versucht, statt als "fertig" uebersprungen zu werden.
try:
    already_done = set(
        spark.table(RAW_TABLE)
        .where((F.col("run_id") == RUN_ID) & (F.col("status") == "ok"))
        .select("page_id")
        .rdd.flatMap(lambda x: x)
        .collect()
    )
except Exception:
    already_done = set()

print(f"Bereits verarbeitet: {len(already_done)} Seiten")

# PDF-Open-Errors sofort schreiben
if pdf_open_error_rows:
    pdf_open_error_df = spark.createDataFrame(pdf_open_error_rows, raw_schema)
    append_or_create_delta(pdf_open_error_df, RAW_TABLE)
    already_done.update(r["page_id"] for r in pdf_open_error_rows)

# Alle Seiten dieses Runs laden
all_pages = (
    spark.table(PAGE_MANIFEST_TABLE)
    .where(F.col("run_id") == RUN_ID)
    .orderBy("file_name", "page_number")
    .toPandas()
)

# Seiten filtern die noch nicht verarbeitet sind
pending_pages = all_pages[~all_pages["page_id"].isin(already_done)]
print(f"Noch zu verarbeiten: {len(pending_pages)} / {len(all_pages)} Seiten")

# Dokument-weise gruppieren
doc_groups = pending_pages.groupby("document_id")
total_docs = len(doc_groups)
processed_docs = 0
total_pages_done = len(already_done)

for doc_id, doc_pages in doc_groups:
    doc_name = doc_pages.iloc[0]["file_name"]
    n_pages = len(doc_pages)
    processed_docs += 1
    
    print(f"\n[{processed_docs}/{total_docs}] {doc_name} ({n_pages} Seiten)")
    
    # Seiten parallel verarbeiten (ThreadPool fuer I/O-bound API calls)
    results = []
    records = doc_pages.to_dict("records")
    
    with ThreadPoolExecutor(max_workers=THREADS_PER_PARTITION) as executor:
        futures = {executor.submit(process_single_page_parallel, row): row for row in records}
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as exc:
                row = futures[future]
                results.append({
                    "run_id": row["run_id"],
                    "document_id": row["document_id"],
                    "page_id": row["page_id"],
                    "pdf_path": row["pdf_path"],
                    "file_name": row["file_name"],
                    "page_number": int(row["page_number"]),
                    "page_count": int(row["page_count"]),
                    "model_name": SELECTED_MODEL,
                    "status": "error",
                    "duration_s": 0.0,
                    "image_width_px": None,
                    "image_height_px": None,
                    "orientation_rotation_degrees": 0,
                    "orientation_confidence": None,
                    "orientation_reason": None,
                    "response_text": None,
                    "parsed_json": None,
                    "error_message": repr(exc),
                    "extraction_prompt_version": EXTRACTION_PROMPT_VERSION,
                    "created_at_utc": utcnow_naive(),
                })
    
    # Sofort zu Delta schreiben
    if results:
        results_df = spark.createDataFrame(results, raw_schema)
        append_or_create_delta(results_df, RAW_TABLE)
        
        ok_count = sum(1 for r in results if r["status"] == "ok")
        err_count = sum(1 for r in results if r["status"] == "error")
        avg_dur = sum(r["duration_s"] for r in results) / len(results)
        total_pages_done += len(results)
        
        print(f"  -> {ok_count} ok, {err_count} errors, avg {avg_dur:.1f}s/Seite")
        print(f"  -> Fortschritt: {total_pages_done}/{len(all_pages)} Seiten gesamt")
    
    gc.collect()

print(f"\n{'='*60}")
print(f"Extraktion abgeschlossen: {total_pages_done} Seiten verarbeitet")

# Re-Run-Bilanz: wie viele zuvor fehlgeschlagene Seiten konnten nun gelesen werden?
if RUN_MODE in ("retry_errors", "reverify") and SOURCE_RUN_ID_RESOLVED and TARGET_PAGE_IDS:
    try:
        cur_ok_ids = {
            r["page_id"]
            for r in (
                spark.table(RAW_TABLE)
                .where((F.col("run_id") == RUN_ID) & (F.col("status") == "ok"))
                .select("page_id")
                .distinct()
                .collect()
            )
        }
        n_target = len(TARGET_PAGE_IDS)
        n_fixed = len(TARGET_PAGE_IDS & cur_ok_ids)
        n_still_open = n_target - n_fixed
        print(f"\nRe-Run-Bilanz (Quelllauf {SOURCE_RUN_ID_RESOLVED}, Modus {RUN_MODE}):")
        print(f"  - erneut verarbeitete Seiten:        {n_target}")
        print(f"  - davon jetzt erfolgreich (ok):      {n_fixed}")
        print(f"  - weiterhin offen / fehlerhaft:      {n_still_open}")
        if RUN_MODE == "reverify":
            print("  Hinweis: Zweite Erfassung -> Confidence wird in der Konsolidierung angehoben (confirmed_by_rerun).")
    except Exception as _exc:
        print(f"Re-Run-Bilanz nicht berechenbar: {repr(_exc)}")

display(
    spark.table(RAW_TABLE)
    .where(F.col("run_id") == RUN_ID)
    .groupBy("status", "model_name")
    .agg(
        F.count("*").alias("pages"),
        F.avg("duration_s").alias("avg_duration_s"),
        F.max("duration_s").alias("max_duration_s"),
    )
    .orderBy("status", "model_name")
)

# COMMAND ----------

# ============================================================
# Flatten Identifier und Datenpunkte
# ============================================================

IDENTIFIER_COLUMNS = [field.name for field in identifier_schema.fields]
DATAPOINT_RUN_COLUMNS = [field.name for field in datapoint_runs_schema.fields]


# Mapping: kompakte ids-Keys -> identifier_type
_IDS_KEY_MAP = {
    "heat": "heat_number", "pipe": "pipe_number", "coil": "coil_number",
    "specimen": "specimen_number", "cert": "certificate_number",
    "batch": "batch_number", "item": "item_number", "counter": "pipe_counter",
    "tube": "tube_number", "charge": "charge_number", "other": "other",
}


def flatten_identifiers_from_parsed(
    parsed: Dict[str, Any],
    row: Dict[str, Any],
    created_at: datetime,
) -> List[Dict[str, Any]]:
    output = []

    for ident in parsed.get("identifiers", []) or []:
        # Support both old format (identifier_type/identifier_value) and new (type/value)
        ident_type = ident.get("type") or ident.get("identifier_type")
        ident_value = ident.get("value") or ident.get("identifier_value")
        ident_label = ident.get("label") or ident.get("identifier_label_raw")
        ident_id = sha1_short(
            safe_json({
                "run_id": row["run_id"],
                "page_id": row["page_id"],
                "type": ident_type,
                "value": ident_value,
                "label": ident_label,
            }),
            28,
        )

        output.append({
            "run_id": row["run_id"],
            "document_id": row["document_id"],
            "page_id": row["page_id"],
            "pdf_path": row["pdf_path"],
            "file_name": row["file_name"],
            "page_number": int(row["page_number"]),
            "identifier_id": ident_id,
            "identifier_type": ident_type,
            "identifier_label_raw": ident_label,
            "identifier_value": ident_value,
            "group_id": ident.get("group_id"),
            "scope": ident.get("scope"),
            "source_page_number": int(parse_float(ident.get("source_page_number")) or row["page_number"]),
            "evidence_text": ident.get("evidence_text"),
            "confidence_model": clamp_confidence(ident.get("conf", ident.get("confidence_model", ident.get("confidence"))), default=0.9),
            "created_at_utc": created_at,
        })

    return output


def _ids_dict_to_list(ids_dict: Any) -> List[Dict[str, Any]]:
    """Konvertiert kompaktes ids-Dict zu identifier-Liste.
    Input:  {"heat": "SQ31295", "pipe": "123"}
    Output: [{"identifier_type": "heat_number", "identifier_value": "SQ31295"}, ...]
    """
    if isinstance(ids_dict, list):
        return ids_dict  # Already old format
    if not isinstance(ids_dict, dict):
        return []
    result = []
    for key, val in ids_dict.items():
        if val is not None:
            result.append({
                "identifier_type": _IDS_KEY_MAP.get(key, key),
                "identifier_value": str(val),
            })
    return result


def flatten_datapoints_from_parsed(
    parsed: Dict[str, Any],
    row: Dict[str, Any],
    created_at: datetime,
) -> List[Dict[str, Any]]:
    output = []
    page_quality = parsed.get("page_quality", parsed.get("page_info", {})) or {}
    page_quality_json = safe_json(page_quality)

    for idx, dp in enumerate(parsed.get("datapoints", []) or [], start=1):
        # Support both old and new format
        ids = _ids_dict_to_list(dp.get("ids", dp.get("identifiers", [])))
        id_keys = identifier_keys(ids)

        # Property name: new format uses "prop", old uses "property_name"
        property_name = dp.get("prop") or dp.get("property_name")
        value_raw = dp.get("val") or dp.get("value_raw")
        value_num = dp.get("num") if dp.get("num") is not None else dp.get("value_num")
        specimen = dp.get("spec") or dp.get("specimen_or_sample")
        orientation = dp.get("loc") or dp.get("orientation")
        temp_c = dp.get("temp_c") if dp.get("temp_c") is not None else dp.get("test_temperature_c")
        lim_min = dp.get("lim_min") or dp.get("limit_min_raw")
        lim_max = dp.get("lim_max") or dp.get("limit_max_raw")
        conf = dp.get("conf") if dp.get("conf") is not None else dp.get("confidence_model")

        dp["source"] = {
            "pdf_path": row["pdf_path"],
            "file_name": row["file_name"],
            "page_number": int(row["page_number"]),
        }

        # Build stable slot key from prop + spec + ids
        value_slot_key = dp.get("value_slot_key")
        if not value_slot_key:
            slot_parts = [dp.get("record_type", "other"), property_name or ""]
            if specimen:
                slot_parts.append(str(specimen))
            value_slot_key = ":".join(p for p in slot_parts if p)

        confidence_model, rule_adjustment, needs_review = calculate_preverification_confidence(dp, page_quality)
        # Override confidence from model response if present
        if conf is not None:
            confidence_model = clamp_confidence(conf, default=0.5)

        stable_key = stable_key_from_dp(
            document_id=row["document_id"],
            page_number=int(row["page_number"]),
            dp=dp,
            ids=ids,
            fallback_index=idx,
        )
        fingerprint = value_fingerprint_from_dp(dp)

        datapoint_id = sha1_short(
            safe_json({
                "run_id": row["run_id"],
                "stable_datapoint_key": stable_key,
                "value_fingerprint": fingerprint,
            }),
            32,
        )

        output.append({
            "run_id": row["run_id"],
            "document_id": row["document_id"],
            "page_id": row["page_id"],
            "datapoint_id": datapoint_id,
            "stable_datapoint_key": stable_key,
            "value_fingerprint": fingerprint,
            "pdf_path": row["pdf_path"],
            "file_name": row["file_name"],
            "page_number": int(row["page_number"]),
            "model_name": row.get("model_name"),
            "orientation_rotation_degrees": int(row["orientation_rotation_degrees"]) if row.get("orientation_rotation_degrees") is not None else None,
            "record_type": dp.get("record_type"),
            "group_id": dp.get("group_id"),
            "identifier_keys": id_keys,
            "identifiers_json": safe_json(ids),
            "section": dp.get("section"),
            "table_name": dp.get("table_name") or dp.get("section"),
            "property_name": property_name,
            "value_slot_key": value_slot_key,
            "property_label_raw": dp.get("property_label_raw") or property_name,
            "test_standard": dp.get("test_standard"),
            "specimen_or_sample": specimen,
            "orientation": orientation,
            "test_temperature_c": parse_float(temp_c),
            "operator": dp.get("operator"),
            "value_raw": None if value_raw is None else str(value_raw),
            "value_num": parse_float(value_num),
            "unit": dp.get("unit"),
            "limit_min_raw": None if lim_min is None else str(lim_min),
            "limit_min_num": parse_float(lim_min),
            "limit_max_raw": None if lim_max is None else str(lim_max),
            "limit_max_num": parse_float(lim_max),
            "result_or_conformity": dp.get("result_or_conformity"),
            "evidence_text": dp.get("evidence_text"),
            "uncertainty_note": dp.get("uncertainty_note"),
            "confidence_model": confidence_model,
            "confidence_rule_adjustment": rule_adjustment,
            "confidence_final_pre_verification": max(0.0, min(1.0, confidence_model + rule_adjustment)),
            "confidence_final": max(0.0, min(1.0, confidence_model + rule_adjustment)),
            "verification_status": "new_in_run",
            "needs_human_review": needs_review,
            "page_quality_json": page_quality_json,
            "created_at_utc": created_at,
        })

    return output


def flatten_partition(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for batch in iterator:
        id_rows = []
        dp_rows = []

        for row in batch.to_dict("records"):
            if row.get("status") != "ok" or not row.get("parsed_json"):
                continue

            try:
                parsed = json.loads(row["parsed_json"])
                created_at = utcnow_naive()
                id_rows.extend(flatten_identifiers_from_parsed(parsed, row, created_at))
                dp_rows.extend(flatten_datapoints_from_parsed(parsed, row, created_at))
            except Exception:
                continue

        # This function is split below; retained only as reference.
        yield pd.DataFrame([], columns=[])


def flatten_identifiers_partition(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for batch in iterator:
        rows = []
        for row in batch.to_dict("records"):
            if row.get("status") != "ok" or not row.get("parsed_json"):
                continue
            try:
                parsed = json.loads(row["parsed_json"])
                rows.extend(flatten_identifiers_from_parsed(parsed, row, utcnow_naive()))
            except Exception:
                continue
        yield pd.DataFrame(rows, columns=IDENTIFIER_COLUMNS)


def flatten_datapoints_partition(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for batch in iterator:
        rows = []
        for row in batch.to_dict("records"):
            if row.get("status") != "ok" or not row.get("parsed_json"):
                continue
            try:
                parsed = json.loads(row["parsed_json"])
                rows.extend(flatten_datapoints_from_parsed(parsed, row, utcnow_naive()))
            except Exception:
                continue
        yield pd.DataFrame(rows, columns=DATAPOINT_RUN_COLUMNS)

raw_ok_this_run_df = (
    spark.table(RAW_TABLE)
    .where((F.col("run_id") == RUN_ID) & (F.col("status") == "ok"))
    .repartition(SPARK_PARTITIONS, "page_id")
)

identifier_df = raw_ok_this_run_df.mapInPandas(
    flatten_identifiers_partition,
    schema=identifier_schema,
)

datapoint_runs_df = raw_ok_this_run_df.mapInPandas(
    flatten_datapoints_partition,
    schema=datapoint_runs_schema,
)

append_or_create_delta(
    identifier_df.dropDuplicates(["run_id", "identifier_id"]),
    IDENTIFIER_TABLE,
)

append_or_create_delta(
    datapoint_runs_df.dropDuplicates(["run_id", "datapoint_id"]),
    DATAPOINT_RUNS_TABLE,
)

print(f"Append abgeschlossen:")
print(f"- {IDENTIFIER_TABLE}")
print(f"- {DATAPOINT_RUNS_TABLE}")

# COMMAND ----------

# ============================================================
# Re-Run-Konsolidierung und finale Konfidenz
# ============================================================

def rebuild_consolidated_datapoints() -> None:
    runs = spark.table(DATAPOINT_RUNS_TABLE)

    if runs.limit(1).count() == 0:
        empty_df = spark.createDataFrame([], datapoint_runs_schema)
        (
            empty_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(DATAPOINT_TABLE)
        )
        return

    latest_w = Window.partitionBy("stable_datapoint_key").orderBy(F.col("created_at_utc").desc(), F.col("run_id").desc())
    first_w = Window.partitionBy("stable_datapoint_key").orderBy(F.col("created_at_utc").asc(), F.col("run_id").asc())

    latest = (
        runs
        .withColumn("_rn_latest", F.row_number().over(latest_w))
        .where(F.col("_rn_latest") == 1)
        .drop("_rn_latest")
    )

    first = (
        runs
        .select("stable_datapoint_key", "run_id", "created_at_utc")
        .withColumn("_rn_first", F.row_number().over(first_w))
        .where(F.col("_rn_first") == 1)
        .select(
            F.col("stable_datapoint_key"),
            F.col("run_id").alias("first_seen_run_id"),
            F.col("created_at_utc").alias("first_seen_at_utc"),
        )
    )

    agg = (
        runs
        .groupBy("stable_datapoint_key")
        .agg(
            F.count("*").alias("extraction_count"),
            F.countDistinct("run_id").alias("run_count"),
            F.countDistinct("value_fingerprint").alias("distinct_value_count"),
            F.max("confidence_final_pre_verification").alias("max_pre_verification_confidence"),
            F.concat_ws(", ", F.array_sort(F.collect_set("run_id"))).alias("seen_run_ids"),
            F.to_json(
                F.collect_set(
                    F.struct(
                        "run_id",
                        "value_fingerprint",
                        "value_raw",
                        "value_num",
                        "unit",
                        "confidence_final_pre_verification",
                        "page_number",
                        "evidence_text",
                    )
                )
            ).alias("value_variants_json"),
        )
    )

    joined = latest.alias("l").join(agg.alias("a"), "stable_datapoint_key", "left").join(first.alias("f"), "stable_datapoint_key", "left")

    capped_confirmation_bonus = (F.least(F.col("a.extraction_count"), F.lit(6)) - F.lit(1)) * F.lit(0.03)

    current = (
        joined
        .select(
            "stable_datapoint_key",
            F.col("l.run_id").alias("last_seen_run_id"),
            F.col("f.first_seen_run_id"),
            F.col("f.first_seen_at_utc"),
            F.col("l.document_id"),
            F.col("l.page_id"),
            F.col("l.datapoint_id"),
            F.col("l.value_fingerprint"),
            F.col("l.pdf_path"),
            F.col("l.file_name"),
            F.col("l.page_number"),
            F.col("l.model_name"),
            F.col("l.orientation_rotation_degrees"),
            F.col("l.record_type"),
            F.col("l.group_id"),
            F.col("l.identifier_keys"),
            F.col("l.identifiers_json"),
            F.col("l.section"),
            F.col("l.table_name"),
            F.col("l.property_name"),
            F.col("l.value_slot_key"),
            F.col("l.property_label_raw"),
            F.col("l.test_standard"),
            F.col("l.specimen_or_sample"),
            F.col("l.orientation"),
            F.col("l.test_temperature_c"),
            F.col("l.operator"),
            F.col("l.value_raw"),
            F.col("l.value_num"),
            F.col("l.unit"),
            F.col("l.limit_min_raw"),
            F.col("l.limit_min_num"),
            F.col("l.limit_max_raw"),
            F.col("l.limit_max_num"),
            F.col("l.result_or_conformity"),
            F.col("l.evidence_text"),
            F.col("l.uncertainty_note"),
            F.col("l.confidence_model"),
            F.col("l.confidence_rule_adjustment"),
            F.col("l.confidence_final_pre_verification"),
            F.col("l.page_quality_json"),
            F.col("l.created_at_utc").alias("last_seen_at_utc"),
            F.col("a.extraction_count"),
            F.col("a.run_count"),
            F.col("a.distinct_value_count"),
            F.col("a.seen_run_ids"),
            F.col("a.value_variants_json"),
            F.when(F.col("a.distinct_value_count") > 1, F.lit("changed_or_conflicting"))
             .when(F.col("a.extraction_count") > 1, F.lit("confirmed_by_rerun"))
             .otherwise(F.lit("new")).alias("verification_status"),
            F.when(
                F.col("a.distinct_value_count") > 1,
                F.greatest(F.lit(0.05), F.round(F.col("l.confidence_final_pre_verification") * F.lit(0.65), 4)),
            ).when(
                F.col("a.extraction_count") > 1,
                F.least(
                    F.lit(0.99),
                    F.round(
                        F.greatest(
                            F.col("l.confidence_final_pre_verification"),
                            F.col("a.max_pre_verification_confidence"),
                        ) + capped_confirmation_bonus,
                        4,
                    ),
                ),
            ).otherwise(F.col("l.confidence_final_pre_verification")).alias("confidence_final"),
            F.col("l.needs_human_review").alias("_needs_human_review_pre"),
        )
        .withColumn(
            "needs_human_review",
            F.col("_needs_human_review_pre")
            | (F.col("distinct_value_count") > 1)
            | (F.col("confidence_final") < F.lit(CONFIDENCE_THRESHOLD_REVIEW)),
        )
        .drop("_needs_human_review_pre")
    )

    (
        current.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(DATAPOINT_TABLE)
    )


rebuild_consolidated_datapoints()

display(
    spark.table(DATAPOINT_TABLE)
    .groupBy("verification_status")
    .agg(
        F.count("*").alias("datapoints"),
        F.avg("confidence_final").alias("avg_confidence_final"),
    )
    .orderBy("verification_status")
)

# COMMAND ----------

# ============================================================
# Quality Checks
# ============================================================

spark.sql(f"""
CREATE OR REPLACE VIEW {QUALITY_VIEW} AS
SELECT
  'failed_pages_this_run' AS check_name,
  COUNT(*) AS n,
  'Seiten des aktuellen Runs, deren LETZTER Versuch fehlschlug (Rendering/Modellaufruf)' AS description
FROM (
  SELECT page_id, status,
         ROW_NUMBER() OVER (PARTITION BY page_id ORDER BY created_at_utc DESC) AS rn
  FROM {RAW_TABLE}
  WHERE run_id = '{RUN_ID}'
)
WHERE rn = 1 AND status <> 'ok'

UNION ALL

SELECT
  'datapoints_without_identifier_current' AS check_name,
  COUNT(*) AS n,
  'Konsolidierte Datenpunkte ohne Identifier; manuell pruefen' AS description
FROM {DATAPOINT_TABLE}
WHERE identifier_keys IS NULL OR TRIM(identifier_keys) = ''

UNION ALL

SELECT
  'low_confidence_current' AS check_name,
  COUNT(*) AS n,
  'Konsolidierte Datenpunkte mit confidence_final < {CONFIDENCE_THRESHOLD_REVIEW}' AS description
FROM {DATAPOINT_TABLE}
WHERE confidence_final IS NOT NULL AND confidence_final < {CONFIDENCE_THRESHOLD_REVIEW}

UNION ALL

SELECT
  'changed_or_conflicting_current' AS check_name,
  COUNT(*) AS n,
  'Wertslots mit abweichenden Ergebnissen ueber Re-Runs' AS description
FROM {DATAPOINT_TABLE}
WHERE verification_status = 'changed_or_conflicting'

UNION ALL

SELECT
  'needs_human_review_current' AS check_name,
  COUNT(*) AS n,
  'Konsolidierte Datenpunkte mit Review-Flag' AS description
FROM {DATAPOINT_TABLE}
WHERE needs_human_review = true

UNION ALL

SELECT
  'pages_without_datapoints_this_run' AS check_name,
  COUNT(*) AS n,
  'Erfolgreich verarbeitete Seiten des aktuellen Runs ohne extrahierte Datenpunkte' AS description
FROM {RAW_TABLE} r
LEFT ANTI JOIN (
  SELECT DISTINCT page_id
  FROM {DATAPOINT_RUNS_TABLE}
  WHERE run_id = '{RUN_ID}'
) d
ON r.page_id = d.page_id
WHERE r.run_id = '{RUN_ID}' AND r.status = 'ok'
""")

display(spark.table(QUALITY_VIEW))

display(
    spark.table(DATAPOINT_TABLE)
    .select(
        "file_name",
        "page_number",
        "record_type",
        "identifier_keys",
        "property_name",
        "value_raw",
        "unit",
        "confidence_final",
        "verification_status",
        "needs_human_review",
        "evidence_text",
    )
    .orderBy("file_name", "page_number", "record_type", "property_name")
    .limit(200)
)

# COMMAND ----------

# DBTITLE 1,CSV-Export (Spark native)
# ============================================================
# CSV-Export direkt aus Delta-Tabellen (Spark native, kein pandas)
# ============================================================

export_base = str(Path(EXPORT_ROOT_PATH) / RUN_ID)
Path(export_base).mkdir(parents=True, exist_ok=True)
print(f"Export-Verzeichnis: {export_base}")

# Dokumente dieses Runs
documents_df = (
    spark.table(PAGE_MANIFEST_TABLE)
    .where(F.col("run_id") == RUN_ID)
    .select("document_id", "pdf_path", "file_name")
    .distinct()
    .orderBy("file_name")
)

doc_list = documents_df.collect()
print(f"Dokumente zu exportieren: {len(doc_list)}")

export_results = []

for doc_row in doc_list:
    doc_id = doc_row["document_id"]
    file_name = doc_row["file_name"]
    pdf_path = doc_row["pdf_path"]
    
    # Ordnername: PDF-Stem + doc_id
    stem = Path(file_name).stem
    safe_name = re.sub(r'[^\w\-. ()]+', '_', stem).strip('_')[:80]
    doc_folder = Path(export_base) / f"{safe_name}__{doc_id}"
    doc_folder.mkdir(parents=True, exist_ok=True)
    
    try:
        # 01_All_Datapoints
        dp = spark.table(DATAPOINT_TABLE).where(F.col("document_id") == doc_id)
        dp_count = dp.count()
        dp.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding", "UTF-8").csv(str(doc_folder / "01_All_Datapoints"))
        
        # 02_Review (low confidence / conflicts)
        review = dp.where(
            (F.col("needs_human_review") == True)
            | (F.col("confidence_final") < CONFIDENCE_THRESHOLD_REVIEW)
            | (F.col("verification_status") == "changed_or_conflicting")
        )
        review.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding", "UTF-8").csv(str(doc_folder / "02_Review"))
        
        # 03_Identifiers - dokumentweit ueber ALLE Laeufe (dedupliziert).
        # So bleibt die Identifier-Liste vollstaendig, auch wenn dieser Lauf
        # nur einzelne (Fehler-)Seiten neu verarbeitet hat.
        ids = (
            spark.table(IDENTIFIER_TABLE)
            .where(F.col("document_id") == doc_id)
            .dropDuplicates(["page_number", "identifier_type", "identifier_value"])
        )
        ids.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding", "UTF-8").csv(str(doc_folder / "03_Identifiers"))

        # 04_Page_Status - pro Seite der AKTUELLE Status ueber alle Laeufe.
        # Eine Seite, die frueher fehlschlug und jetzt erfolgreich gelesen
        # wurde, erscheint hier mit ihrem neuen (besten) Status inkl. Quell-Run.
        page_w = Window.partitionBy("page_id").orderBy(
            F.when(F.col("status") == "ok", F.lit(1)).otherwise(F.lit(0)).desc(),
            F.col("created_at_utc").desc(),
        )
        raw = (
            spark.table(RAW_TABLE)
            .where(F.col("document_id") == doc_id)
            .withColumn("_rn", F.row_number().over(page_w))
            .where(F.col("_rn") == 1)
            .select("page_id", "page_number", "file_name", "status", "run_id", "model_name", "duration_s", "error_message")
            .orderBy("page_number")
        )
        raw.coalesce(1).write.mode("overwrite").option("header", "true").option("encoding", "UTF-8").csv(str(doc_folder / "04_Page_Status"))
        
        export_results.append({
            "run_id": RUN_ID,
            "document_id": doc_id,
            "file_name": file_name,
            "export_path": str(doc_folder),
            "datapoint_rows": dp_count,
            "created_at_utc": datetime.utcnow().isoformat(),
        })
        print(f"  OK: {file_name} ({dp_count} datapoints)")
        
    except Exception as exc:
        print(f"  FEHLER: {file_name}: {repr(exc)}")

# Master-Index als CSV
if export_results:
    index_df = spark.createDataFrame(export_results)
    index_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(Path(export_base) / "00_MASTER_INDEX"))
    print(f"\nMaster-Index: {export_base}/00_MASTER_INDEX/")

print(f"\nCSV-Export abgeschlossen: {len(export_results)} Dokumente")
print(f"Pfad: {export_base}")