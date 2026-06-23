# Databricks notebook source
# DBTITLE 1,Materialzeugnis Re-Flatten (Confidence-Korrektur ohne Neu-Extraktion)
# MAGIC %md
# MAGIC # Re-Flatten: Datenpunkte aus vorhandenem parsed_json neu berechnen
# MAGIC
# MAGIC Berechnet `identifiers` und `datapoints_runs` aus dem bereits gespeicherten
# MAGIC `parsed_json` **aller** Laeufe neu und baut anschliessend die konsolidierte
# MAGIC Tabelle `datapoints` neu auf. **Keine Modell-Calls.**
# MAGIC
# MAGIC Zweck: Die korrigierte Confidence-Logik (der faelschliche -0.20-Abzug fuer
# MAGIC "kein Identifier" griff zuvor bei jedem Wert) auf die **bereits erfassten**
# MAGIC Daten anwenden, ohne erneut zu extrahieren.
# MAGIC
# MAGIC **Quelle der Wahrheit:** `materialzeugnisse_raw_page_extractions.parsed_json`
# MAGIC (wird nur gelesen). Die Tabellen `identifiers`, `datapoints_runs` und
# MAGIC `datapoints` werden **ueberschrieben** und vollstaendig aus RAW reproduziert.
# MAGIC
# MAGIC Die Logik ist 1:1 aus `01_materialzeugnis_serverless_excel_export` uebernommen.

# COMMAND ----------

from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# COMMAND ----------

# DBTITLE 1,Parameter
CATALOG = "playground"
SCHEMA = "u_daniel_bick"

RAW_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_raw_page_extractions"
IDENTIFIER_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_identifiers"
DATAPOINT_RUNS_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints_runs"
DATAPOINT_TABLE = f"{CATALOG}.{SCHEMA}.materialzeugnisse_datapoints"

CONFIDENCE_THRESHOLD_REVIEW = 0.70

# Sicherheits-Schalter: nur wenn "true", werden die Tabellen ueberschrieben.
try:
    dbutils.widgets.dropdown("CONFIRM_OVERWRITE", "true", ["true", "false"])
    CONFIRM_OVERWRITE = dbutils.widgets.get("CONFIRM_OVERWRITE").strip().lower() == "true"
except Exception:
    CONFIRM_OVERWRITE = True

print(f"RAW_TABLE: {RAW_TABLE}")
print(f"CONFIRM_OVERWRITE: {CONFIRM_OVERWRITE}")

# COMMAND ----------

# DBTITLE 1,Hilfsfunktionen (1:1 aus der Pipeline)
def utcnow_naive() -> datetime:
    return datetime.utcnow()


def table_exists(table_name: str) -> bool:
    try:
        return spark.catalog.tableExists(table_name)
    except Exception:
        return False


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


def calculate_preverification_confidence(
    dp: Dict[str, Any],
    ids: List[Dict[str, Any]],
    page_quality: Dict[str, Any],
) -> tuple:
    base = clamp_confidence(
        dp.get("conf", dp.get("confidence_model", dp.get("confidence"))),
        default=0.55,
    )
    adjustment = 0.0

    # KORRIGIERT: ids wird uebergeben (zuvor wurde dp["identifiers"] gelesen, das im
    # kompakten Format nicht existiert -> jeder Wert bekam faelschlich -0.20).
    ids = ids or []
    if not ids:
        adjustment -= 0.20

    scopes = {normalise_text(i.get("scope")) for i in ids}
    if "ambiguous" in scopes:
        adjustment -= 0.12
    if "inferred_from_previous_page" in scopes:
        adjustment -= 0.07

    scan_quality = normalise_text(page_quality.get("quality", page_quality.get("scan_quality")))
    if scan_quality in ("poor", "bad"):
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


_IDS_KEY_MAP = {
    "heat": "heat_number", "pipe": "pipe_number", "coil": "coil_number",
    "specimen": "specimen_number", "cert": "certificate_number",
    "batch": "batch_number", "item": "item_number", "counter": "pipe_counter",
    "tube": "tube_number", "charge": "charge_number", "other": "other",
}


def _ids_dict_to_list(ids_dict: Any) -> List[Dict[str, Any]]:
    if isinstance(ids_dict, list):
        return ids_dict
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

# COMMAND ----------

# DBTITLE 1,Schemas
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

IDENTIFIER_COLUMNS = [f.name for f in identifier_schema.fields]
DATAPOINT_RUN_COLUMNS = [f.name for f in datapoint_runs_schema.fields]

# COMMAND ----------

# DBTITLE 1,Flatten-Funktionen (1:1, mit korrigierter Confidence)
def flatten_identifiers_from_parsed(parsed: Dict[str, Any], row: Dict[str, Any], created_at: datetime) -> List[Dict[str, Any]]:
    output = []
    for ident in parsed.get("identifiers", []) or []:
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


def flatten_datapoints_from_parsed(parsed: Dict[str, Any], row: Dict[str, Any], created_at: datetime) -> List[Dict[str, Any]]:
    output = []
    page_quality = parsed.get("page_quality", parsed.get("page_info", {})) or {}
    page_quality_json = safe_json(page_quality)

    for idx, dp in enumerate(parsed.get("datapoints", []) or [], start=1):
        ids = _ids_dict_to_list(dp.get("ids", dp.get("identifiers", [])))
        id_keys = identifier_keys(ids)

        property_name = dp.get("prop") or dp.get("property_name")
        value_raw = dp.get("val") or dp.get("value_raw")
        value_num = dp.get("num") if dp.get("num") is not None else dp.get("value_num")
        specimen = dp.get("spec") or dp.get("specimen_or_sample")
        orientation = dp.get("loc") or dp.get("orientation")
        temp_c = dp.get("temp_c") if dp.get("temp_c") is not None else dp.get("test_temperature_c")
        lim_min = dp.get("lim_min") or dp.get("limit_min_raw")
        lim_max = dp.get("lim_max") or dp.get("limit_max_raw")
        conf = dp.get("conf") if dp.get("conf") is not None else dp.get("confidence_model")

        value_slot_key = dp.get("value_slot_key")
        if not value_slot_key:
            slot_parts = [dp.get("record_type", "other"), property_name or ""]
            if specimen:
                slot_parts.append(str(specimen))
            value_slot_key = ":".join(p for p in slot_parts if p)

        confidence_model, rule_adjustment, needs_review = calculate_preverification_confidence(dp, ids, page_quality)
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


def _row_created_at(row: Dict[str, Any]) -> datetime:
    """Verwendet den Erfassungszeitpunkt der RAW-Seite, damit die spaetere
    Konsolidierung (latest/first je stable_key) die Lauf-Reihenfolge erhaelt."""
    val = row.get("created_at_utc")
    try:
        if val is None or pd.isna(val):
            return utcnow_naive()
    except Exception:
        pass
    return val.to_pydatetime() if hasattr(val, "to_pydatetime") else val


def flatten_identifiers_partition(iterator: Iterable[pd.DataFrame]) -> Iterable[pd.DataFrame]:
    for batch in iterator:
        rows = []
        for row in batch.to_dict("records"):
            if row.get("status") != "ok" or not row.get("parsed_json"):
                continue
            try:
                parsed = json.loads(row["parsed_json"])
                rows.extend(flatten_identifiers_from_parsed(parsed, row, _row_created_at(row)))
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
                rows.extend(flatten_datapoints_from_parsed(parsed, row, _row_created_at(row)))
            except Exception:
                continue
        yield pd.DataFrame(rows, columns=DATAPOINT_RUN_COLUMNS)

# COMMAND ----------

# DBTITLE 1,Vorher-Stand erfassen
before_stats = None
if table_exists(DATAPOINT_TABLE):
    try:
        before_stats = (
            spark.table(DATAPOINT_TABLE)
            .agg(
                F.count("*").alias("datapoints"),
                F.round(F.avg("confidence_final"), 4).alias("avg_confidence_final"),
                F.sum(F.col("needs_human_review").cast("int")).alias("needs_review"),
            )
            .collect()[0]
        )
        print("VORHER (konsolidiert):")
        print(f"  Datenpunkte:        {before_stats['datapoints']}")
        print(f"  Ø confidence_final: {before_stats['avg_confidence_final']}")
        print(f"  needs_human_review: {before_stats['needs_review']}")
    except Exception as exc:
        print(f"Vorher-Stand nicht lesbar: {repr(exc)}")
else:
    print("Noch keine konsolidierte Tabelle vorhanden (Erstlauf).")

# COMMAND ----------

# DBTITLE 1,Re-Flatten ueber ALLE Laeufe
if not CONFIRM_OVERWRITE:
    raise RuntimeError("CONFIRM_OVERWRITE=false -> Abbruch. Auf 'true' setzen, um neu zu berechnen.")

raw_ok = spark.table(RAW_TABLE).where(F.col("status") == "ok")
n_pages = raw_ok.count()
SPARK_PARTITIONS = min(64, max(1, math.ceil(n_pages / 200)))
print(f"OK-Seiten ueber alle Laeufe: {n_pages}  ->  {SPARK_PARTITIONS} Partitionen")

raw_ok = raw_ok.repartition(SPARK_PARTITIONS, "page_id")

identifier_df = raw_ok.mapInPandas(flatten_identifiers_partition, schema=identifier_schema)
datapoint_runs_df = raw_ok.mapInPandas(flatten_datapoints_partition, schema=datapoint_runs_schema)

(
    identifier_df.dropDuplicates(["run_id", "identifier_id"]).write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(IDENTIFIER_TABLE)
)
(
    datapoint_runs_df.dropDuplicates(["run_id", "datapoint_id"]).write
    .format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(DATAPOINT_RUNS_TABLE)
)

print(f"Neu geschrieben: {IDENTIFIER_TABLE} ({spark.table(IDENTIFIER_TABLE).count()} Zeilen)")
print(f"Neu geschrieben: {DATAPOINT_RUNS_TABLE} ({spark.table(DATAPOINT_RUNS_TABLE).count()} Zeilen)")

# COMMAND ----------

# DBTITLE 1,Konsolidierung neu aufbauen (1:1 aus der Pipeline)
def rebuild_consolidated_datapoints() -> None:
    runs = spark.table(DATAPOINT_RUNS_TABLE)

    if runs.limit(1).count() == 0:
        empty_df = spark.createDataFrame([], datapoint_runs_schema)
        (
            empty_df.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").saveAsTable(DATAPOINT_TABLE)
        )
        return

    latest_w = Window.partitionBy("stable_datapoint_key").orderBy(F.col("created_at_utc").desc(), F.col("run_id").desc())
    first_w = Window.partitionBy("stable_datapoint_key").orderBy(F.col("created_at_utc").asc(), F.col("run_id").asc())

    latest = (
        runs.withColumn("_rn_latest", F.row_number().over(latest_w))
        .where(F.col("_rn_latest") == 1).drop("_rn_latest")
    )

    first = (
        runs.select("stable_datapoint_key", "run_id", "created_at_utc")
        .withColumn("_rn_first", F.row_number().over(first_w))
        .where(F.col("_rn_first") == 1)
        .select(
            F.col("stable_datapoint_key"),
            F.col("run_id").alias("first_seen_run_id"),
            F.col("created_at_utc").alias("first_seen_at_utc"),
        )
    )

    agg = (
        runs.groupBy("stable_datapoint_key").agg(
            F.count("*").alias("extraction_count"),
            F.countDistinct("run_id").alias("run_count"),
            F.countDistinct("value_fingerprint").alias("distinct_value_count"),
            F.max("confidence_final_pre_verification").alias("max_pre_verification_confidence"),
            F.concat_ws(", ", F.array_sort(F.collect_set("run_id"))).alias("seen_run_ids"),
            F.to_json(
                F.collect_set(
                    F.struct(
                        "run_id", "value_fingerprint", "value_raw", "value_num",
                        "unit", "confidence_final_pre_verification", "page_number", "evidence_text",
                    )
                )
            ).alias("value_variants_json"),
        )
    )

    joined = latest.alias("l").join(agg.alias("a"), "stable_datapoint_key", "left").join(first.alias("f"), "stable_datapoint_key", "left")
    capped_confirmation_bonus = (F.least(F.col("a.extraction_count"), F.lit(6)) - F.lit(1)) * F.lit(0.03)

    current = (
        joined.select(
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
        current.write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(DATAPOINT_TABLE)
    )


rebuild_consolidated_datapoints()
print(f"Konsolidiert neu aufgebaut: {DATAPOINT_TABLE}")

# COMMAND ----------

# DBTITLE 1,Nachher-Stand und Vergleich
after = (
    spark.table(DATAPOINT_TABLE)
    .agg(
        F.count("*").alias("datapoints"),
        F.round(F.avg("confidence_final"), 4).alias("avg_confidence_final"),
        F.sum(F.col("needs_human_review").cast("int")).alias("needs_review"),
    )
    .collect()[0]
)

print("NACHHER (konsolidiert):")
print(f"  Datenpunkte:        {after['datapoints']}")
print(f"  Ø confidence_final: {after['avg_confidence_final']}")
print(f"  needs_human_review: {after['needs_review']}")
if before_stats is not None:
    print("\nVeraenderung:")
    print(f"  Ø confidence_final: {before_stats['avg_confidence_final']} -> {after['avg_confidence_final']}")
    print(f"  needs_human_review: {before_stats['needs_review']} -> {after['needs_review']}")

# Verteilung der finalen Confidence in Buckets
display(
    spark.table(DATAPOINT_TABLE)
    .withColumn(
        "confidence_bucket",
        F.when(F.col("confidence_final") >= 0.95, F.lit("0.95-1.00"))
         .when(F.col("confidence_final") >= 0.90, F.lit("0.90-0.95"))
         .when(F.col("confidence_final") >= 0.80, F.lit("0.80-0.90"))
         .when(F.col("confidence_final") >= 0.70, F.lit("0.70-0.80"))
         .otherwise(F.lit("< 0.70")),
    )
    .groupBy("confidence_bucket")
    .agg(F.count("*").alias("datapoints"))
    .orderBy("confidence_bucket")
)

display(
    spark.table(DATAPOINT_TABLE)
    .groupBy("verification_status")
    .agg(
        F.count("*").alias("datapoints"),
        F.round(F.avg("confidence_final"), 4).alias("avg_confidence_final"),
    )
    .orderBy("verification_status")
)
