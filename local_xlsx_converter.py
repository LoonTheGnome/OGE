#!/usr/bin/env python3
"""Lokaler Konverter: Parquet (aus Notebook 05) -> zwei xlsx pro Dokument.

Laeuft auf dem eigenen Rechner (z.B. Laptop in VS Code), NICHT auf Databricks.
So entsteht das speicherhungrige xlsx dort, wo genug RAM ist.

Erzeugt je Dokument zwei Dateien in getrennten Baeumen, benannt mit der Nummer
des Herkunftsordners:
    <output>/mit_run_info/<ordnernummer>/<stem>.xlsx   (alle Felder)
    <output>/ohne_run_info/<ordnernummer>/<stem>.xlsx  (ohne Run-/Technik-Felder)

Sheets je Datei: 00_Overview, 01_All_Datapoints, 02_Review, 03_Identifiers,
04_Page_Status, fachliche Sheets je record_type (10_Chemistry ... 21_Other) und
Wide-Pivots fuer Chemie/Zug/Kerbschlag/Haerte.

Voraussetzungen:
    pip install pandas pyarrow xlsxwriter

Aufruf:
    python local_xlsx_converter.py --input ./_export_parquet --output ./xlsx
    # erneut aufrufen -> ueberspringt bereits erzeugte Dateien (resumebar)
    python local_xlsx_converter.py --input ./_export_parquet --output ./xlsx --overwrite
"""
from __future__ import annotations

import argparse
import os
import re
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from pathlib import Path

import pandas as pd

# ------------------------------------------------------------------
# Konfiguration (identisch zur Notebook-Logik)
# ------------------------------------------------------------------
CONFIDENCE_THRESHOLD_REVIEW = 0.70

RUN_META_COLUMNS = {
    "confidence_model", "confidence_rule_adjustment", "confidence_final_pre_verification",
    "model_name", "orientation_rotation_degrees",
    "last_seen_run_id", "first_seen_run_id", "seen_run_ids",
    "extraction_count", "run_count", "distinct_value_count",
    "first_seen_at_utc", "last_seen_at_utc",
    "document_id", "page_id", "datapoint_id",
    "stable_datapoint_key", "value_fingerprint", "value_slot_key",
    "identifiers_json", "value_variants_json",
}

PREFERRED_DP_COLS = [
    "file_name", "page_number", "record_type", "section", "table_name",
    "identifier_keys", "group_id", "specimen_or_sample", "orientation",
    "test_temperature_c", "property_name", "property_label_raw",
    "value_raw", "value_num", "unit", "operator",
    "limit_min_raw", "limit_min_num", "limit_max_raw", "limit_max_num",
    "result_or_conformity", "test_standard",
    "confidence_final", "verification_status", "needs_human_review",
    "evidence_text", "uncertainty_note", "pdf_path",
    "confidence_model", "confidence_rule_adjustment", "confidence_final_pre_verification",
    "model_name", "orientation_rotation_degrees",
    "last_seen_run_id", "first_seen_run_id", "seen_run_ids",
    "extraction_count", "run_count", "distinct_value_count",
    "first_seen_at_utc", "last_seen_at_utc",
    "document_id", "page_id", "datapoint_id",
    "stable_datapoint_key", "value_fingerprint", "value_slot_key",
    "identifiers_json", "value_variants_json",
]

RECORD_SHEETS = [
    ("chemical", "10_Chemistry"), ("tensile", "11_Tensile"), ("impact", "12_Impact"),
    ("hardness", "13_Hardness"), ("dimensional", "14_Dimensions"),
    ("heat_treatment", "15_Heat_Treatment"), ("nde", "16_NDE"),
    ("pressure", "17_Pressure"), ("product_info", "18_Product_Info"),
    ("certificate", "19_Certificate"), ("compliance", "20_Compliance"),
    ("other", "21_Other"),
]
WIDE_RECORD_TYPES = {"chemical", "tensile", "impact", "hardness"}

IDENTIFIER_DROP_CLEAN = {"run_id", "identifier_id", "document_id", "page_id", "created_at_utc"}
PAGESTATUS_DROP_CLEAN = {"run_id", "model_name", "duration_s", "created_at_utc"}
PAGESTATUS_COLS = ["page_id", "page_number", "file_name", "status", "run_id",
                   "model_name", "duration_s", "error_message"]

FULL_SUBDIR = "mit_run_info"
CLEAN_SUBDIR = "ohne_run_info"

# Excel-Zeilenlimit pro Sheet inkl. Kopfzeile = 1.048.576 -> max. Datenzeilen.
# Sheets mit mehr Zeilen werden AUTOMATISCH (verlustfrei) auf mehrere Sheets
# aufgeteilt - bevorzugt an Seitengrenzen. Es wird NICHTS gekuerzt.
EXCEL_MAX_DATA_ROWS = 1_048_575


# ------------------------------------------------------------------
# Hilfsfunktionen
# ------------------------------------------------------------------
def origin_folder_number(pdf_path: str) -> str:
    folder = Path(str(pdf_path)).parent.name
    match = re.search(r"\d+(?:[._]\d+)*", folder)
    raw = match.group(0) if match else folder
    return sanitize_name(raw) or "unbekannt"


def sanitize_name(name: str, max_len: int = 120) -> str:
    base = re.sub(r"[^\w\-. ()]+", "_", str(name), flags=re.UNICODE).strip(" _.")
    return base[:max_len]


def safe_sheet_name(name: str, used: set) -> str:
    clean = re.sub(r"[\[\]\:\*\?\/\\]", "_", str(name))[:31] or "Sheet"
    candidate, i = clean, 1
    while candidate.lower() in used:
        suffix = f"_{i}"
        candidate = clean[: 31 - len(suffix)] + suffix
        i += 1
    used.add(candidate.lower())
    return candidate


def reorder_columns(df: pd.DataFrame, preferred) -> pd.DataFrame:
    cols = [c for c in preferred if c in df.columns] + [c for c in df.columns if c not in preferred]
    return df[cols]


def drop_columns(df: pd.DataFrame, drop) -> pd.DataFrame:
    return df[[c for c in df.columns if c not in drop]]


def make_wide_sheet(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty or "property_name" not in df.columns or "value_raw" not in df.columns:
        return pd.DataFrame()
    index_cols = [c for c in [
        "file_name", "page_number", "identifier_keys", "record_type", "section",
        "table_name", "specimen_or_sample", "orientation", "test_temperature_c",
    ] if c in df.columns]
    if not index_cols:
        return pd.DataFrame()
    tmp = df.copy()
    tmp["property_name"] = tmp["property_name"].fillna("unknown_property").astype(str)
    try:
        # WICHTIG: KEIN dropna=False - das erzeugt bei MultiIndex das kartesische
        # Produkt aller Index-Werte (Millionen Zeilen). Default haelt nur die
        # tatsaechlich vorkommenden Kombinationen.
        wide = tmp.pivot_table(
            index=index_cols, columns="property_name", values="value_raw",
            aggfunc=lambda x: " | ".join([str(v) for v in x if pd.notna(v)]),
        ).reset_index()
        wide.columns = [str(c) for c in wide.columns]
        return wide
    except Exception:
        return pd.DataFrame()


def _clean_for_excel(df: pd.DataFrame) -> pd.DataFrame:
    """NaN/NaT/None -> leere Zellen (versionsunabhaengig, ohne applymap).
    Inf wird zusaetzlich ersetzt; der Writer hat als Netz nan_inf_to_errors=True.
    """
    if df is None or df.empty:
        return df
    out = df.where(pd.notna(df), "")
    return out.replace([float("inf"), float("-inf")], "")


def _split_frame(df: pd.DataFrame, max_rows: int = EXCEL_MAX_DATA_ROWS):
    """Teilt einen DataFrame verlustfrei in moeglichst WENIGE Teile <= max_rows.

    Schneidet bevorzugt an Seitengrenzen (df ist nach page_number sortiert), sodass
    keine Seite ueber zwei Sheets zerrissen wird. Jeder Teil wird maximal gefuellt.
    Nur falls eine einzelne Seite > max_rows haette (praktisch nie: >1 Mio Zeilen),
    wird hart geteilt - auch dann ohne Datenverlust.
    """
    n = len(df)
    if n <= max_rows:
        return [df]

    if "page_number" in df.columns:
        pages = df["page_number"].tolist()
        boundaries = [0] + [i for i in range(1, n) if pages[i] != pages[i - 1]] + [n]
    else:
        boundaries = list(range(0, n, max_rows)) + [n]

    parts = []
    cur = 0
    for b in range(1, len(boundaries)):
        if boundaries[b] - cur > max_rows:
            prev = boundaries[b - 1]
            if prev > cur:
                parts.append(df.iloc[cur:prev])
                cur = prev
            while boundaries[b] - cur > max_rows:  # Einzelseite groesser als Limit
                parts.append(df.iloc[cur:cur + max_rows])
                cur += max_rows
    if cur < n:
        parts.append(df.iloc[cur:n])
    return parts


def write_xlsx(sheets, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    used: set = set()
    # WICHTIG: KEIN constant_memory! In Kombination mit pandas.to_excel (das
    # spaltenweise schreibt) verwirft constant_memory fast alle Zeilen -> Dateien
    # waren "fast leer". Lokal (genug RAM) ist der Normalmodus problemlos.
    with pd.ExcelWriter(target_path, engine="xlsxwriter",
                        engine_kwargs={"options": {"nan_inf_to_errors": True}}) as writer:
        wrote = False
        for raw_name, df in sheets:
            frame = df if isinstance(df, pd.DataFrame) else pd.DataFrame()
            # Verlustfrei: bei Ueberschreitung des Zeilenlimits auf mehrere SHEETS
            # (in DERSELBEN Datei) aufteilen - es entstehen KEINE zusaetzlichen Dateien.
            chunks = _split_frame(frame, EXCEL_MAX_DATA_ROWS)
            multi = len(chunks) > 1
            for ci, chunk in enumerate(chunks, start=1):
                sub_name = f"{raw_name}_{ci}" if multi else raw_name
                name = safe_sheet_name(sub_name, used)
                _clean_for_excel(chunk).to_excel(writer, sheet_name=name, index=False, freeze_panes=(1, 0))
                ws = writer.sheets[name]
                n_rows, n_cols = chunk.shape
                if n_rows > 0 and n_cols > 0:
                    ws.autofilter(0, 0, n_rows, n_cols - 1)
                wrote = True
            if multi:
                print(f"    Hinweis: '{raw_name}' auf {len(chunks)} Sheets aufgeteilt "
                      f"(Excel-Zeilenlimit; verlustfrei, an Seitengrenzen).", flush=True)
        if not wrote:
            pd.DataFrame().to_excel(writer, sheet_name="leer", index=False)


def build_sheets(dp_doc, ids_doc, pages_doc, meta, include_run_info):
    dp = dp_doc.copy()
    ids = ids_doc.copy()
    pages = pages_doc.copy()
    dp_total = int(len(dp))

    if not include_run_info and not dp.empty:
        dp = drop_columns(dp, RUN_META_COLUMNS)
    dp = reorder_columns(dp, PREFERRED_DP_COLS)

    if not dp.empty:
        m_rev = dp["needs_human_review"] == True if "needs_human_review" in dp.columns else pd.Series(False, index=dp.index)
        m_low = dp["confidence_final"].fillna(0.0) < CONFIDENCE_THRESHOLD_REVIEW if "confidence_final" in dp.columns else pd.Series(False, index=dp.index)
        m_con = dp["verification_status"] == "changed_or_conflicting" if "verification_status" in dp.columns else pd.Series(False, index=dp.index)
        review = dp[m_rev | m_low | m_con]
    else:
        review = dp.iloc[0:0]

    if not include_run_info:
        ids = drop_columns(ids, IDENTIFIER_DROP_CLEAN)
        pages = drop_columns(pages, PAGESTATUS_DROP_CLEAN)

    overview_rows = [
        {"field": "file_name", "value": meta["file_name"]},
        {"field": "origin_folder_number", "value": meta["origin_folder"]},
        {"field": "pdf_path", "value": meta["pdf_path"]},
        {"field": "datapoints_total", "value": dp_total},
        {"field": "review_rows", "value": int(len(review))},
        {"field": "identifier_rows", "value": int(len(ids))},
        {"field": "page_rows", "value": int(len(pages))},
        {"field": "version", "value": "full (mit Run-Infos)" if include_run_info else "clean (ohne Run-Infos)"},
    ]
    if include_run_info:
        models = sorted({str(x) for x in dp_doc.get("model_name", pd.Series(dtype=str)).dropna().unique()}) if not dp_doc.empty else []
        run_ids = sorted({str(x) for x in dp_doc.get("seen_run_ids", pd.Series(dtype=str)).dropna().unique()}) if not dp_doc.empty else []
        overview_rows += [
            {"field": "document_id", "value": meta["document_id"]},
            {"field": "model_names", "value": ", ".join(models)},
            {"field": "seen_run_ids", "value": " ; ".join(run_ids)},
        ]

    sheets = [
        ("00_Overview", pd.DataFrame(overview_rows)),
        ("01_All_Datapoints", dp),
        ("02_Review", review),
        ("03_Identifiers", ids),
        ("04_Page_Status", pages),
    ]

    if not dp.empty and "record_type" in dp.columns:
        for record_type, sheet_name in RECORD_SHEETS:
            sub = dp[dp["record_type"] == record_type]
            if sub.empty:
                continue
            sheets.append((sheet_name, sub))
            if record_type in WIDE_RECORD_TYPES:
                wide = make_wide_sheet(sub)
                if not wide.empty:
                    sheets.append((f"{sheet_name}_Wide", wide))
    return sheets


def _slice_doc(dp_all, ids_all, pages_all, document_id):
    """Schneidet die drei Frames auf ein Dokument zu (im Hauptprozess)."""
    dp_doc = dp_all[dp_all["document_id"] == document_id].sort_values(
        [c for c in ["page_number", "record_type", "identifier_keys", "property_name"] if c in dp_all.columns]
    )
    if "document_id" in ids_all.columns:
        ids_doc = ids_all[ids_all["document_id"] == document_id]
        if not ids_doc.empty:
            ids_doc = ids_doc.sort_values([c for c in ["page_number", "identifier_type", "identifier_value"] if c in ids_doc.columns])
    else:
        ids_doc = ids_all.iloc[0:0]
    if "document_id" in pages_all.columns:
        pages_doc = pages_all[pages_all["document_id"] == document_id]
    else:
        pages_doc = pages_all.iloc[0:0]
    if not pages_doc.empty and "page_number" in pages_doc.columns:
        pages_doc = pages_doc.sort_values("page_number")
    pages_doc = pages_doc[[c for c in PAGESTATUS_COLS if c in pages_doc.columns]]
    return dp_doc, ids_doc, pages_doc


def build_and_write(meta, dp_doc, ids_doc, pages_doc, full_path, clean_path):
    """Erzeugt beide xlsx-Versionen fuer EIN Dokument. Modul-Ebene -> picklebar
    fuer ProcessPoolExecutor."""
    write_xlsx(build_sheets(dp_doc, ids_doc, pages_doc, meta, True), Path(full_path))
    write_xlsx(build_sheets(dp_doc, ids_doc, pages_doc, meta, False), Path(clean_path))
    return int(len(dp_doc))


def main():
    ap = argparse.ArgumentParser(description="Parquet -> zwei xlsx pro Dokument (lokal).")
    ap.add_argument("--input", required=True, help="Ordner mit datapoints/ identifiers/ page_status/ (Parquet).")
    ap.add_argument("--output", required=True, help="Zielordner fuer die xlsx-Baeume.")
    ap.add_argument("--overwrite", action="store_true", help="Vorhandene xlsx ueberschreiben (sonst ueberspringen).")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) - 2),
                    help="Parallele Prozesse (Default: CPU-Kerne - 2; 1 = sequenziell). "
                         "GPU/NPU werden nicht genutzt - xlsx ist reine CPU-Arbeit.")
    args = ap.parse_args()

    in_root = Path(args.input)
    out_root = Path(args.output)

    print("Lese Parquet ...", flush=True)
    dp_all = pd.read_parquet(in_root / "datapoints")
    ids_all = pd.read_parquet(in_root / "identifiers")
    pages_all = pd.read_parquet(in_root / "page_status")
    print(f"  datapoints: {len(dp_all)} Zeilen, identifiers: {len(ids_all)}, page_status: {len(pages_all)}", flush=True)

    pages_cols = [c for c in PAGESTATUS_COLS if c in pages_all.columns]
    pages_all = pages_all[pages_cols + [c for c in ["document_id"] if c in pages_all.columns]]

    docs = (
        dp_all[["document_id", "pdf_path", "file_name"]]
        .drop_duplicates()
        .sort_values("file_name")
        .to_dict("records")
    )
    total = len(docs)

    # Aufgaben vorbereiten und bereits vorhandene (resume) ueberspringen.
    tasks = []  # (meta, full_path, clean_path)
    skipped = 0
    for doc in docs:
        document_id = doc["document_id"]
        folder_number = origin_folder_number(doc["pdf_path"])
        stem = sanitize_name(Path(str(doc["file_name"])).stem) or str(document_id)
        full_path = out_root / FULL_SUBDIR / folder_number / f"{stem}.xlsx"
        clean_path = out_root / CLEAN_SUBDIR / folder_number / f"{stem}.xlsx"
        if not args.overwrite and full_path.exists() and clean_path.exists():
            skipped += 1
            continue
        meta = {"document_id": document_id, "pdf_path": doc["pdf_path"],
                "file_name": doc["file_name"], "origin_folder": folder_number}
        tasks.append((meta, full_path, clean_path))

    workers = max(1, args.workers)
    print(f"Dokumente: {total} | zu erzeugen: {len(tasks)} | uebersprungen: {skipped} | Workers: {workers}", flush=True)

    done, errors = 0, []

    def handle_result(meta, fut):
        nonlocal done
        try:
            n = fut.result()
            done += 1
            print(f"OK [{meta['origin_folder']}] {meta['file_name']} ({n} DP)", flush=True)
        except Exception as exc:
            errors.append((meta["file_name"], repr(exc)))
            print(f"FEHLER [{meta['origin_folder']}] {meta['file_name']}: {repr(exc)}", flush=True)

    if workers == 1:
        for meta, fp, cp in tasks:
            try:
                n = build_and_write(meta, *_slice_doc(dp_all, ids_all, pages_all, meta["document_id"]), fp, cp)
                done += 1
                print(f"OK [{meta['origin_folder']}] {meta['file_name']} ({n} DP)", flush=True)
            except Exception as exc:
                errors.append((meta["file_name"], repr(exc)))
                print(f"FEHLER [{meta['origin_folder']}] {meta['file_name']}: {repr(exc)}", flush=True)
    else:
        # Bounded submission: nie mehr als ~2*workers Slices gleichzeitig im Speicher.
        it = iter(tasks)
        meta_by_future = {}
        with ProcessPoolExecutor(max_workers=workers) as ex:
            def submit_next():
                try:
                    meta, fp, cp = next(it)
                except StopIteration:
                    return False
                dp_doc, ids_doc, pages_doc = _slice_doc(dp_all, ids_all, pages_all, meta["document_id"])
                fut = ex.submit(build_and_write, meta, dp_doc, ids_doc, pages_doc, str(fp), str(cp))
                meta_by_future[fut] = meta
                return True

            for _ in range(workers * 2):
                if not submit_next():
                    break
            while meta_by_future:
                finished, _pending = wait(set(meta_by_future), return_when=FIRST_COMPLETED)
                for fut in finished:
                    handle_result(meta_by_future.pop(fut), fut)
                    submit_next()

    print(f"\nFertig: {done} erzeugt, {skipped} uebersprungen, {len(errors)} Fehler.")
    if errors:
        for fn, err in errors:
            print(f"  - {fn}: {err}")


if __name__ == "__main__":
    main()
