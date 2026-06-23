# Refactoring: PyMuPDF entfernen, Volume-Bilder nutzen

## Ziel-Notebook
`01_materialzeugnis_serverless_excel_export` (ID: 3259836811338600)

## Vorgerenderte Bilder
- **Pfad:** `/Volumes/playground/u_daniel_bick/rohdaten/bilder/`
- **Schema:** `<IMAGES_ROOT>/<ordner>/<pdf_stem>/<pdf_stem>__seite_NNNN.png`
- **Format:** PNG (150 KB - 2.6 MB pro Seite)
- **Gesamt:** 5979 Bilder, 54 Ordner

## Betroffene Stellen

| Cell | nuid | Problem |
|------|------|---------|
| 2 | 1b366157 | `%pip install pymupdf` |
| 3 | 07496e5a | `import fitz`, PIL ImageFilter/ImageOps |
| 4 | 4e574147 | Fehlender IMAGES_ROOT_PATH Parameter |
| 5 | 02133b8a | Fehlendes IMAGES_ROOT_PATH Widget |
| 9 | b55a4486 | `render_page_from_path_to_jpeg()`, `make_orientation_collage()`, `make_test_image()` |
| 12 | 2db791cb | Aufrufe von `render_page_from_path_to_jpeg()`, kein gc.collect() |
| 13 | 5d102b8c | `fitz.open(pdf_path)` zum Seitenzaehlen |

---

## CELL 2 - pip install

```diff
-%pip install pymupdf json-repair --quiet
+%pip install json-repair --quiet
 dbutils.library.restartPython()
```

---

## CELL 3 - Imports

```diff
 from __future__ import annotations
 
 import base64
+import gc
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
 
-import fitz  # PyMuPDF
 import pandas as pd
 import requests
 from json_repair import repair_json
-from PIL import Image, ImageDraw, ImageFilter, ImageOps
+from PIL import Image, ImageDraw
 from pyspark.sql import Window
 from pyspark.sql import functions as F
 from pyspark.sql import types as T
```

---

## CELL 4 - Parameter

Nach `DEFAULT_EXPORT_ROOT_PATH` hinzufuegen:

```diff
 DEFAULT_EXPORT_ROOT_PATH = "/Volumes/playground/u_daniel_bick/rohdaten/Projektdaten_Excel_Export"
+DEFAULT_IMAGES_ROOT_PATH = "/Volumes/playground/u_daniel_bick/rohdaten/bilder"
+IMAGE_FILE_PATTERN = "{stem}__seite_{page:04d}.png"
```

---

## CELL 5 - Widgets

```diff
     dbutils.widgets.text("EXPORT_ROOT_PATH", DEFAULT_EXPORT_ROOT_PATH)
+    dbutils.widgets.text("IMAGES_ROOT_PATH", DEFAULT_IMAGES_ROOT_PATH)
     dbutils.widgets.text("RUN_ID", RUN_ID)
```

```diff
     EXPORT_ROOT_PATH = dbutils.widgets.get("EXPORT_ROOT_PATH").strip() or DEFAULT_EXPORT_ROOT_PATH
+    IMAGES_ROOT_PATH = dbutils.widgets.get("IMAGES_ROOT_PATH").strip() or DEFAULT_IMAGES_ROOT_PATH
```

```diff
 except Exception:
     ROOT_PATH = DEFAULT_ROOT_PATH
     EXPORT_ROOT_PATH = DEFAULT_EXPORT_ROOT_PATH
+    IMAGES_ROOT_PATH = DEFAULT_IMAGES_ROOT_PATH
     DOCUMENT_FILTER = ""
```

```diff
 print(f"EXPORT_ROOT_PATH: {EXPORT_ROOT_PATH}")
+print(f"IMAGES_ROOT_PATH: {IMAGES_ROOT_PATH}")
 print(f"DOCUMENT_FILTER: {DOCUMENT_FILTER or '(alle)'}")
```

---

## CELL 9 - KOMPLETT ERSETZEN

Alten Inhalt (`render_page_from_path_to_jpeg`, `make_orientation_collage`, `make_test_image`) vollstaendig entfernen und durch folgenden Code ersetzen:

```python
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
    
    Returns:
        (image_bytes, (width, height), size_mb)
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
    # Fallback: PIL oeffnen aber sofort schliessen
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
    
    return len([
        f for f in image_dir.iterdir()
        if f.suffix.lower() == ".png" and "__seite_" in f.name
    ])


def make_orientation_collage(pdf_path: str, page_number: int) -> bytes:
    """Erzeugt 2x2-Montage aus 4 Rotationen des vorgerenderten Bildes."""
    image_bytes, _, _ = load_page_image_bytes(pdf_path, page_number)
    
    base_img = Image.open(io.BytesIO(image_bytes))
    del image_bytes  # Original-Bytes sofort freigeben
    
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
```

---

## CELL 12 - process_single_page_parallel

Rendering-Aufrufe ersetzen:

```diff
-        current_bytes, current_size = render_page_from_path_to_jpeg(
-            pdf_path=row["pdf_path"],
-            page_number=int(row["page_number"]),
-            rotation_degrees=rotation,
-        )
-        image_width_px, image_height_px = current_size
+        current_bytes, current_size, img_size_mb = load_page_image_bytes(
+            pdf_path=row["pdf_path"],
+            page_number=int(row["page_number"]),
+        )
+        image_width_px, image_height_px = current_size
```

Vorseite ersetzen:

```diff
         if INCLUDE_PREVIOUS_PAGE_AS_CONTEXT_IMAGE and int(row["page_number"]) > 1:
             try:
-                previous_bytes, _ = render_page_from_path_to_jpeg(
-                    pdf_path=row["pdf_path"],
-                    page_number=int(row["page_number"]) - 1,
-                    rotation_degrees=rotation,
-                    dpi=max(180, int(DPI * 0.85)),
-                    max_side_px=MAX_IMAGE_SIDE_PX,
-                    jpeg_quality=JPEG_QUALITY,
-                )
+                previous_bytes, _, _ = load_page_image_bytes(
+                    pdf_path=row["pdf_path"],
+                    page_number=int(row["page_number"]) - 1,
+                )
                 labelled_images.append(("PREVIOUS_CONTEXT_PAGE", previous_bytes))
             except Exception:
                 pass
```

Nach `parsed = extract_json_from_model_text(response_text)` einfuegen:

```diff
         parsed = extract_json_from_model_text(response_text)
+
+        # Speicher sofort freigeben
+        del current_bytes
+        del labelled_images
+        gc.collect()
```

Am Ende von `process_pages_partition`, nach `output_rows.append(future.result())`:

```diff
             for future in as_completed(futures):
                 output_rows.append(future.result())
+
+        gc.collect()
         yield pd.DataFrame(output_rows, columns=RAW_COLUMNS)
```

---

## CELL 13 - Manifest erstellen

`fitz.open()` durch `count_page_images()` ersetzen:

```diff
     try:
-        doc = fitz.open(pdf_path)
-        page_count = len(doc)
-        doc.close()
+        page_count = count_page_images(pdf_path)
+        if page_count == 0:
+            raise FileNotFoundError(f"Keine Seitenbilder fuer: {pdf_path}")
 
         for page_number in range(1, page_count + 1):
```

---

## OOM-Risiken ausserhalb PDF-Rendering

1. **Base64-Encoding verdoppelt Bildgroesse** - 2.6 MB PNG wird zu ~3.5 MB String
   - Fix: Original-bytes sofort nach Encoding freigeben
2. **ThreadPoolExecutor** haelt alle Futures gleichzeitig
   - Fix: THREADS_PER_PARTITION = 1 auf Serverless beibehalten
3. **response_text Akkumulation** - 8 KB pro Seite * Partitionsgroesse
   - Fix: gc.collect() nach jeder Partition
4. **PIL Image-Objekte in Orientation Collage** - 4x unkomprimiertes Bild
   - Fix: Explizites close()/del nach Verwendung

## Speicher-Budget pro Seite (nach Refactoring)

| Komponente | Groesse |
|---|---|
| PNG-Bytes von Disk | ~1.5 MB (Median) |
| Base64-String | ~2.0 MB |
| HTTP-Request-Payload | ~2.2 MB |
| Model-Response | ~0.01 MB |
| **Peak pro Seite** | **~5.7 MB** (statt ~25 MB mit PyMuPDF) |
