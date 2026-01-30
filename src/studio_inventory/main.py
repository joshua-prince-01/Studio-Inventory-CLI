# studio_inventory/main.py
# Unified CLI ingest for multiple vendors via studio_inventory.vendors.registry.pick_parser
# Includes per-run log file written to project_root/log/

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
import re
import traceback
import hashlib
import shutil
import sqlite3
import uuid
import os
from urllib.parse import quote_plus
import pandas as pd

from studio_inventory.vendors.registry import pick_parser
from studio_inventory.paths import workspace_root, log_dir, receipts_dir, project_root

# ----------------------------
# Simple run logger
# ----------------------------


# ----------------------------
# Quiet noisy PDF font warnings (pdfminer)
# ----------------------------
import logging

def suppress_pdfminer_font_warnings() -> None:
    """Silence pdfminer warnings like 'Could not get FontBBox...'."""
    for name in ("pdfminer", "pdfminer.pdffont", "pdfminer.psparser", "pdfminer.pdfinterp"):
        logger = logging.getLogger(name)
        logger.setLevel(logging.ERROR)
        logger.propagate = False

suppress_pdfminer_font_warnings()

class RunLogger:
    def __init__(self, log_path: Path, echo: bool = True):
        self.log_path = log_path
        self._fh = log_path.open("w", encoding="utf-8")
        self.echo = echo

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass

    def log(self, msg: str = ""):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}"
        self._fh.write(line + "\n")
        self._fh.flush()
        if self.echo:
            print(msg)

    def exception(self, context: str):
        self.log(f"ERROR: {context}")
        self.log(traceback.format_exc())

def create_run_log(echo: bool = True) -> RunLogger:
    root = workspace_root()
    log_dir_path = log_dir()
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir_path / f"run_{stamp}.txt"
    logger = RunLogger(log_path=log_path, echo=echo)
    logger.log(f"Log file: {log_path}")
    logger.log(f"Workspace root: {root}")
    logger.log(f"CWD: {Path.cwd().resolve()}")
    return logger


# ----------------------------
# Folder + PDF pickers
# ----------------------------

def pick_folder_from_cwd(start_dir: str | Path | None = None) -> Path:
    cur = Path(start_dir) if start_dir else Path.cwd()

    while True:
        cur = cur.expanduser().resolve()
        print("\n=== Folder Picker ===")
        print(f"Current directory (cwd): {cur}")
        print("Enter folder number to go in, '.' to select this folder, '..' to go up,")
        print("or type a path to jump (~/... or /... or relative). 'q' quits.\n")

        try:
            subdirs = sorted([p for p in cur.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
        except PermissionError:
            print("Permission denied. Going up one level.")
            cur = cur.parent
            continue
        except FileNotFoundError:
            cur = cur.parent
            continue

        if not subdirs:
            print("  (No subfolders here)")
        else:
            for i, d in enumerate(subdirs, start=1):
                print(f"  [{i}] {d.name}")

        choice = input("\n> ").strip()

        if choice.lower() == "q":
            raise SystemExit(0)

        if choice == ".":
            return cur

        if choice == "..":
            cur = cur.parent if cur.parent != cur else cur
            continue

        if choice.startswith(("/", "~")) or "/" in choice or choice.startswith("."):
            candidate = Path(choice).expanduser()
            if not candidate.is_absolute():
                candidate = cur / candidate
            candidate = candidate.resolve()

            if candidate.exists() and candidate.is_dir():
                cur = candidate
            else:
                print(f"Not a folder: {candidate}")
            continue

        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(subdirs):
                cur = subdirs[idx]
            else:
                print("Invalid number.")
            continue

        print("Unrecognized input.")

def pick_pdfs_in_folder(folder: Path) -> list[Path]:
    pdfs = sorted(list(folder.glob("*.pdf")) + list(folder.glob("*.PDF")))
    if not pdfs:
        print(f"\nNo PDFs found in: {folder}")
        return []

    print("\n=== PDFs Found ===")
    for i, p in enumerate(pdfs, start=1):
        print(f"  [{i}] {p.name}")

    print("\nSelect PDFs:")
    print("  all            -> ingest all")
    print("  3              -> ingest one (by number)")
    print("  1,4,7          -> ingest many")
    print("  55152414       -> substring match")
    print("  q              -> quit")

    choice = input("\n> ").strip().lower()
    if choice == "q":
        raise SystemExit(0)

    if choice == "all":
        return pdfs

    if choice.isdigit():
        idx = int(choice) - 1
        return [pdfs[idx]] if 0 <= idx < len(pdfs) else []

    if "," in choice:
        out: list[Path] = []
        for part in choice.split(","):
            part = part.strip()
            if not part.isdigit():
                return []
            idx = int(part) - 1
            if not (0 <= idx < len(pdfs)):
                return []
            out.append(pdfs[idx])
        return out

    needle = choice
    return [p for p in pdfs if needle in p.name.lower()]


# ----------------------------
# Export folder picker
# ----------------------------

def _export_browser(start_dir: Path) -> Path | None:
    cur = start_dir.expanduser()

    while True:
        cur = cur.expanduser().resolve()
        print("\n=== Export Folder Browser ===")
        print(f"Current folder: {cur}")
        print("Enter = choose this folder | number = enter folder | .. = up | q = cancel")
        print("Or type a path to jump (~/... or /... or relative).\n")

        try:
            subdirs = sorted([p for p in cur.iterdir() if p.is_dir()], key=lambda p: p.name.lower())
        except PermissionError:
            print("Permission denied. Going up one level.")
            cur = cur.parent
            continue
        except FileNotFoundError:
            cur = cur.parent
            continue

        if not subdirs:
            print("  (No subfolders here)")
        else:
            for i, d in enumerate(subdirs, start=1):
                print(f"  [{i}] {d.name}")

        choice = input("\n> ").strip()

        if choice == "":
            return cur

        if choice.lower() == "q":
            return None

        if choice == "..":
            cur = cur.parent if cur.parent != cur else cur
            continue

        if choice.startswith(("/", "~")) or "/" in choice or choice.startswith("."):
            candidate = Path(choice).expanduser()
            if not candidate.is_absolute():
                candidate = cur / candidate
            candidate = candidate.resolve()

            if candidate.exists() and candidate.is_dir():
                cur = candidate
            else:
                print(f"Not a folder: {candidate}")
            continue

        if choice.isdigit():
            idx = int(choice) - 1
            if 0 <= idx < len(subdirs):
                cur = subdirs[idx]
            else:
                print("Invalid number.")
            continue

        print("Unrecognized input.")

def pick_export_folder(default_dir: Path) -> Path | None:
    default_dir = default_dir.expanduser()
    browse_start = default_dir if default_dir.exists() else default_dir.parent

    print("\n=== Export Folder Picker ===")
    print("Default export folder:")
    print(f"  {default_dir.resolve()}")
    print("\nPress Enter to use default, 'q' to cancel export, 'pick' to browse, or type a path to jump.")

    choice = input("> ").strip()

    if choice == "":
        return default_dir.resolve()
    if choice.lower() == "q":
        return None
    if choice.lower() == "pick":
        return _export_browser(browse_start)

    candidate = Path(choice).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    return candidate

# ----------------------------
# Ingest helpers + normalization
# ----------------------------

# ----------------------------
# Ingest integrity: duplicate detection + stable IDs
# ----------------------------

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

class IngestRegistry:
    """
    Persistent registry of ingested PDFs (by content hash), so we can skip duplicates across runs.
    Stored in a local SQLite DB under project_root/.ingest/
    """
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingested_files (
                    file_hash TEXT PRIMARY KEY,
                    first_seen_utc TEXT NOT NULL,
                    original_path TEXT,
                    vendor TEXT,
                    order_ref TEXT
                );
            """)
            conn.commit()

    def has_hash(self, file_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM ingested_files WHERE file_hash = ? LIMIT 1;",
                (file_hash,)
            ).fetchone()
        return row is not None

    def register(self, *, file_hash: str, pdf_path: Path, vendor: str | None = None, order_ref: str | None = None):
        with self._connect() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO ingested_files(file_hash, first_seen_utc, original_path, vendor, order_ref)
                VALUES (?, ?, ?, ?, ?);
            """, (file_hash, datetime.utcnow().isoformat(), str(pdf_path), vendor, order_ref))
            conn.commit()

def move_to_duplicates(pdf_path: Path, duplicates_dir: Path) -> Path:
    """
    Move pdf_path into duplicates_dir, de-conflicting filename if needed.
    Returns the new path.
    """
    duplicates_dir.mkdir(parents=True, exist_ok=True)
    dest = duplicates_dir / pdf_path.name
    if dest.exists():
        stem, suffix = pdf_path.stem, pdf_path.suffix
        i = 2
        while True:
            candidate = duplicates_dir / f"{stem}__dup{i}{suffix}"
            if not candidate.exists():
                dest = candidate
                break
            i += 1
    return Path(shutil.move(str(pdf_path), str(dest)))


_NAMESPACE_ORDER = uuid.UUID("0c9d55f5-6920-4e55-92a9-1a9b7b2a7a1a")
_NAMESPACE_LINEITEM = uuid.UUID("6b6a3d35-7b8c-4b68-8e6a-3d6cf2c3a2a1")

_WS = re.compile(r"\s+")

def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    return _WS.sub(" ", s)

def make_order_uid(*, vendor: str, order_ref: str, file_hash: str) -> str:
    """
    Stable-ish order ID.
    If you want it stable across multiple PDFs for the "same order", change the key to exclude file_hash.
    """
    key = "|".join([_norm(vendor), _norm(order_ref), file_hash])
    return str(uuid.uuid5(_NAMESPACE_ORDER, key))

def make_line_item_uid(
    *,
    vendor: str,
    order_ref: str,
    file_hash: str,
    line_index: int,
    sku: str = "",
    description: str = "",
    unit_price: str = "",
    ordered: str = "",
) -> str:
    """
    Deterministic line-item ID for linked databases.
    """
    key = "|".join([
        _norm(vendor),
        _norm(order_ref),
        file_hash,            # tie to exact source PDF content
        str(line_index),
        _norm(sku),
        _norm(description),
        _norm(str(unit_price)),
        _norm(str(ordered)),
    ])
    return str(uuid.uuid5(_NAMESPACE_LINEITEM, key))

PACK_RE = re.compile(r"\bPacks?\s+of\s+(\d+)\b", re.I)

_PACK_RE = re.compile(
    r"""\s*,?\s*(packs?|pack|package|pkg|bag|boxes?)\s+of\s+\d+\s*$""",
    re.IGNORECASE
)

def _tighten_units(s: str) -> str:
    # 24 mm -> 24mm, 3/8" -> 3/8"
    s = re.sub(r"(\d)\s+(mm|cm|m|in)\b", r"\1\2", s, flags=re.IGNORECASE)
    # Normalize common diameter terms
    s = re.sub(r"\bouter diameter\b", "OD", s, flags=re.IGNORECASE)
    s = re.sub(r"\binner diameter\b", "ID", s, flags=re.IGNORECASE)
    s = re.sub(r"\bdiameter\b", "Dia", s, flags=re.IGNORECASE)
    s = re.sub(r"\bthread size\b", "Thread", s, flags=re.IGNORECASE)
    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s

def clean_description(desc: str) -> str:
    if desc is None:
        return ""
    s = str(desc).strip()
    s = _PACK_RE.sub("", s).strip()
    # also remove trailing ", Each" if it shows up
    s = re.sub(r"\s*,?\s*each\s*$", "", s, flags=re.IGNORECASE).strip()
    return s

def make_label_fields(vendor: str, sku: str, description: str, mfg_pn: str | None = None) -> tuple[str, str, str]:
    """
    Returns: (desc_clean, label_line1, label_line2)
    """
    desc_clean = clean_description(description)
    if not desc_clean:
        # fallback to sku/mfg_pn
        line1 = (mfg_pn or sku or "").strip()
        return desc_clean, line1, ""

    # Special case: multi-line descriptions that end in a CAD filename (e.g., SendCutSend)
    # Example:
    #   6061 T6 Aluminum (.250")
    #   1.693 x 2.586 in
    #   Adjustment_Assembly_Male_v6.step
    desc_lines = [ln.strip() for ln in re.split(r"[\r\n]+", desc_clean) if ln.strip()]
    if len(desc_lines) >= 2:
        last = desc_lines[-1]
        # If the last line looks like a file name, use it as the display name
        if re.search(r"\.(step|stp|dxf|dwg|iges|igs|sldprt|sldasm|pdf)\b", last, re.I):
            material = desc_lines[0]
            dims = desc_lines[1] if len(desc_lines) > 1 else ""
            # If there are extra lines between dims and filename, fold them into dims/specs
            extra = desc_lines[2:-1]
            spec_bits = [dims] + extra if extra else [dims]
            spec_bits = [b for b in ( _tighten_units(x) for x in spec_bits ) if b]
            line2 = " — ".join([material] + spec_bits) if spec_bits else material
            return desc_clean, last, line2

    # If SKU itself is a CAD filename, prefer it as the name
    if sku and re.search(r"\.(step|stp|dxf|dwg|iges|igs|sldprt|sldasm|pdf)\b", str(sku).strip(), re.I):
        # Use the (cleaned) description as the spec line
        spec = " — ".join([ln.strip() for ln in re.split(r"[\r\n]+", desc_clean) if ln.strip()])
        return desc_clean, str(sku).strip(), spec

    # McMaster-style comma specs work great for labels
    parts = [p.strip() for p in desc_clean.split(",") if p.strip()]

    # Prefer first clause as the "name"
    line1 = parts[0] if parts else desc_clean

    # Build spec line from chunks containing numbers or key spec words
    spec_candidates = []
    key_words = ("OD", "ID", "Thread", "Long", "Length", "Wide", "Width", "Thick", "Thickness", "Gauge", "Size", "Pitch", "Dia")
    for p in parts[1:]:
        # keep chunks with digits OR key spec words
        if any(ch.isdigit() for ch in p) or any(k.lower() in p.lower() for k in key_words):
            # avoid anything pack-related that slipped through
            if re.search(r"\b(pack|packs|pkg|package)\b", p, flags=re.IGNORECASE):
                continue
            spec_candidates.append(_tighten_units(p))

    # If comma parsing didn’t yield anything, try a weaker regex scan
    if not spec_candidates:
        # Example: "3/8\"-16 Thread Size" -> pull the "3/8\"-16"
        m = re.search(r'(\d+\s*/\s*\d+\s*"?\s*-\s*\d+)', desc_clean)
        if m:
            spec_candidates.append(_tighten_units(m.group(1)))

    line2 = " - ".join(spec_candidates[:4])  # limit clutter

    # For non-comma vendor descriptions, use a stable identifier on line2 if empty
    if not line2:
        if mfg_pn and str(mfg_pn).strip():
            line2 = str(mfg_pn).strip()
        elif sku and str(sku).strip():
            line2 = str(sku).strip()

    return desc_clean, line1, line2
# ----------------------------
# Links + QR targets
# ----------------------------

QR_TARGET = os.environ.get("QR_TARGET", "purchase").strip().lower()
AIRTABLE_ITEM_URL_TEMPLATE = os.environ.get("AIRTABLE_ITEM_URL_TEMPLATE", "").strip()

def make_purchase_url(vendor: str, sku: str) -> str:
    """
    Returns a URL that can be encoded in a QR code for quick re-ordering.
    """
    v = (vendor or "").strip().lower()
    s = (sku or "").strip()
    if not v or not s:
        return ""
    if v == "digikey":
        # Search-by-keywords works reliably with Digi-Key part numbers
        return f"https://www.digikey.com/en/products?keywords={quote_plus(s)}"
    if v == "mcmaster":
        # McMaster deep-links commonly use a fragment with the part number
        return f"https://www.mcmaster.com/#{quote_plus(s)}"
    if v == "arduino":
        # Arduino store search (Shopify) – use SKU as query
        return f"https://store-usa.arduino.cc/search?type=product%2Cquery&options%5Bprefix%5D=last&q={quote_plus(s)}"
    return ""

def make_airtable_url(part_key: str, vendor: str, sku: str) -> str:
    """
    Optional: provide AIRTABLE_ITEM_URL_TEMPLATE, e.g.
      AIRTABLE_ITEM_URL_TEMPLATE="https://airtable.com/appXXXX/tblYYYY/{part_key}"
    Supported tokens: {part_key}, {vendor}, {sku}
    """
    if not AIRTABLE_ITEM_URL_TEMPLATE:
        return ""
    try:
        return AIRTABLE_ITEM_URL_TEMPLATE.format(part_key=part_key, vendor=vendor, sku=sku)
    except Exception:
        return ""

def pick_qr_url(purchase_url: str, airtable_url: str) -> str:
    """
    Chooses the URL to encode in the QR code.
    Set QR_TARGET=airtable to prefer airtable_url when available.
    """
    p = (purchase_url or "").strip()
    a = (airtable_url or "").strip()
    if QR_TARGET == "airtable":
        return a or p
    return p or a

def make_label_short(label_line1: str, label_line2: str, *, sku: str = "", mfg_pn: str | None = None, max_len: int = 42) -> str:
    """
    A compact one-liner suitable for small QR labels.
    """
    l1 = (label_line1 or "").strip()
    l2 = (label_line2 or "").strip()
    base = l1
    if l2 and (not l1 or l2.lower() not in l1.lower()):
        base = f"{l1} ({l2})" if l1 else l2
    if not base:
        base = (str(mfg_pn).strip() if mfg_pn else "") or (sku or "").strip()
    base = re.sub(r"\\s+", " ", base).strip()
    if len(base) > max_len:
        base = base[: max_len - 3].rstrip() + "..."
    return base

def to_int(x):
    try:
        s = str(x).strip()
        if s == "" or s.lower() == "none":
            return pd.NA
        return int(float(s))
    except Exception:
        return pd.NA

def to_float(x):
    try:
        s = str(x).replace("$", "").replace(",", "").strip()
        if s == "" or s.lower() == "none":
            return pd.NA
        return float(s)
    except Exception:
        return pd.NA

def infer_pack_qty(description: str) -> int:
    if not description:
        return 1
    m = PACK_RE.search(description)
    if not m:
        return 1
    try:
        return int(m.group(1))
    except Exception:
        return 1

def _dictify(obj) -> dict:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if is_dataclass(obj):
        return asdict(obj)
    if hasattr(obj, "__dict__"):
        return dict(obj.__dict__)
    return {}

def ingest_receipts(pdf_paths: list[Path], debug: bool = False, logger: RunLogger | None = None):
    order_rows: list[dict] = []
    item_rows: list[dict] = []

    # Duplicate detection: persistent across runs + within this run
    registry = IngestRegistry(db_path())
    seen_hashes: set[str] = set()

    def log(msg: str):
        if logger:
            logger.log(msg)
        else:
            print(msg)

    for pdf_path in pdf_paths:
        pdf_path = Path(pdf_path)

        # Hash the file first so we can skip/move duplicates before any parsing work
        try:
            file_hash = sha256_file(pdf_path)
        except Exception:
            log(f"  RESULT: SKIPPED (could not hash file: {pdf_path})\n")
            continue

        dup_reason = None
        if file_hash in seen_hashes:
            dup_reason = "duplicate in selected batch"
        elif registry.has_hash(file_hash):
            dup_reason = "already ingested"

        if dup_reason:
            try:
                moved = move_to_duplicates(pdf_path, pdf_path.parent / "duplicates")
                log(f"  RESULT: DUPLICATE ({dup_reason}) moved -> {moved}\n")
            except Exception:
                log(f"  RESULT: DUPLICATE ({dup_reason}) (move failed) skipped: {pdf_path}\n")
            continue

        seen_hashes.add(file_hash)

        parser = pick_parser(str(pdf_path))
        parser_name = getattr(parser, "__name__", None) if parser else "(none)"

        log(f"FILE: {pdf_path.name}")
        log(f"  PATH: {pdf_path}")
        log(f"  PARSER: {parser_name}")

        if parser is None:
            log("  RESULT: SKIPPED (no parser matched)\n")
            continue

        if debug:
            print(f"\n=== Processing: {pdf_path.name} ===")
            print(f"Using parser: {parser_name}")

        try:
            info = _dictify(parser.parse_order(str(pdf_path), debug=debug))
            vendor = (info.get("vendor") or getattr(parser, "VENDOR", None) or "unknown").lower()

            order_ref = str(info.get("invoice") or info.get("purchase_order") or "")
            order_uid = make_order_uid(vendor=vendor, order_ref=order_ref, file_hash=file_hash)

            order_rows.append({
                "order_uid": order_uid,
                "file_hash": file_hash,
                "vendor": vendor,
                "source_file": pdf_path.name,
                "pdf_path": str(pdf_path),
                "purchase_order": info.get("purchase_order"),
                "invoice": info.get("invoice"),
                "invoice_date": info.get("invoice_date"),
                "account_number": info.get("account_number"),
                "payment_date": info.get("payment_date"),
                "credit_card": info.get("credit_card"),
                "merchandise": info.get("merchandise"),
                "shipping": info.get("shipping"),
                "sales_tax": info.get("sales_tax"),
                "total": info.get("total"),
            })

            items = parser.parse_line_items(str(pdf_path), debug=debug) or []
            log(f"  ORDER: vendor={vendor} invoice={info.get('invoice')} po={info.get('purchase_order')} date={info.get('invoice_date')}")
            log(f"  LINE_ITEMS: {len(items)} parsed")

            for idx, d in enumerate(items, start=1):
                line_idx = d.get("line")
                if line_idx is None:
                    line_idx = idx

                line_item_uid = make_line_item_uid(
                    vendor=vendor,
                    order_ref=order_ref,
                    file_hash=file_hash,
                    line_index=int(line_idx),
                    sku=str(d.get("sku") or ""),
                    description=str(d.get("description") or ""),
                    unit_price=str(d.get("unit_price") or ""),
                    ordered=str(d.get("ordered") or ""),
                )

                row = {
                    "line_item_uid": line_item_uid,
                    "order_uid": order_uid,
                    "file_hash": file_hash,
                    "vendor": vendor,
                    "source_file": pdf_path.name,
                    "invoice": info.get("invoice"),
                    "purchase_order": info.get("purchase_order"),
                    "line": line_idx,
                    "sku": d.get("sku"),
                    "description": d.get("description"),
                    "ordered": d.get("ordered"),
                    "shipped": d.get("shipped"),
                    "balance": d.get("balance"),
                    "unit_price": d.get("unit_price"),
                    "line_total": d.get("line_total"),
                }
                for k in ("part", "mfg", "mfg_pn", "coo"):
                    if k in d and k not in row:
                        row[k] = d.get(k)
                item_rows.append(row)

            # Mark this PDF as ingested only after successful parse
            registry.register(file_hash=file_hash, pdf_path=pdf_path, vendor=vendor, order_ref=order_ref)

            log("  RESULT: OK\n")

        except Exception:
            if logger:
                logger.exception(f"Failed parsing {pdf_path.name} with parser={parser_name}")
            else:
                print(f"[ERROR] Failed parsing {pdf_path.name} with parser={parser_name}")
                traceback.print_exc()
            log("")

    orders_df = pd.DataFrame(order_rows)
    line_items_df = pd.DataFrame(item_rows)

    for col in ("merchandise", "shipping", "sales_tax", "total"):
        if col in orders_df.columns:
            orders_df[col] = orders_df[col].apply(to_float)

    if line_items_df.empty:
        parts_received_df = pd.DataFrame(columns=[
            "part_key", "vendor", "sku", "description", "desc_clean", "label_line1", "label_line2", "label_short",
            "purchase_url", "airtable_url", "label_qr_url", "label_qr_text",
            "units_received", "total_spend", "last_invoice", "avg_unit_cost"
        ])
        parts_removed_df = pd.DataFrame(columns=["removal_uid","part_key","qty_removed","ts_utc","project","note"])
        return orders_df, line_items_df, parts_received_df, parts_removed_df

    for col in ("line", "ordered", "shipped", "balance"):
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_int)

    for col in ("unit_price", "line_total"):
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_float)


    # Ensure unit_price is never NULL when we have line_total.
    # Prefer line_total / ordered when ordered is numeric and > 0; otherwise fall back to line_total.
    if "unit_price" in line_items_df.columns and "line_total" in line_items_df.columns:
        up = line_items_df["unit_price"]
        lt = line_items_df["line_total"]
        need = up.isna() & lt.notna()
        if need.any():
            if "ordered" in line_items_df.columns:
                ord_ = pd.to_numeric(line_items_df["ordered"], errors="coerce")
                div_ok = need & ord_.notna() & (ord_ > 0)
                line_items_df.loc[div_ok, "unit_price"] = (lt[div_ok] / ord_[div_ok]).astype(float)
                # remaining: just use line_total
                need2 = line_items_df["unit_price"].isna() & lt.notna()
                line_items_df.loc[need2, "unit_price"] = lt[need2].astype(float)
            else:
                line_items_df.loc[need, "unit_price"] = lt[need].astype(float)

    if "description" not in line_items_df.columns:
        line_items_df["description"] = ""

    # Label fields (for drawer/bin labels) derived from description text
    def _row_label(r):
        desc_clean, l1, l2 = make_label_fields(
            vendor=str(r.get("vendor", "") or ""),
            sku=str(r.get("sku", "") or ""),
            description=str(r.get("description", "") or ""),
            mfg_pn=(r.get("mfg_pn") if "mfg_pn" in r else None),
        )
        return desc_clean, l1, l2

    labels = line_items_df.apply(_row_label, axis=1, result_type="expand")
    labels.columns = ["desc_clean", "label_line1", "label_line2"]
    line_items_df = line_items_df.join(labels)

    line_items_df["pack_qty"] = line_items_df["description"].fillna("").apply(infer_pack_qty)

    shipped = pd.to_numeric(line_items_df.get("shipped"), errors="coerce").fillna(0).astype(int)
    pack_qty = pd.to_numeric(line_items_df.get("pack_qty"), errors="coerce").fillna(1).astype(int)
    line_items_df["units_received"] = shipped * pack_qty

    if "line_total" not in line_items_df.columns:
        line_items_df["line_total"] = pd.NA

    computed_total = (
        pd.to_numeric(line_items_df.get("ordered"), errors="coerce")
        * pd.to_numeric(line_items_df.get("unit_price"), errors="coerce")
    )
    line_items_df["line_total"] = line_items_df["line_total"].fillna(computed_total)

    if "sku" not in line_items_df.columns:
        line_items_df["sku"] = ""
    line_items_df["part_key"] = line_items_df["vendor"].astype(str) + ":" + line_items_df["sku"].astype(str)
    # Links + QR targets (purchase URL and optional Airtable URL)
    line_items_df["purchase_url"] = line_items_df.apply(
        lambda r: make_purchase_url(str(r.get("vendor", "") or ""), str(r.get("sku", "") or "")),
        axis=1
    )
    line_items_df["airtable_url"] = line_items_df.apply(
        lambda r: make_airtable_url(str(r.get("part_key", "") or ""), str(r.get("vendor", "") or ""), str(r.get("sku", "") or "")),
        axis=1
    )
    line_items_df["label_qr_url"] = line_items_df.apply(
        lambda r: pick_qr_url(str(r.get("purchase_url", "") or ""), str(r.get("airtable_url", "") or "")),
        axis=1
    )
    line_items_df["label_qr_text"] = line_items_df["label_qr_url"]

    # A compact one-liner for small QR labels
    line_items_df["label_short"] = line_items_df.apply(
        lambda r: make_label_short(
            str(r.get("label_line1", "") or ""),
            str(r.get("label_line2", "") or ""),
            sku=str(r.get("sku", "") or ""),
            mfg_pn=(r.get("mfg_pn") if "mfg_pn" in line_items_df.columns else None),
        ),
        axis=1
    )

    parts_received_df = (
        line_items_df.groupby("part_key", as_index=False)
        .agg(
            vendor=("vendor", "first"),
            sku=("sku", "first"),
            description=("description", "first"),
            desc_clean=("desc_clean", "first"),
            label_line1=("label_line1", "first"),
            label_line2=("label_line2", "first"),
            label_short=("label_short", "first"),
            purchase_url=("purchase_url", "first"),
            airtable_url=("airtable_url", "first"),
            label_qr_url=("label_qr_url", "first"),
            label_qr_text=("label_qr_text", "first"),
            units_received=("units_received", "sum"),
            total_spend=("line_total", "sum"),
            last_invoice=("invoice", "max"),
        )
    )
    parts_received_df["avg_unit_cost"] = parts_received_df["total_spend"] / parts_received_df["units_received"].replace({0: pd.NA})

    parts_removed_df = pd.DataFrame(columns=["removal_uid","part_key","qty_removed","ts_utc","project","note"])
    return orders_df, line_items_df, parts_received_df, parts_removed_df

# ----------------------------
# MAIN
# ----------------------------

# ----------------------------
# SQLite database (optional) - orders + line_items (+ inventory) + ingested_files registry
# ----------------------------

def db_path() -> Path:
    # Single project DB file (auto-created on first run)
    return workspace_root() / "studio_inventory.sqlite"

def _ensure_table(conn: sqlite3.Connection, table: str, pk_col: str):
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ("{pk_col}" TEXT PRIMARY KEY);')

def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}");').fetchall()
    # PRAGMA columns: cid, name, type, notnull, dflt_value, pk
    return {r[1] for r in rows}

MONEY_COLS = {
    "subtotal", "shipping", "tax", "total", "balance",
    "unit_price", "line_total", "total_spend", "avg_unit_cost",
    "on_hand", "qty_removed", "units_received"
}
INT_COLS = {"line", "ordered", "shipped", "pack_qty"}

def _sql_type_for_col(df: pd.DataFrame | None, col: str) -> str:
    if col in MONEY_COLS:
        return "REAL"
    if col in INT_COLS:
        return "INTEGER"
    if df is None or col not in df.columns:
        return "TEXT"

    s = df[col]
    if pd.api.types.is_integer_dtype(s):
        return "INTEGER"
    if pd.api.types.is_float_dtype(s):
        return "REAL"
    if pd.api.types.is_bool_dtype(s):
        return "INTEGER"
    return "TEXT"

def _ensure_columns(conn: sqlite3.Connection, table: str, cols: list[str], df: pd.DataFrame | None = None):
    existing = _existing_columns(conn, table)
    for c in cols:
        if c not in existing:
            coltype = _sql_type_for_col(df, c)
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" {coltype};')

def _upsert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame, pk_col: str):
    if df is None or df.empty:
        return

    _ensure_table(conn, table, pk_col)
    cols = [c for c in df.columns if c]
    _ensure_columns(conn, table, cols, df=df)

    # Add/update timestamp column
    if "updated_utc" not in cols:
        df = df.copy()
        df["updated_utc"] = datetime.utcnow().isoformat()
        cols = cols + ["updated_utc"]
        _ensure_columns(conn, table, ["updated_utc"], df=df)

    col_list = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    update_set = ", ".join([f'"{c}"=excluded."{c}"' for c in cols if c != pk_col])

    sql = f"""
        INSERT INTO "{table}" ({col_list})
        VALUES ({placeholders})
        ON CONFLICT("{pk_col}") DO UPDATE SET {update_set};
    """

    rows = [tuple(None if pd.isna(v) else v for v in r) for r in df[cols].itertuples(index=False, name=None)]
    conn.executemany(sql, rows)

def init_inventory_db(dbfile: Path):
    """
    Initializes the SQLite schema.

    Naming:
      - parts_received: aggregated receipts per part_key (what you've brought into the studio)
      - parts_removed: manual/usage removals (what you've consumed/used)
      - inventory: materialized current on-hand snapshot (for easy GUI syncing)
      - inventory_view: SQL view computing on-hand from parts_received - parts_removed
    """
    dbfile.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(dbfile) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingested_files (
                file_hash TEXT PRIMARY KEY,
                first_seen_utc TEXT NOT NULL,
                original_path TEXT,
                vendor TEXT,
                order_id TEXT
            );
        """)

        # Orders + line_items (base schema; extra columns may be added dynamically on upsert)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_uid TEXT PRIMARY KEY,
                vendor TEXT,
                order_id TEXT,
                order_date TEXT,
                subtotal REAL,
                shipping REAL,
                tax REAL,
                total REAL,
                balance REAL,
                file_hash TEXT,
                updated_utc TEXT
            );
        """)

        conn.execute("""
            CREATE TABLE IF NOT EXISTS line_items (
                line_item_uid TEXT PRIMARY KEY,
                order_uid TEXT,
                vendor TEXT,
                invoice TEXT,
                sku TEXT,
                part_key TEXT,
                description TEXT,
                desc_clean TEXT,
                label_line1 TEXT,
                label_line2 TEXT,
                label_short TEXT,
                purchase_url TEXT,
                airtable_url TEXT,
                label_qr_url TEXT,
                label_qr_text TEXT,
                line INTEGER,
                ordered INTEGER,
                shipped INTEGER,
                pack_qty INTEGER,
                units_received REAL,
                unit_price REAL,
                line_total REAL,
                file_hash TEXT,
                updated_utc TEXT
            );
        """)

        # Aggregated receipts table (what you've received)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parts_received (
                part_key TEXT PRIMARY KEY,
                vendor TEXT,
                sku TEXT,
                description TEXT,
                desc_clean TEXT,
                label_line1 TEXT,
                label_line2 TEXT,
                label_short TEXT,
                purchase_url TEXT,
                airtable_url TEXT,
                label_qr_url TEXT,
                label_qr_text TEXT,
                units_received REAL,
                total_spend REAL,
                last_invoice TEXT,
                avg_unit_cost REAL,
                updated_utc TEXT
            );
        """)

        # Removals/usage table (what you've consumed/used)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS parts_removed (
                removal_uid TEXT PRIMARY KEY,
                part_key TEXT NOT NULL,
                qty_removed REAL NOT NULL,
                ts_utc TEXT,
                project TEXT,
                note TEXT,
                updated_utc TEXT
            );
        """)

        # Materialized on-hand snapshot (easy to sync to external GUIs like Airtable)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                part_key TEXT PRIMARY KEY,
                vendor TEXT,
                sku TEXT,
                description TEXT,
                desc_clean TEXT,
                label_line1 TEXT,
                label_line2 TEXT,
                label_short TEXT,
                purchase_url TEXT,
                airtable_url TEXT,
                label_qr_url TEXT,
                units_received REAL,
                units_removed REAL,
                on_hand REAL,
                avg_unit_cost REAL,
                total_spend REAL,
                last_invoice TEXT,
                updated_utc TEXT
            );
        """)

        # Indexes
        conn.execute('CREATE INDEX IF NOT EXISTS idx_line_items_order_uid ON line_items(order_uid);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_line_items_part_key ON line_items(part_key);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_parts_removed_part_key ON parts_removed(part_key);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_orders_vendor ON orders(vendor);')

        # Ensure label columns exist (supports schema upgrades without rebuilding the DB)
        _ensure_columns(conn, "line_items", ["desc_clean", "label_line1", "label_line2", "label_short", "purchase_url", "airtable_url", "label_qr_url", "label_qr_text"])
        _ensure_columns(conn, "parts_received", ["desc_clean", "label_line1", "label_line2", "label_short", "purchase_url", "airtable_url", "label_qr_url", "label_qr_text"])

        # View: computed inventory (received - removed)
        conn.execute("DROP VIEW IF EXISTS inventory_view;")
        conn.execute("""
            CREATE VIEW inventory_view AS
            SELECT
                pr.part_key,
                pr.vendor,
                pr.sku,
                pr.description,
                pr.desc_clean,
                pr.label_line1,
                pr.label_line2,
                pr.label_short,
                pr.purchase_url,
                pr.airtable_url,
                pr.label_qr_url,
                pr.label_qr_text,
                pr.units_received,
                COALESCE(r.removed, 0) AS units_removed,
                (pr.units_received - COALESCE(r.removed, 0)) AS on_hand,
                pr.avg_unit_cost,
                pr.total_spend,
                pr.last_invoice
            FROM parts_received pr
            LEFT JOIN (
                SELECT part_key, SUM(qty_removed) AS removed
                FROM parts_removed
                GROUP BY part_key
            ) r
            ON pr.part_key = r.part_key;
        """)

        conn.commit()

def update_database(
    orders_df: pd.DataFrame,
    line_items_df: pd.DataFrame,
    parts_received_df: pd.DataFrame,
    parts_removed_df: pd.DataFrame,
    *,
    dbfile: Path | None = None,
    logger: RunLogger | None = None
) -> pd.DataFrame:
    """
    Writes dataframes into SQLite and refreshes the materialized `inventory` table.

    Returns:
        inventory_on_hand_df: contents of inventory_view (computed on-hand)
    """
    dbfile = dbfile or db_path()
    init_inventory_db(dbfile)

    with sqlite3.connect(dbfile) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        _upsert_df(conn, "orders", orders_df, pk_col="order_uid")
        _upsert_df(conn, "line_items", line_items_df, pk_col="line_item_uid")
        _upsert_df(conn, "parts_received", parts_received_df, pk_col="part_key")
        _upsert_df(conn, "parts_removed", parts_removed_df, pk_col="removal_uid")

        # Refresh materialized on-hand snapshot from the view
        ts = datetime.utcnow().isoformat()
        conn.execute("DELETE FROM inventory;")
        conn.execute("""
            INSERT INTO inventory(
                part_key, vendor, sku, description, desc_clean,
                label_line1, label_line2, label_short,
                purchase_url, airtable_url, label_qr_url,
                units_received, units_removed, on_hand,
                avg_unit_cost, total_spend, last_invoice, updated_utc
            )
            SELECT
                part_key, vendor, sku, description, desc_clean,
                label_line1, label_line2, label_short,
                purchase_url, airtable_url, label_qr_url,
                units_received, units_removed, on_hand,
                avg_unit_cost, total_spend, last_invoice, ?
            FROM inventory_view;
        """, (ts,))

        inventory_on_hand_df = pd.read_sql_query("SELECT * FROM inventory_view;", conn)
        conn.commit()

    if logger:
        logger.log(f"SQLite DB updated: {dbfile}")

    return inventory_on_hand_df

def main():
    print("=== Receipt Ingest (CLI) ===")

    debug = (input("Debug prints? [y/N]: ").strip().lower() == "y")
    logger = create_run_log(echo=True)
    logger.log(f"Debug: {debug}")

    try:
        receipts_folder = pick_folder_from_cwd()
        logger.log(f"Selected receipts folder: {receipts_folder}")

        pdf_paths = pick_pdfs_in_folder(receipts_folder)
        logger.log(f"Selected PDFs ({len(pdf_paths)}): " + ", ".join(p.name for p in pdf_paths))

        if not pdf_paths:
            logger.log("Nothing selected. Exiting.")
            return

        orders_df, line_items_df, parts_received_df, parts_removed_df = ingest_receipts(pdf_paths, debug=debug, logger=logger)

        print("\n--- ORDERS (head) ---")
        print(orders_df.head(10).to_string(index=False))

        print("\n--- LINE ITEMS (head) ---")
        print(line_items_df.head(15).to_string(index=False))

        print("\n--- PARTS RECEIVED (top spend) ---")
        if parts_received_df.empty:
            print("(empty)")
        else:
            print(parts_received_df.sort_values("total_spend", ascending=False).head(20).to_string(index=False))

        # ----------------------------
        # Confirm before writing to DB
        # ----------------------------
        # This is intentionally a simple, reliable guardrail: users can dry-run
        # parsing + CSV export without mutating the SQLite database.
        apply_db = (input("\nApply this ingest to the SQLite database? [y/N]: ").strip().lower() == "y")
        logger.log(f"Apply DB update: {apply_db}")
        # this was commented out becuase it would place the default folder inside where we grabbed reciepts, not
        # the default 'exports' folder of the project:
        #default_export_dir = (receipts_folder.parent / "exports").resolve()
        default_export_dir = (workspace_root() / "exports").resolve()

        logger.log(f"Default export dir: {default_export_dir}")

        export_dir = pick_export_folder(default_export_dir)
        if export_dir is None:
            logger.log("Export cancelled.")
            return

        export_dir.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        orders_csv = export_dir / f"orders_{stamp}.csv"
        items_csv = export_dir / f"line_items_{stamp}.csv"
        received_csv = export_dir / f"parts_received_{stamp}.csv"
        removed_csv = export_dir / f"parts_removed_{stamp}.csv"
        inv_csv = export_dir / f"inventory_on_hand_{stamp}.csv"

        orders_df.to_csv(orders_csv, index=False)
        line_items_df.to_csv(items_csv, index=False)
        parts_received_df.to_csv(received_csv, index=False)
        parts_removed_df.to_csv(removed_csv, index=False)
        # inventory_on_hand CSV written after DB update

        logger.log("CSV files saved:")
        logger.log(f"  {orders_csv}")
        logger.log(f"  {items_csv}")
        logger.log(f"  {received_csv}")
        logger.log(f"  {removed_csv}")

        # Update SQLite database + refresh inventory snapshot (optional)
        if apply_db:
            inventory_on_hand_df = update_database(
                orders_df,
                line_items_df,
                parts_received_df,
                parts_removed_df,
                dbfile=db_path(),
                logger=logger
            )
            inventory_on_hand_df.to_csv(inv_csv, index=False)
            logger.log(f"  {inv_csv}")
        else:
            logger.log("DB update skipped by user (dry-run).")

        print("\n✅ CSV files saved:")
        print(" ", orders_csv)
        print(" ", items_csv)
        print(" ", received_csv)
        print(" ", removed_csv)
        if apply_db:
            print(" ", inv_csv)
        else:
            print(" (DB update skipped; inventory_on_hand not written)")

    finally:
        logger.close()

if __name__ == "__main__":
    main()