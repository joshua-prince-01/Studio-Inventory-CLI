from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime

import hashlib
import shutil
import sqlite3
import uuid
import os
from urllib.parse import quote_plus

import pandas as pd

from vendors.registry import pick_parser


# ----------------------------
# Shared helpers
# ----------------------------

# ----------------------------
# Duplicate detection + stable IDs
# ----------------------------

def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


class IngestRegistry:
    """Tracks ingested PDFs by content hash so re-runs can skip duplicates."""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ingested_files (
                    file_hash TEXT PRIMARY KEY,
                    first_seen_utc TEXT NOT NULL,
                    original_path TEXT,
                    vendor TEXT,
                    order_id TEXT
                );
                """
            )
            conn.commit()

    def has_hash(self, file_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT 1 FROM ingested_files WHERE file_hash = ? LIMIT 1;",
                (file_hash,),
            ).fetchone()
        return row is not None

    def register(self, file_hash: str, pdf_path: Path, vendor: str | None = None, order_id: str | None = None):
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO ingested_files(file_hash, first_seen_utc, original_path, vendor, order_id) "
                "VALUES (?, ?, ?, ?, ?);",
                (file_hash, datetime.utcnow().isoformat(), str(pdf_path), vendor, order_id),
            )
            conn.commit()


def move_to_duplicates(pdf_path: Path) -> Path:
    duplicates_dir = pdf_path.parent / "duplicates"
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


_WS = re.compile(r"\s+")

def _norm(s: object) -> str:
    s = "" if s is None or (isinstance(s, float) and pd.isna(s)) else str(s)
    return _WS.sub(" ", s.strip().lower())


NAMESPACE_LINEITEM = uuid.UUID("6b6a3d35-7b8c-4b68-8e6a-3d6cf2c3a2a1")
NAMESPACE_ORDER = uuid.UUID("c2a3b7d1-2df7-46c0-8de8-8d0d0c2a4f17")


def _first_nonempty(obj, names: tuple[str, ...], default: str = "") -> str:
    for n in names:
        if hasattr(obj, n):
            v = getattr(obj, n)
            if v is not None and str(v).strip() != "":
                return str(v)
    return default


def make_order_uid(vendor: str, order_id: str, file_hash: str) -> str:
    key = "|".join([_norm(vendor), _norm(order_id), file_hash])
    return str(uuid.uuid5(NAMESPACE_ORDER, key))



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

def make_line_item_uid(
    *,
    vendor: str,
    order_id: str,
    file_hash: str,
    line_index: int,
    part_number: str = "",
    description: str = "",
    unit_price: str = "",
    quantity: str = "",
) -> str:
    key = "|".join([
        _norm(vendor),
        _norm(order_id),
        file_hash,
        str(line_index),
        _norm(part_number),
        _norm(description),
        _norm(unit_price),
        _norm(quantity),
    ])
    return str(uuid.uuid5(NAMESPACE_LINEITEM, key))

PACK_RE = re.compile(r"\bPacks?\s+of\s+(\d+)\b", re.I)

def to_int(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return pd.NA
        s = str(x).strip()
        if s == "": return pd.NA
        return int(float(s))
    except Exception:
        return pd.NA

def to_float(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return pd.NA
        s = str(x).replace("$", "").replace(",", "").strip()
        if s == "": return pd.NA
        return float(s)
    except Exception:
        return pd.NA

def infer_pack_qty(description: str) -> int:
    if not description:
        return 1
    m = PACK_RE.search(description)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return 1
    return 1


def ingest_receipts(pdf_paths: list[Path], debug: bool = False):
    """Parse a mixed set of vendor PDFs into orders, line_items, and inventory rollups."""

    # Persistent registry so re-runs don't re-ingest the same PDF bytes
    project_root = Path(__file__).resolve().parents[1]
    dbfile = project_root / "studio_inventory.sqlite"
    registry = IngestRegistry(dbfile)

    order_rows = []
    item_rows = []

    # Also avoid duplicates within the same selection (before the registry is updated)
    seen_hashes: set[str] = set()

    for pdf_path in pdf_paths:
        pdf_path = Path(pdf_path)

        parser = pick_parser(pdf_path)
        if parser is None:
            print(f"⚠️  No parser matched: {pdf_path.name} (skipping)")
            continue

        file_hash = sha256_file(pdf_path)

        if (file_hash in seen_hashes) or registry.has_hash(file_hash):
            moved = move_to_duplicates(pdf_path)
            print(f"🟡 DUPLICATE skipped: {pdf_path.name} → {moved.name}")
            continue
        seen_hashes.add(file_hash)

        if debug:
            print(f"\\n=== {parser.vendor.upper()} :: {pdf_path.name} ===")

        try:
            order = parser.parse_order(pdf_path, debug=debug)
            items = parser.parse_line_items(pdf_path, debug=debug)
        except Exception as e:
            print(f"❌ Parse failed: {pdf_path.name} ({e})")
            continue

        vendor = getattr(parser, "vendor", None) or _first_nonempty(order, ("vendor",), default="unknown") or "unknown"
        order_id = _first_nonempty(order, ("order_id", "order", "invoice", "invoice_no", "id", "number"), default="unknown")

        order_uid = make_order_uid(vendor, order_id, file_hash)

        od = dict(order.__dict__)
        od["file_hash"] = file_hash
        od["order_uid"] = order_uid
        order_rows.append(od)

        for i, it in enumerate(items):
            d = dict(it.__dict__)
            d.setdefault("vendor", vendor)
            d.setdefault("order_id", order_id)
            d["file_hash"] = file_hash
            d["order_uid"] = order_uid

            part_number = d.get("part_number") or d.get("sku") or d.get("mfg_part") or ""
            description = d.get("description") or ""
            unit_price = d.get("unit_price") or d.get("price") or ""
            quantity = d.get("ordered") or d.get("quantity") or d.get("qty") or d.get("shipped") or ""

            d["line_item_uid"] = make_line_item_uid(
                vendor=vendor,
                order_id=order_id,
                file_hash=file_hash,
                line_index=i,
                part_number=str(part_number),
                description=str(description),
                unit_price=str(unit_price),
                quantity=str(quantity),
            )
            item_rows.append(d)

        # Register only after successful parse (so failures aren't marked as ingested)
        registry.register(file_hash, pdf_path, vendor=vendor, order_id=order_id)

    orders_df = pd.DataFrame(order_rows)
    line_items_df = pd.DataFrame(item_rows)

    # Add label fields for all vendors (for drawer/bin labels)
    if not line_items_df.empty:
        def _row_label(r):
            desc_clean, l1, l2 = make_label_fields(
                vendor=str(r.get("vendor", "") or ""),
                sku=str(r.get("sku", "") or ""),
                description=str(r.get("description", "") or ""),
                mfg_pn=(None if "mfg_pn" not in r else r.get("mfg_pn"))
            )
            return desc_clean, l1, l2

        labels = line_items_df.apply(_row_label, axis=1, result_type="expand")
        labels.columns = ["desc_clean", "label_line1", "label_line2"]
        line_items_df = line_items_df.join(labels)
    else:
        line_items_df["desc_clean"] = []
        line_items_df["label_line1"] = []
        line_items_df["label_line2"] = []

    # Normalize numeric types (keep vendor-specific extra cols intact)
    for col in ["merchandise", "shipping", "sales_tax", "total"]:
        if col in orders_df.columns:
            orders_df[col] = orders_df[col].apply(to_float)

    for col in ["line", "ordered", "shipped", "balance"]:
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_int)

    for col in ["unit_price", "line_total"]:
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
                need2 = line_items_df["unit_price"].isna() & lt.notna()
                line_items_df.loc[need2, "unit_price"] = lt[need2].astype(float)
            else:
                line_items_df.loc[need, "unit_price"] = lt[need].astype(float)

    # Inventory rollup helpers
    if not line_items_df.empty:
        if "description" in line_items_df.columns:
            line_items_df["pack_qty"] = line_items_df["description"].apply(infer_pack_qty)
        else:
            line_items_df["pack_qty"] = 1

        line_items_df["units_received"] = (
            pd.to_numeric(line_items_df.get("shipped"), errors="coerce").fillna(0).astype(int)
            * pd.to_numeric(line_items_df.get("pack_qty"), errors="coerce").fillna(1).astype(int)
        )

        # Fill missing totals if possible
        if ("line_total" in line_items_df.columns) and ("ordered" in line_items_df.columns) and ("unit_price" in line_items_df.columns):
            computed_total = (
                pd.to_numeric(line_items_df["ordered"], errors="coerce")
                * pd.to_numeric(line_items_df["unit_price"], errors="coerce")
            )
            line_items_df["line_total"] = line_items_df["line_total"].fillna(computed_total)

        # A stable part key: prefer vendor+sku, fallback vendor+mfg_part, fallback vendor+description hash
        def _part_key(row):
            v = str(row.get("vendor") or "")
            sku = str(row.get("sku") or "").strip()
            mfg = str(row.get("mfg_part") or "").strip()
            desc = str(row.get("description") or "").strip()
            if sku:
                return f"{v}:{sku}"
            if mfg:
                return f"{v}:{mfg}"
            return f"{v}:{hash(desc)}"

        line_items_df["part_key"] = line_items_df.apply(_part_key, axis=1)
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
                mfg_part=("mfg_part", "first") if "mfg_part" in line_items_df.columns else ("vendor", "first"),
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
                total_spend=("line_total", "sum") if "line_total" in line_items_df.columns else ("units_received", "sum"),
                last_invoice=("invoice", "max") if "invoice" in line_items_df.columns else ("vendor", "first"),
            )
        )
        parts_received_df["avg_unit_cost"] = parts_received_df["total_spend"] / parts_received_df["units_received"].replace({0: pd.NA})
    else:
        parts_received_df = pd.DataFrame(columns=["part_key", "vendor", "sku", "description", "desc_clean", "label_line1", "label_line2", "label_short", "purchase_url", "airtable_url", "label_qr_url", "label_qr_text", "units_received", "total_spend", "last_invoice", "avg_unit_cost"])

    parts_removed_df = pd.DataFrame(columns=["removal_uid","part_key","qty_removed","ts_utc","project","note"])
    return orders_df, line_items_df, parts_received_df, parts_removed_df


def upsert_master_csv(new_df: pd.DataFrame, master_path: Path, key_cols: list[str]) -> pd.DataFrame:
    """Append new_df into master_path, dropping duplicates by key_cols."""
    master_path.parent.mkdir(parents=True, exist_ok=True)

    if master_path.exists():
        old = pd.read_csv(master_path)
        combined = pd.concat([old, new_df], ignore_index=True)
    else:
        combined = new_df.copy()

    # Ensure key cols exist
    for c in key_cols:
        if c not in combined.columns:
            combined[c] = pd.NA

    combined = combined.drop_duplicates(subset=key_cols, keep="last")
    combined.to_csv(master_path, index=False)
    return combined


# ----------------------------
# SQLite DB upserts (orders, line_items, inventory)
# ----------------------------

def _ensure_table(conn: sqlite3.Connection, table: str, pk_col: str):
    conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ("{pk_col}" TEXT PRIMARY KEY);')


def _existing_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table}");').fetchall()
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


def update_database(orders_df: pd.DataFrame, line_items_df: pd.DataFrame, parts_received_df: pd.DataFrame, parts_removed_df: pd.DataFrame, dbfile: Path):
    init_inventory_db(dbfile)
    with sqlite3.connect(dbfile) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        _upsert_df(conn, "orders", orders_df, pk_col="order_uid")
        _upsert_df(conn, "line_items", line_items_df, pk_col="line_item_uid")
        _upsert_df(conn, "parts_received", parts_received_df, pk_col="part_key")
        _upsert_df(conn, "parts_removed", parts_removed_df, pk_col="removal_uid")

        # Refresh materialized on-hand snapshot from the view (DELETE+INSERT for broad SQLite compatibility)
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
    return inventory_on_hand_df

def cli():
    import os

    print("=== Mixed Vendor Receipt Ingest (CLI) ===")
    folder = Path(input("Receipts folder path: ").strip() or ".").expanduser().resolve()
    pdf_paths = sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF"))
    if not pdf_paths:
        print(f"No PDFs found in {folder}")
        return

    debug = (input("Debug prints? [y/N]: ").strip().lower() == "y")

    orders_df, line_items_df, parts_received_df, parts_removed_df = ingest_receipts(pdf_paths, debug=debug)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_dir = folder / "exports"
    export_dir.mkdir(exist_ok=True)

    # per-run exports
    orders_df.to_csv(export_dir / f"orders_{stamp}.csv", index=False)
    line_items_df.to_csv(export_dir / f"line_items_{stamp}.csv", index=False)
    parts_received_df.to_csv(export_dir / f"parts_received_{stamp}.csv", index=False)
    parts_removed_df.to_csv(export_dir / f"parts_removed_{stamp}.csv", index=False)

    # master upserts
    orders_master = upsert_master_csv(
        orders_df,
        export_dir / "orders_master.csv",
        key_cols=["vendor", "invoice", "source_file"],
    )
    items_master = upsert_master_csv(
        line_items_df,
        export_dir / "line_items_master.csv",
        key_cols=["vendor", "invoice", "line", "sku", "source_file"],
    )

    parts_removed_master = upsert_master_csv(
        parts_removed_df,
        export_dir / "parts_removed_master.csv",
        key_cols=["removal_uid"],
    )

    # inventory recompute from master line items (so it stays consistent)
    if not items_master.empty:
        _, _, parts_received_master, _ = ingest_receipts([])  # create empty with correct cols
        # quick recompute using the same rollup logic without reparsing PDFs:
        # easiest: reuse the rollup block by calling ingest_receipts on none is messy.
        # Instead, compute here:
        items = items_master.copy()
        if "description" in items.columns:
            items["pack_qty"] = items["description"].apply(infer_pack_qty)
        else:
            items["pack_qty"] = 1
        items["units_received"] = (
            pd.to_numeric(items.get("shipped"), errors="coerce").fillna(0).astype(int)
            * pd.to_numeric(items.get("pack_qty"), errors="coerce").fillna(1).astype(int)
        )

        def _part_key(row):
            v = str(row.get("vendor") or "")
            sku = str(row.get("sku") or "").strip()
            mfg = str(row.get("mfg_part") or "").strip()
            desc = str(row.get("description") or "").strip()
            if sku:
                return f"{v}:{sku}"
            if mfg:
                return f"{v}:{mfg}"
            return f"{v}:{hash(desc)}"

        items["part_key"] = items.apply(_part_key, axis=1)

        parts_received_master = (
            items.groupby("part_key", as_index=False)
            .agg(
                vendor=("vendor", "first"),
                sku=("sku", "first"),
                mfg_part=("mfg_part", "first") if "mfg_part" in items.columns else ("vendor", "first"),
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
                total_spend=("line_total", "sum") if "line_total" in items.columns else ("units_received", "sum"),
                last_invoice=("invoice", "max") if "invoice" in items.columns else ("vendor", "first"),
            )
        )
        parts_received_master["avg_unit_cost"] = parts_received_master["total_spend"] / parts_received_master["units_received"].replace({0: pd.NA})
        parts_received_master.to_csv(export_dir / "parts_received_master.csv", index=False)

        # Update SQLite DB from master views
        inventory_on_hand_df = update_database(orders_master, items_master, parts_received_master, parts_removed_master, dbfile=dbfile)
        inventory_on_hand_df.to_csv(export_dir / f"inventory_on_hand_{stamp}.csv", index=False)


    print("\n✅ Done.")
    print("Per-run CSVs and master CSVs written to:", export_dir)


if __name__ == "__main__":
    cli()