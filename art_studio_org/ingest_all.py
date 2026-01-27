from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime

import hashlib
import shutil
import sqlite3
import uuid

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

        inventory_df = (
            line_items_df.groupby("part_key", as_index=False)
            .agg(
                vendor=("vendor", "first"),
                sku=("sku", "first"),
                mfg_part=("mfg_part", "first") if "mfg_part" in line_items_df.columns else ("vendor", "first"),
                description=("description", "first"),
                units_received=("units_received", "sum"),
                total_spend=("line_total", "sum") if "line_total" in line_items_df.columns else ("units_received", "sum"),
                last_invoice=("invoice", "max") if "invoice" in line_items_df.columns else ("vendor", "first"),
            )
        )
        inventory_df["avg_unit_cost"] = inventory_df["total_spend"] / inventory_df["units_received"].replace({0: pd.NA})
    else:
        inventory_df = pd.DataFrame(columns=["part_key", "vendor", "sku", "description", "units_received", "total_spend", "last_invoice", "avg_unit_cost"])

    return orders_df, line_items_df, inventory_df


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


def _ensure_columns(conn: sqlite3.Connection, table: str, cols: list[str]):
    existing = _existing_columns(conn, table)
    for c in cols:
        if c not in existing:
            conn.execute(f'ALTER TABLE "{table}" ADD COLUMN "{c}" TEXT;')


def _upsert_df(conn: sqlite3.Connection, table: str, df: pd.DataFrame, pk_col: str):
    if df is None or df.empty:
        return

    _ensure_table(conn, table, pk_col)
    cols = [c for c in df.columns if c]
    _ensure_columns(conn, table, cols)

    if "updated_utc" not in cols:
        df = df.copy()
        df["updated_utc"] = datetime.utcnow().isoformat()
        cols = cols + ["updated_utc"]
        _ensure_columns(conn, table, ["updated_utc"])

    col_list = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["?"] * len(cols))
    update_set = ", ".join([f'"{c}"=excluded."{c}"' for c in cols if c != pk_col])

    sql = f"""
        INSERT INTO "{table}" ({col_list})
        VALUES ({placeholders})
        ON CONFLICT("{pk_col}") DO UPDATE SET {update_set};
    """

    rows = [tuple("" if pd.isna(v) else v for v in r) for r in df[cols].itertuples(index=False, name=None)]
    conn.executemany(sql, rows)


def init_inventory_db(dbfile: Path):
    dbfile.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(dbfile) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ingested_files (
                file_hash TEXT PRIMARY KEY,
                first_seen_utc TEXT NOT NULL,
                original_path TEXT,
                vendor TEXT,
                order_id TEXT
            );
        """)

        _ensure_table(conn, "orders", "order_uid")
        _ensure_table(conn, "line_items", "line_item_uid")
        _ensure_table(conn, "inventory", "part_key")

        conn.execute('CREATE INDEX IF NOT EXISTS idx_line_items_order_uid ON line_items(order_uid);')
        conn.commit()


def update_database(dbfile: Path, orders_df: pd.DataFrame, line_items_df: pd.DataFrame, inventory_df: pd.DataFrame):
    init_inventory_db(dbfile)
    with sqlite3.connect(dbfile) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        _upsert_df(conn, "orders", orders_df, pk_col="order_uid")
        _upsert_df(conn, "line_items", line_items_df, pk_col="line_item_uid")
        _upsert_df(conn, "inventory", inventory_df, pk_col="part_key")
        conn.commit()

def cli():
    import os

    print("=== Mixed Vendor Receipt Ingest (CLI) ===")
    folder = Path(input("Receipts folder path: ").strip() or ".").expanduser().resolve()
    pdf_paths = sorted(folder.glob("*.pdf")) + sorted(folder.glob("*.PDF"))
    if not pdf_paths:
        print(f"No PDFs found in {folder}")
        return

    debug = (input("Debug prints? [y/N]: ").strip().lower() == "y")

    orders_df, line_items_df, inventory_df = ingest_receipts(pdf_paths, debug=debug)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_dir = folder / "exports"
    export_dir.mkdir(exist_ok=True)

    # per-run exports
    orders_df.to_csv(export_dir / f"orders_{stamp}.csv", index=False)
    line_items_df.to_csv(export_dir / f"line_items_{stamp}.csv", index=False)
    inventory_df.to_csv(export_dir / f"inventory_{stamp}.csv", index=False)

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

    # inventory recompute from master line items (so it stays consistent)
    if not items_master.empty:
        _, _, inv_master = ingest_receipts([])  # create empty with correct cols
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

        inv_master = (
            items.groupby("part_key", as_index=False)
            .agg(
                vendor=("vendor", "first"),
                sku=("sku", "first"),
                mfg_part=("mfg_part", "first") if "mfg_part" in items.columns else ("vendor", "first"),
                description=("description", "first"),
                units_received=("units_received", "sum"),
                total_spend=("line_total", "sum") if "line_total" in items.columns else ("units_received", "sum"),
                last_invoice=("invoice", "max") if "invoice" in items.columns else ("vendor", "first"),
            )
        )
        inv_master["avg_unit_cost"] = inv_master["total_spend"] / inv_master["units_received"].replace({0: pd.NA})
        inv_master.to_csv(export_dir / "inventory_master.csv", index=False)

        # Update SQLite DB from master views
        update_database(dbfile, orders_master, items_master, inv_master)


    print("\n✅ Done.")
    print("Per-run CSVs and master CSVs written to:", export_dir)


if __name__ == "__main__":
    cli()
