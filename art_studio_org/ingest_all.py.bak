from __future__ import annotations

import re
from pathlib import Path
from datetime import datetime

import pandas as pd

from vendors.registry import pick_parser


# ----------------------------
# Shared helpers
# ----------------------------

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

    order_rows = []
    item_rows = []

    for pdf_path in pdf_paths:
        parser = pick_parser(pdf_path)
        if parser is None:
            print(f"⚠️  No parser matched: {pdf_path.name} (skipping)")
            continue

        if debug:
            print(f"\n=== {parser.vendor.upper()} :: {pdf_path.name} ===")

        order = parser.parse_order(pdf_path, debug=debug)
        items = parser.parse_line_items(pdf_path, debug=debug)

        order_rows.append(order.__dict__)

        for it in items:
            item_rows.append(it.__dict__)

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

    print("\n✅ Done.")
    print("Per-run CSVs and master CSVs written to:", export_dir)


if __name__ == "__main__":
    cli()
