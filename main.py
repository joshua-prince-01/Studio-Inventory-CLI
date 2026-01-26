import os
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

from Read_Line_Items import parse_receipt
from Read_Order_Details import extract_order_info_by_page


# ----------------------------
# Folder + PDF pickers
# ----------------------------

def pick_folder_from_cwd(start_dir=None) -> Path:
    """
    Interactive folder navigator starting at start_dir (default: cwd).
    Commands:
      - number : enter that folder
      - .      : select current folder
      - ..     : go up one folder
      - /path, ~/path, relative/path : jump to a path
      - q      : quit
    """
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

        # Jump to a path
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

        # Number selection
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
        out = []
        for part in choice.split(","):
            part = part.strip()
            if not part.isdigit():
                return []
            idx = int(part) - 1
            if not (0 <= idx < len(pdfs)):
                return []
            out.append(pdfs[idx])
        return out

    # substring match
    needle = choice
    matches = [p for p in pdfs if needle in p.name.lower()]
    return matches


# ----------------------------
# Ingest helpers
# ----------------------------

PACK_RE = re.compile(r"\bPacks?\s+of\s+(\d+)\b", re.I)

def to_int(x):
    try:
        return int(str(x).strip())
    except Exception:
        return pd.NA

def to_float(x):
    try:
        return float(str(x).replace("$", "").replace(",", "").strip())
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


def ingest_mcmaster_receipts(pdf_paths: list[Path], debug: bool = False):
    order_rows = []
    item_rows = []

    for pdf_path in pdf_paths:
        if debug:
            print(f"\n=== Processing: {pdf_path.name} ===")

        # Order-level info (your module)
        info = extract_order_info_by_page(str(pdf_path), debug=debug)
        invoice = info.invoice
        purchase_order = info.purchase_order

        order_rows.append({
            "vendor": "mcmaster",
            "source_file": pdf_path.name,
            "pdf_path": str(pdf_path),

            "purchase_order": purchase_order,
            "invoice": invoice,
            "invoice_date": info.invoice_date,
            "account_number": info.account_number,
            "payment_date": info.payment_date,
            "credit_card": info.credit_card,

            "merchandise": info.merchandise,
            "shipping": info.shipping,
            "sales_tax": info.sales_tax,
            "total": info.total,
        })

        # Line items (your module)
        items = parse_receipt(str(pdf_path), page_num=0, debug=debug)
        for d in items:
            item_rows.append({
                "vendor": "mcmaster",
                "source_file": pdf_path.name,
                "invoice": invoice,
                "purchase_order": purchase_order,

                "line": d.get("line"),
                "sku": d.get("sku"),
                "description": d.get("description"),
                "ordered": d.get("ordered"),
                "shipped": d.get("shipped"),
                "balance": d.get("balance"),
                "unit_price": d.get("price"),
                "line_total": d.get("total"),
            })

    orders_df = pd.DataFrame(order_rows)
    line_items_df = pd.DataFrame(item_rows)

    # Normalize types
    for col in ["merchandise", "shipping", "sales_tax", "total"]:
        if col in orders_df.columns:
            orders_df[col] = orders_df[col].apply(to_float)

    for col in ["line", "ordered", "shipped"]:
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_int)

    for col in ["unit_price", "line_total"]:
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_float)

    # Inventory rollup helpers
    line_items_df["pack_qty"] = line_items_df["description"].apply(infer_pack_qty)
    line_items_df["units_received"] = (
        pd.to_numeric(line_items_df["shipped"], errors="coerce").fillna(0).astype(int)
        * pd.to_numeric(line_items_df["pack_qty"], errors="coerce").fillna(1).astype(int)
    )

    # Fill missing totals if needed
    computed_total = (
        pd.to_numeric(line_items_df["ordered"], errors="coerce")
        * pd.to_numeric(line_items_df["unit_price"], errors="coerce")
    )
    line_items_df["line_total"] = line_items_df["line_total"].fillna(computed_total)

    # Inventory rollup
    line_items_df["part_key"] = (
        line_items_df["vendor"].astype(str) + ":" + line_items_df["sku"].astype(str)
    )

    inventory_df = (
        line_items_df.groupby("part_key", as_index=False)
        .agg(
            vendor=("vendor", "first"),
            sku=("sku", "first"),
            description=("description", "first"),
            units_received=("units_received", "sum"),
            total_spend=("line_total", "sum"),
            last_invoice=("invoice", "max"),
        )
    )
    inventory_df["avg_unit_cost"] = inventory_df["total_spend"] / inventory_df["units_received"].replace({0: pd.NA})

    return orders_df, line_items_df, inventory_df


# ----------------------------
# MAIN
# ----------------------------

def main():
    print("=== Receipt Ingest (CLI) ===")
    print(f"Python sees cwd as: {Path.cwd().resolve()}")

    receipts_folder = pick_folder_from_cwd()   # browse from cwd
    pdf_paths = pick_pdfs_in_folder(receipts_folder)

    if not pdf_paths:
        print("Nothing selected. Exiting.")
        return

    debug = (input("\nDebug prints? [y/N]: ").strip().lower() == "y")

    orders_df, line_items_df, inventory_df = ingest_mcmaster_receipts(pdf_paths, debug=debug)

    print("\n--- ORDERS (head) ---")
    print(orders_df.head(10).to_string(index=False))

    print("\n--- LINE ITEMS (head) ---")
    print(line_items_df.head(15).to_string(index=False))

    print("\n--- INVENTORY (top spend) ---")
    print(inventory_df.sort_values("total_spend", ascending=False).head(20).to_string(index=False))

    # ----------------------------
    # EXPORT CSVs
    # ----------------------------

    default_export_dir = receipts_folder / "exports"

    print("\nExport CSV files")
    print(f"Press Enter to use default:")
    print(f"  {default_export_dir}")
    print("Or type a path (.., ../exports, ~/Desktop/csv, etc.)")

    export_input = input("> ").strip()

    if export_input:
        export_dir = Path(export_input).expanduser()
        if not export_dir.is_absolute():
            export_dir = (receipts_folder / export_dir).resolve()
    else:
        export_dir = default_export_dir

    # create folder if missing
    export_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    orders_csv = export_dir / f"orders_{stamp}.csv"
    items_csv = export_dir / f"line_items_{stamp}.csv"
    inv_csv = export_dir / f"inventory_{stamp}.csv"

    orders_df.to_csv(orders_csv, index=False)
    line_items_df.to_csv(items_csv, index=False)
    inventory_df.to_csv(inv_csv, index=False)

    print("\n✅ CSV files saved:")
    print(" ", orders_csv)
    print(" ", items_csv)
    print(" ", inv_csv)

if __name__ == "__main__":
    main()