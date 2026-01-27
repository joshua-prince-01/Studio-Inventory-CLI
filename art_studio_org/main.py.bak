# art_studio_org/main.py
# Unified CLI ingest for multiple vendors via art_studio_org.vendors.registry.pick_parser
# Includes per-run log file written to project_root/log/

from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime
from pathlib import Path
import re
import traceback
import pandas as pd

from art_studio_org.vendors.registry import pick_parser


# ----------------------------
# Simple run logger
# ----------------------------

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


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def create_run_log(echo: bool = True) -> RunLogger:
    root = project_root()
    log_dir = root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"run_{stamp}.txt"
    logger = RunLogger(log_path=log_path, echo=echo)
    logger.log(f"Log file: {log_path}")
    logger.log(f"Project root: {root}")
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

PACK_RE = re.compile(r"\bPacks?\s+of\s+(\d+)\b", re.I)


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

    def log(msg: str):
        if logger:
            logger.log(msg)
        else:
            print(msg)

    for pdf_path in pdf_paths:
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

            order_rows.append({
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

            for d in items:
                row = {
                    "vendor": vendor,
                    "source_file": pdf_path.name,
                    "invoice": info.get("invoice"),
                    "purchase_order": info.get("purchase_order"),
                    "line": d.get("line"),
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
        inventory_df = pd.DataFrame(columns=[
            "part_key", "vendor", "sku", "description",
            "units_received", "total_spend", "last_invoice", "avg_unit_cost"
        ])
        return orders_df, line_items_df, inventory_df

    for col in ("line", "ordered", "shipped", "balance"):
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_int)

    for col in ("unit_price", "line_total"):
        if col in line_items_df.columns:
            line_items_df[col] = line_items_df[col].apply(to_float)

    if "description" not in line_items_df.columns:
        line_items_df["description"] = ""

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

        orders_df, line_items_df, inventory_df = ingest_receipts(pdf_paths, debug=debug, logger=logger)

        print("\n--- ORDERS (head) ---")
        print(orders_df.head(10).to_string(index=False))

        print("\n--- LINE ITEMS (head) ---")
        print(line_items_df.head(15).to_string(index=False))

        print("\n--- INVENTORY (top spend) ---")
        if inventory_df.empty:
            print("(empty)")
        else:
            print(inventory_df.sort_values("total_spend", ascending=False).head(20).to_string(index=False))
        # this was commented out becuase it would place the default folder inside where we grabbed reciepts, not
        # the default 'exports' folder of the project:
        #default_export_dir = (receipts_folder.parent / "exports").resolve()
        default_export_dir = (project_root() / "exports").resolve()

        logger.log(f"Default export dir: {default_export_dir}")

        export_dir = pick_export_folder(default_export_dir)
        if export_dir is None:
            logger.log("Export cancelled.")
            return

        export_dir.mkdir(parents=True, exist_ok=True)

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        orders_csv = export_dir / f"orders_{stamp}.csv"
        items_csv = export_dir / f"line_items_{stamp}.csv"
        inv_csv = export_dir / f"inventory_{stamp}.csv"

        orders_df.to_csv(orders_csv, index=False)
        line_items_df.to_csv(items_csv, index=False)
        inventory_df.to_csv(inv_csv, index=False)

        logger.log("CSV files saved:")
        logger.log(f"  {orders_csv}")
        logger.log(f"  {items_csv}")
        logger.log(f"  {inv_csv}")

        print("\n✅ CSV files saved:")
        print(" ", orders_csv)
        print(" ", items_csv)
        print(" ", inv_csv)

    finally:
        logger.close()


if __name__ == "__main__":
    main()
