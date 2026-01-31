from __future__ import annotations
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Any
from uuid import uuid4

import csv
import os
import subprocess
import sys

from importlib import resources
import shutil

# ----------------------------
# Workspace (runtime data) paths
# ----------------------------
from studio_inventory.paths import (
    workspace_root,
    ensure_workspace,
    exports_dir,
    receipts_dir,
    log_dir,
    label_presets_dir,
    label_templates_dir,
    secrets_dir,
    project_root,
)

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, FloatPrompt, Confirm
from rich.table import Table

from studio_inventory.db import DB, default_db_path

from studio_inventory.labels.make_pdf import make_labels_pdf, LabelTemplate
from studio_inventory.labels.presets import list_label_presets, load_label_preset, save_label_preset

app = typer.Typer(add_completion=False, no_args_is_help=False)
console = Console()


# ----------------------------
# Helpers
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

def ensure_inventory_events_table(db: DB) -> None:
    # Unified audit log for manual receive/remove actions
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_events (
            event_uid   TEXT PRIMARY KEY,
            ts_utc      TEXT NOT NULL,
            event_type  TEXT NOT NULL,   -- 'receive' or 'remove'
            part_key    TEXT NOT NULL,
            qty         REAL NOT NULL,
            unit_cost   REAL,
            total_cost  REAL,
            project     TEXT,
            note        TEXT
        )
        """
    )

def header():
    console.print(Panel.fit("[bold]Studio Inventory[/bold]\nMenu-first CLI", border_style="cyan"))

def pause():
    console.print()
    input("Press Enter to continue...")

def get_db(db_path: Optional[Path] = None) -> DB:
    return DB(path=db_path or default_db_path())

def safe_str(v) -> str:
    return "" if v is None else str(v)


def row_get(row: Any, key: str, default=None):
    """Safe getter for sqlite3.Row (and dict-like objects)."""
    try:
        return row[key]  # sqlite3.Row supports mapping access
    except Exception:
        try:
            return row.get(key, default)  # type: ignore[attr-defined]
        except Exception:
            return default


def _table_exists(db: DB, table: str) -> bool:
    try:
        return db.scalar("SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1", [table]) is not None
    except Exception:
        return False


def _table_columns(db: DB, table: str) -> set[str]:
    if not _table_exists(db, table):
        return set()
    with db.connect() as con:
        rows = con.execute(f'PRAGMA table_info("{table}");').fetchall()
    return {r[1] for r in rows}  # (cid, name, type, notnull, dflt_value, pk)


def _ensure_columns(db: DB, table: str, cols: dict[str, str]) -> None:
    existing = _table_columns(db, table)
    if not existing:
        return
    with db.connect() as con:
        for col, coltype in cols.items():
            if col in existing:
                continue
            try:
                con.execute(f'ALTER TABLE "{table}" ADD COLUMN "{col}" {coltype};')
            except Exception:
                # Ignore if already exists/locked; caller queries should be defensive.
                pass


def ensure_orders_ingest_schema(db: DB) -> None:
    """Forward-compatible schema for archived import paths + ingest metadata."""
    # Orders table
    _ensure_columns(db, "orders", {
        "archived_path": "TEXT",
        "original_path": "TEXT",
        "order_ref": "TEXT",
        # soft-delete / void support
        "is_voided": "INTEGER DEFAULT 0",
        "voided_utc": "TEXT",
    })
    # Ingested files table (duplicate stopper)
    _ensure_columns(db, "ingested_files", {
        "vendor": "TEXT",
        "order_ref": "TEXT",
        "original_path": "TEXT",
        "archived_path": "TEXT",
        # optional: keep hash but mark inactive
        "is_voided": "INTEGER DEFAULT 0",
    })
    # Removals table: allow order-level reversals / auditing
    _ensure_columns(db, "parts_removed", {
        "order_uid": "TEXT",
        "file_hash": "TEXT",
        "reason": "TEXT",
    })



def fmt_money(v) -> str:
    try:
        return f"{float(v):,.2f}"
    except (TypeError, ValueError):
        return ""

def shorten(s: str, n: int = 54) -> str:
    s = safe_str(s)
    return s if len(s) <= n else s[: n - 1] + "…"

def parse_row_spec(spec: str) -> list[int]:
    """
    Parse "87:200,205,206" into 1-based row numbers (inclusive ranges).
    """
    out: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" in part:
            a, b = part.split(":", 1)
            a, b = int(a), int(b)
            lo, hi = (a, b) if a <= b else (b, a)
            out.extend(range(lo, hi + 1))
        else:
            out.append(int(part))

    seen = set()
    uniq: list[int] = []
    for n in out:
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq

def timestamp_slug() -> str:
    # local time is fine for filenames
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def seed_label_templates() -> int:
    """
    Copy packaged default templates into workspace label_templates/.
    Does not overwrite existing user files.
    """
    dst = label_templates_dir()
    copied = 0

    pkg_dir = resources.files("studio_inventory") / "label_templates"
    if not pkg_dir.is_dir():
        return 0

    for src in pkg_dir.iterdir():
        if src.is_file() and src.name.endswith(".json"):
            out = dst / src.name
            if not out.exists():
                with resources.as_file(src) as src_path:
                    shutil.copy2(src_path, out)
                copied += 1

    return copied

# ----------------------------
# Ingest runner (subprocess)
# ----------------------------
def run_module_in_subprocess(module_name: str) -> int:
    """
    Run: python -m <module_name> from project root, so relative paths behave.
    Returns process returncode.
    """
    cmd = [sys.executable, "-m", module_name]
    console.print(f"\n[dim]Running:[/dim] {' '.join(cmd)}")
    try:
        # Let the child process use the terminal normally (interactive prompts etc.)
        proc = subprocess.run(cmd, cwd=str(workspace_root()))
        return proc.returncode
    except FileNotFoundError:
        console.print("[red]Python executable not found.[/red]")
        return 1
    except KeyboardInterrupt:
        console.print("\n[yellow]Cancelled.[/yellow]")
        return 130

def run_ingest() -> None:
    """Run the ingest entrypoint in a subprocess.

    Return codes (child process):
      - 0   success (DB updated)
      - 2   cancelled / dry-run (DB not updated)
      - 130 interrupted (Ctrl+C)
    """
    rc = run_module_in_subprocess("studio_inventory.main")
    if rc == 0:
        console.print("[green]Ingest completed.[/green]")
        return
    if rc in (2, 130):
        console.print("[yellow]Ingest cancelled.[/yellow]")
        return

    console.print(f"[yellow]studio_inventory.main exited with code {rc}. Trying fallback…[/yellow]")

    rc2 = run_module_in_subprocess("studio_inventory.ingest_all")
    if rc2 == 0:
        console.print("[green]Ingest completed.[/green]")
        return
    if rc2 in (2, 130):
        console.print("[yellow]Ingest cancelled.[/yellow]")
        return

    console.print(f"[red]Ingest failed.[/red] exit codes: main={rc}, ingest_all={rc2}")

# ----------------------------
# Export helpers
# ----------------------------
def object_columns(db: DB, name: str) -> list[str]:
    info = db.rows(f"PRAGMA table_info({name})")
    return [r["name"] for r in info]

def export_sqlite_object_to_csv(
    db: DB,
    name: str,
    out_path: Path,
    order_by: Optional[str] = None,
    limit: Optional[int] = None,
) -> None:
    cols = object_columns(db, name)
    if not cols:
        raise RuntimeError(f"Could not read columns for {name}")

    sql = f"SELECT * FROM {name}"
    if order_by:
        sql += f" ORDER BY {order_by}"
    if limit:
        sql += f" LIMIT {int(limit)}"

    rows = db.rows(sql)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for r in rows:
            w.writerow([r[c] for c in cols])

    console.print(f"[green]Exported[/green] {name} → [cyan]{out_path}[/cyan] ({len(rows)} rows)")

# ----------------------------
# Menu-first entry
# ----------------------------
@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """Launch the menu UI when no subcommand is provided."""
    if ctx.invoked_subcommand is None:
        run_menu()

def run_menu():
    while True:
        console.clear()
        header()

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "[bold]Orders[/bold] | ingest/review: receipts / packing lists")
        menu.add_row("2.", "[bold]Export[/bold] | make: data (CSV / reports)")
        menu.add_row("3.", "[bold]Inventory[/bold] | browse / search / receive / remove")
        menu.add_row("4.", "[bold]Vendors[/bold] | enrich: (DigiKey / McMaster) [dim](coming soon)[/dim]")
        menu.add_row("5.", "[bold]Labels[/bold] | generate PDFs")
        menu.add_row("6.", "DB diagnostics")
        menu.add_row("0.", "Quit")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "3", "4", "5", "6", "0"], default="3")

        if choice == "1":
            menu_ingest()
        elif choice == "2":
            menu_export()
        elif choice == "3":
            menu_inventory()
        elif choice == "4":
            menu_vendors()
        elif choice == "5":
            menu_labels()
        elif choice == "6":
            menu_db_diagnostics()
        elif choice == "0":
            console.print("\nBye.\n")
            return



# ----------------------------
# Ingest (wired)
# ----------------------------
def menu_ingest():
    db = get_db()

    while True:
        console.clear()
        header()
        console.print("[bold]Ingest[/bold]\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Run ingest")
        menu.add_row("2.", "Browse orders / receipts")
        menu.add_row("3.", "Show recent ingested files")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "3", "0"], default="1")
        if choice == "0":
            return
        if choice == "1":
            run_ingest()
            pause()
        elif choice == "2":
            orders_browse(db)
        elif choice == "3":
            show_recent_ingests(db)



def show_recent_ingests(db: DB):
    ensure_orders_ingest_schema(db)
    if not _table_exists(db, "ingested_files"):
        console.print("[yellow]No ingested file history yet. Run an ingest first.[/yellow]")
        pause()
        return

    console.clear()
    header()
    console.print("[bold]Recent ingests[/bold]\n")

    try:
        rows = db.rows("""
            SELECT first_seen_utc, vendor, order_ref, original_path, archived_path
            FROM ingested_files
            ORDER BY first_seen_utc DESC
            LIMIT 30
        """)
    except Exception as e:
        console.print(f"[red]Query failed:[/red] {e}")
        pause()
        return

    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("first_seen_utc", style="dim", width=22)
    t.add_column("vendor", width=18)
    t.add_column("order_ref", width=14)
    t.add_column("archived", width=7)
    t.add_column("original_path")

    for r in rows:
        archived = safe_str(row_get(r, "archived_path"))
        t.add_row(
            safe_str(row_get(r, "first_seen_utc")),
            safe_str(row_get(r, "vendor")),
            safe_str(row_get(r, "order_ref")),
            "✅" if archived else "",
            shorten(row_get(r, "original_path"), 80),
        )

    console.print(t)
    pause()


# ----------------------------
# Orders / receipts browser
# ----------------------------

def _orders_where(filters: dict[str, str]) -> tuple[str, list[str]]:
    wh = []
    params: list[str] = []

    v = (row_get(filters, "vendor") or "").strip()
    if v:
        wh.append("o.vendor LIKE ?")
        params.append(f"%{v}%")

    oid = (row_get(filters, "order_id") or "").strip()
    if oid:
        wh.append("o.order_id LIKE ?")
        params.append(f"%{oid}%")

    d = (row_get(filters, "date") or "").strip()
    if d:
        wh.append("o.order_date LIKE ?")
        params.append(f"%{d}%")

    where = "" if not wh else "WHERE " + " AND ".join(wh)
    return where, params


def _orders_filter_prompt(filters: dict[str, str]) -> dict[str, str]:
    console.print("\n[bold]Filter orders[/bold]  [dim](Enter keeps current, '*' clears a field)[/dim]")
    vendor = Prompt.ask("Vendor contains", default=filters.get("vendor", ""))
    order_id = Prompt.ask("Order # contains", default=filters.get("order_id", ""))
    date = Prompt.ask("Date contains", default=filters.get("date", ""))

    def norm(s: str) -> str:
        s = (s or "").strip()
        return "" if s == "*" else s

    return {"vendor": norm(vendor), "order_id": norm(order_id), "date": norm(date)}


def _orders_sort_prompt() -> str:
    console.print("\n[bold]Sort orders[/bold]")
    console.print("  1) newest ingested (default)")
    console.print("  2) vendor, order")
    console.print("  3) total desc")
    console.print("  4) date desc")
    choice = Prompt.ask("Choose", choices=["1","2","3","4"], default="1")
    if choice == "2":
        return "o.vendor, o.order_id"
    if choice == "3":
        return "COALESCE(o.total, 0) DESC, o.vendor"
    if choice == "4":
        return "COALESCE(o.order_date, '') DESC, o.vendor, o.order_id"
    # Newest ingested (first_seen_utc is ISO)
    return "(i.first_seen_utc IS NULL), i.first_seen_utc DESC, o.order_uid DESC"


def orders_browse(db: DB, *, page_size: int = 20) -> None:
    ensure_orders_ingest_schema(db)
    if not _table_exists(db, "orders"):
        console.print("[yellow]No orders have been ingested yet. Run an ingest first.[/yellow]")
        pause()
        return

    if not db.path.exists():
        console.print(f"[red]DB not found:[/red] {db.path}")
        pause()
        return

    filters = {"vendor": "", "order_id": "", "date": ""}
    order_by = "(i.first_seen_utc IS NULL), i.first_seen_utc DESC, o.order_uid DESC"
    page = 0

    while True:
        console.clear()
        header()
        console.print("[bold]Orders / receipts[/bold]  (row # details; n/p page; f filter; s sort; q back)\n")

        where, params = _orders_where(filters)

        try:
            total = int(db.scalar(
                f"SELECT COUNT(*) FROM orders o LEFT JOIN ingested_files i ON i.file_hash = o.file_hash {where}",
                params,
            ) or 0)
        except Exception as e:
            console.print(f"[red]Query failed:[/red] {e}")
            pause()
            return

        max_page = 0 if total == 0 else (total - 1) // page_size
        page = max(0, min(page, max_page))
        offset = page * page_size

        sql = f"""
            SELECT
                o.order_uid,
                o.vendor,
                o.order_id,
                o.order_date,
                o.total,
                o.file_hash,
                COALESCE(o.is_voided,0) AS is_voided,
                i.first_seen_utc,
                COALESCE(o.archived_path, i.archived_path) AS archived_path,
                COALESCE(o.original_path, i.original_path) AS original_path,
                COALESCE(o.order_ref, i.order_ref) AS order_ref
            FROM orders o
            LEFT JOIN ingested_files i ON i.file_hash = o.file_hash
            {where}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """

        rows = db.rows(sql, params + [page_size, offset])

        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("#", justify="right", width=4)
        t.add_column("vendor", width=16)
        t.add_column("order", width=14)
        t.add_column("date", width=12)
        t.add_column("status", width=6)
        t.add_column("total", justify="right", width=10)
        t.add_column("arch", width=4)

        for i, r in enumerate(rows, start=1):
            arch = safe_str(row_get(r, "archived_path"))
            total_s = "" if row_get(r, "total") is None else f"{float(row_get(r, 'total')):,.2f}"
            t.add_row(
                str(i),
                safe_str(row_get(r, "vendor")),
                safe_str(row_get(r, "order_id") or row_get(r, "order_ref") or ""),
                safe_str(row_get(r, "order_date")),
                ("VOID" if int(row_get(r, "is_voided") or 0) else ""),
                total_s,
                "✅" if arch else "",
            )

        console.print(t)
        console.print(f"\n[dim]Page {page+1}/{max_page+1}  •  {total} orders  •  Filters: vendor='{filters['vendor']}' order='{filters['order_id']}' date='{filters['date']}'[/dim]")

        cmd = Prompt.ask("\nCommand")
        cmd = (cmd or "").strip().lower()

        if cmd in {"q", "0", "back"}:
            return
        if cmd in {"n", "next"}:
            page = min(page + 1, max_page)
            continue
        if cmd in {"p", "prev", "previous"}:
            page = max(page - 1, 0)
            continue
        if cmd in {"f", "filter"}:
            filters = _orders_filter_prompt(filters)
            page = 0
            continue
        if cmd in {"s", "sort"}:
            order_by = _orders_sort_prompt()
            page = 0
            continue

        if cmd.isdigit():
            idx = int(cmd)
            if 1 <= idx <= len(rows):
                _show_order_details(db, rows[idx - 1]["order_uid"])
            else:
                console.print("[yellow]Row out of range.[/yellow]")
                pause()
            continue

        console.print("[dim]Commands: row#, n, p, f, s, q[/dim]")
        pause()


def _show_order_details(db: DB, order_uid: str) -> None:
    ensure_orders_ingest_schema(db)
    while True:
        console.clear()
        header()

        o = db.rows(
            """
            SELECT
                o.*,
                i.first_seen_utc,
                COALESCE(o.archived_path, i.archived_path) AS archived_path,
                COALESCE(o.original_path, i.original_path) AS original_path,
                COALESCE(o.order_ref, i.order_ref) AS order_ref
            FROM orders o
            LEFT JOIN ingested_files i ON i.file_hash = o.file_hash
            WHERE o.order_uid = ?
            """,
            [order_uid],
        )
        if not o:
            console.print("[yellow]Order not found.[/yellow]")
            pause()
            return
        o = o[0]

        archived = safe_str(row_get(o, "archived_path"))
        original = safe_str(row_get(o, "original_path"))

        body = []
        body.append(f"[bold]Vendor:[/bold] {safe_str(row_get(o, 'vendor'))}")
        is_voided = bool(int(row_get(o, 'is_voided') or 0))
        if is_voided:
            body.append("[bold red]Status:[/bold red] VOIDED")
        else:
            body.append("[bold green]Status:[/bold green] ACTIVE")
        body.append(f"[bold]Order:[/bold] {safe_str(row_get(o, 'order_id') or row_get(o, 'order_ref') or '')}")
        body.append(f"[bold]Order date:[/bold] {safe_str(row_get(o, 'order_date'))}")
        body.append(f"[bold]Ingested:[/bold] {safe_str(row_get(o, 'first_seen_utc'))}")
        body.append(f"[bold]Total:[/bold] {safe_str(row_get(o, 'total'))}")
        body.append(f"[bold]File hash:[/bold] {safe_str(row_get(o, 'file_hash'))}")
        if archived:
            body.append(f"[bold]Archived PDF:[/bold] {archived}")
        if original:
            body.append(f"[bold]Original path:[/bold] {original}")

        console.print(Panel.fit("\n".join(body), title="Order details", border_style="cyan"))

        items = db.rows(
            """
            SELECT line, sku, description, ordered, shipped, units_received, unit_price, line_total
            FROM line_items
            WHERE order_uid = ?
            ORDER BY line
            LIMIT 200
            """,
            [order_uid],
        )

        it = Table(show_header=True, header_style="bold magenta")
        it.add_column("line", justify="right", width=4)
        it.add_column("sku", width=14)
        it.add_column("description")
        it.add_column("qty", justify="right", width=6)
        it.add_column("unit", justify="right", width=9)
        it.add_column("total", justify="right", width=10)

        for r in items:
            it.add_row(
                safe_str(row_get(r, "line")),
                safe_str(row_get(r, "sku")),
                shorten(row_get(r, "description"), 60),
                safe_str(row_get(r, "units_received") or row_get(r, "shipped") or row_get(r, "ordered") or ""),
                safe_str(row_get(r, "unit_price") or ""),
                safe_str(row_get(r, "line_total") or ""),
            )

        console.print(it)

        
        opts = ["b"]
        prompt = "\n[b] Back"
        if archived:
            opts.append("o")
            prompt += "   [o] Open archived PDF"

        if not is_voided:
            opts.append("v")
            prompt += "   [v] Void this order (log removals)"
        else:
            opts.append("u")
            prompt += "   [u] Undo void (remove those removals)"

        opts.append("p")
        prompt += "   [p] Purge order (delete rows + hash)"

        cmd = Prompt.ask(prompt, choices=opts, default="b")

        if cmd == "b":
            return

        if cmd == "o" and archived:
            _open_pdf(Path(archived))
            pause()
            continue

        if cmd == "v" and not is_voided:
            ok = Confirm.ask("Void this order? (adds entries to parts_removed so inventory on-hand stays correct)", default=False)
            if not ok:
                continue
            token = Prompt.ask("Type VOID to confirm", default="")
            if token.strip() != "VOID":
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue
            try:
                n = _void_order_to_parts_removed(db, order_uid)
                console.print(f"[green]Order voided.[/green] Logged {n} removal row(s).")
            except Exception as e:
                console.print(f"[red]Void failed:[/red] {e}")
            pause()
            continue

        if cmd == "u" and is_voided:
            ok = Confirm.ask("Undo void? (removes parts_removed rows created by voiding this order)", default=False)
            if not ok:
                continue
            token = Prompt.ask("Type UNVOID to confirm", default="")
            if token.strip() != "UNVOID":
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue
            try:
                n = _undo_void_order(db, order_uid)
                console.print(f"[green]Void undone.[/green] Removed {n} removal row(s).")
            except Exception as e:
                console.print(f"[red]Unvoid failed:[/red] {e}")
            pause()
            continue

        if cmd == "p":
            ok = Confirm.ask("Purge this order from the DB? (deletes orders + line_items and clears the duplicate stopper hash)", default=False)
            if not ok:
                continue
            token = Prompt.ask("Type DELETE to confirm", default="")
            if token.strip() != "DELETE":
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue
            try:
                _purge_order_and_rebuild(db, order_uid)
                console.print("[green]Order purged. Inventory rebuilt.[/green]")
            except Exception as e:
                console.print(f"[red]Purge failed:[/red] {e}")
            pause()
            return




def _void_order_to_parts_removed(db: DB, order_uid: str) -> int:
    """Marks an order as voided and logs offsetting removals into parts_removed.

    This preserves history (orders/line_items remain) while keeping inventory_view on_hand consistent.
    """
    ensure_orders_ingest_schema(db)
    ts = utc_now_iso()
    ensure_inventory_events_table(db)

    with db.connect() as con:
        con.execute("PRAGMA foreign_keys = ON;")

        o = con.execute(
            "SELECT order_uid, vendor, order_id, order_ref, order_date, file_hash, COALESCE(is_voided,0) AS is_voided FROM orders WHERE order_uid = ?",
            [order_uid],
        ).fetchone()
        if o is None:
            raise ValueError("Order not found.")
        if int(o["is_voided"] or 0) == 1:
            return 0

        # Aggregate received units per part_key from this order
        rows = con.execute(
            """
            SELECT part_key, SUM(COALESCE(units_received, 0)) AS qty
            FROM line_items
            WHERE order_uid = ?
            GROUP BY part_key
            HAVING SUM(COALESCE(units_received, 0)) > 0
            """,
            [order_uid],
        ).fetchall()

        vendor = safe_str(o["vendor"])
        order_label = safe_str(o["order_id"] or o["order_ref"] or "")
        file_hash = safe_str(o["file_hash"])
        reason = f"void_order vendor={vendor} order={order_label} uid={order_uid} hash={file_hash}".strip()

        n = 0
        for r in rows:
            part_key = safe_str(r["part_key"])
            qty = float(r["qty"] or 0)
            if not part_key or qty <= 0:
                continue

            removal_uid = str(uuid4())
            con.execute(
                """
                INSERT INTO parts_removed (removal_uid, part_key, qty_removed, ts_utc, project, note, updated_utc, order_uid, file_hash, reason)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [removal_uid, part_key, qty, ts, "order_void", reason, ts, order_uid, file_hash, "order_void"],
            )

            # Unified event log (qty negative for remove)
            con.execute(
                """
                INSERT INTO inventory_events (event_uid, ts_utc, event_type, part_key, qty, unit_cost, total_cost, project, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [str(uuid4()), ts, "order_void", part_key, -qty, None, None, "order_void", reason],
            )
            n += 1

        # Mark the order voided
        con.execute(
            """
            UPDATE orders
            SET is_voided = 1, voided_utc = ?, updated_utc = COALESCE(updated_utc, ?)
            WHERE order_uid = ?
            """,
            [ts, ts, order_uid],
        )
        # Optional: mark ingested_files too (does NOT affect duplicate stopper unless ingest code checks it)
        if file_hash:
            try:
                con.execute("UPDATE ingested_files SET is_voided = 1 WHERE file_hash = ?", [file_hash])
            except Exception:
                pass

        con.commit()

    return n


def _undo_void_order(db: DB, order_uid: str) -> int:
    """Undo a prior void: deletes the parts_removed rows created by _void_order_to_parts_removed."""
    ensure_orders_ingest_schema(db)
    ts = utc_now_iso()
    ensure_inventory_events_table(db)

    with db.connect() as con:
        con.execute("PRAGMA foreign_keys = ON;")

        o = con.execute(
            "SELECT order_uid, file_hash, COALESCE(is_voided,0) AS is_voided FROM orders WHERE order_uid = ?",
            [order_uid],
        ).fetchone()
        if o is None:
            raise ValueError("Order not found.")
        if int(o["is_voided"] or 0) == 0:
            return 0

        file_hash = safe_str(o["file_hash"])

        # Remove the removals we created (tagged by order_uid + reason)
        cur = con.execute(
            """
            DELETE FROM parts_removed
            WHERE order_uid = ?
              AND (reason = 'order_void' OR project = 'order_void')
            """,
            [order_uid],
        )
        removed = int(cur.rowcount or 0)

        # Also remove matching inventory_events (best-effort)
        try:
            con.execute(
                """
                DELETE FROM inventory_events
                WHERE event_type = 'order_void'
                  AND note LIKE ?
                """,
                [f"%uid={order_uid}%"],
            )
        except Exception:
            pass

        # Unmark order
        con.execute(
            """
            UPDATE orders
            SET is_voided = 0, voided_utc = NULL, updated_utc = COALESCE(updated_utc, ?)
            WHERE order_uid = ?
            """,
            [ts, order_uid],
        )
        if file_hash:
            try:
                con.execute("UPDATE ingested_files SET is_voided = 0 WHERE file_hash = ?", [file_hash])
            except Exception:
                pass

        con.commit()

    return removed


def _purge_order_and_rebuild(db: DB, order_uid: str) -> None:
    """Hard-delete the order + line items and clear its hash, then rebuild parts_received/inventory.

    This is the "I want to ingest again" path.
    """
    with db.connect() as con:
        con.execute("PRAGMA foreign_keys = ON;")
        row = con.execute("SELECT file_hash FROM orders WHERE order_uid = ?", [order_uid]).fetchone()
        file_hash = None if row is None else row[0]

        # If this order was voided, remove the void removals too (so inventory doesn't stay offset).
        try:
            con.execute(
                """
                DELETE FROM parts_removed
                WHERE order_uid = ?
                  AND (reason = 'order_void' OR project = 'order_void')
                """,
                [order_uid],
            )
        except Exception:
            pass

        con.execute("DELETE FROM line_items WHERE order_uid = ?", [order_uid])
        con.execute("DELETE FROM orders WHERE order_uid = ?", [order_uid])

        # remove file hash record if no orders remain for that hash
        if file_hash:
            remain = con.execute("SELECT COUNT(*) FROM orders WHERE file_hash = ?", [file_hash]).fetchone()[0]
            if int(remain) == 0:
                con.execute("DELETE FROM ingested_files WHERE file_hash = ?", [file_hash])

        _rebuild_parts_received_and_inventory(con)
        con.commit()


def _delete_order_and_rebuild(db: DB, order_uid: str) -> None:
    # Backwards-compat wrapper
    _purge_order_and_rebuild(db, order_uid)



def _rebuild_parts_received_and_inventory(con) -> None:
    from datetime import datetime, timezone

    ts = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # Rebuild parts_received from current line_items
    con.execute("DELETE FROM parts_received;")
    con.execute(
        """
        INSERT INTO parts_received(
            part_key, vendor, sku, description, desc_clean,
            label_line1, label_line2, label_short,
            purchase_url, airtable_url, label_qr_url, label_qr_text,
            units_received, total_spend, last_invoice, avg_unit_cost, updated_utc
        )
        SELECT
            part_key,
            MIN(vendor) AS vendor,
            MIN(sku) AS sku,
            MIN(description) AS description,
            MIN(desc_clean) AS desc_clean,
            MIN(label_line1) AS label_line1,
            MIN(label_line2) AS label_line2,
            MIN(label_short) AS label_short,
            MIN(purchase_url) AS purchase_url,
            MIN(airtable_url) AS airtable_url,
            MIN(label_qr_url) AS label_qr_url,
            MIN(label_qr_text) AS label_qr_text,
            SUM(COALESCE(units_received, 0)) AS units_received,
            SUM(COALESCE(line_total, 0)) AS total_spend,
            MAX(invoice) AS last_invoice,
            CASE WHEN SUM(COALESCE(units_received,0)) = 0 THEN NULL
                 ELSE SUM(COALESCE(line_total,0)) / SUM(COALESCE(units_received,0))
            END AS avg_unit_cost,
            ? AS updated_utc
        FROM line_items
        GROUP BY part_key
        """,
        [ts],
    )

    # Refresh materialized inventory snapshot from inventory_view
    con.execute("DELETE FROM inventory;")
    con.execute(
        """
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
        FROM inventory_view
        """,
        [ts],
    )

# ----------------------------
# Export (implemented)
# ----------------------------
def menu_export():
    db = get_db()
    if not db.path.exists():
        console.clear()
        header()
        console.print(f"[red]DB not found:[/red] {db.path}")
        pause()
        return

    while True:
        console.clear()
        header()
        console.print("[bold]Export[/bold]\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Export inventory_view (recommended)")
        menu.add_row("2.", "Export orders")
        menu.add_row("3.", "Export line_items")
        menu.add_row("4.", "Export parts_received")
        menu.add_row("5.", "Export parts_removed")
        menu.add_row("6.", "Export ingested_files")
        menu.add_row("7.", "Export ALL of the above")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=[str(i) for i in range(0, 8)], default="1")
        if choice == "0":
            return

        slug = timestamp_slug()
        outdir = exports_dir() / f"export_{slug}"
        outdir.mkdir(parents=True, exist_ok=True)

        try:
            if choice == "1":
                export_sqlite_object_to_csv(
                    db,
                    "inventory_view",
                    outdir / "inventory_view.csv",
                    order_by="vendor, sku",
                )
            elif choice == "2":
                export_sqlite_object_to_csv(
                    db, "orders", outdir / "orders.csv", order_by="vendor, order_date"
                )
            elif choice == "3":
                export_sqlite_object_to_csv(
                    db, "line_items", outdir / "line_items.csv", order_by="vendor, invoice, line_item_uid"
                )
            elif choice == "4":
                export_sqlite_object_to_csv(
                    db, "parts_received", outdir / "parts_received.csv", order_by="vendor, sku"
                )
            elif choice == "5":
                export_sqlite_object_to_csv(
                    db, "parts_removed", outdir / "parts_removed.csv", order_by="ts_utc DESC"
                )
            elif choice == "6":
                export_sqlite_object_to_csv(
                    db, "ingested_files", outdir / "ingested_files.csv", order_by="first_seen_utc DESC"
                )
            elif choice == "7":
                export_sqlite_object_to_csv(db, "inventory_view", outdir / "inventory_view.csv", order_by="vendor, sku")
                export_sqlite_object_to_csv(db, "orders", outdir / "orders.csv", order_by="vendor, order_date")
                export_sqlite_object_to_csv(db, "line_items", outdir / "line_items.csv", order_by="vendor, invoice, line_item_uid")
                export_sqlite_object_to_csv(db, "parts_received", outdir / "parts_received.csv", order_by="vendor, sku")
                export_sqlite_object_to_csv(db, "parts_removed", outdir / "parts_removed.csv", order_by="ts_utc DESC")
                export_sqlite_object_to_csv(db, "ingested_files", outdir / "ingested_files.csv", order_by="first_seen_utc DESC")

            console.print(f"\n[cyan]Export folder:[/cyan] {outdir}")
        except Exception as e:
            console.print(f"[red]Export failed:[/red] {e}")

        pause()

# ----------------------------
# Inventory (unchanged from our last version; kept here)
# ----------------------------
def menu_inventory():
    db = get_db()
    if not db.path.exists():
        console.clear()
        header()
        console.print(f"[red]DB not found:[/red] {db.path}")
        pause()
        return

    while True:
        console.clear()
        header()
        console.print("[bold]Inventory[/bold] (from [cyan]inventory_view[/cyan])\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "List (top 30)")
        menu.add_row("2.", "Search")
        menu.add_row("3.", "Show details (by part_key)")
        menu.add_row("4.", "Receive stock (manual)")
        menu.add_row("5.", "Remove stock (log usage)")
        menu.add_row("6.", "Edit label fields (line1/line2/short/QR/url)")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "3", "4", "5", "6", "0"], default="2")
        if choice == "0":
            return
        if choice == "1":
            inv_list(db)
        elif choice == "2":
            inv_search(db)
        elif choice == "3":
            inv_show(db)
        elif choice == "4":
            inv_receive(db)
        elif choice == "5":
            inv_remove(db)
        elif choice == "6":
            inv_edit_labels(db)

def inv_list(db: DB):
        inv_browse(db, title="Inventory (all)", order_by="vendor, sku")

def inv_browse(
    db: DB,
    where_sql: str | None = None,
    params: list | None = None,
    title: str = "Inventory browse",
    order_by: str = "vendor, sku",
    allow_select: bool = False,
) -> Any:
    """
    Paged browser for inventory_view.
    - where_sql: e.g. "WHERE vendor LIKE ? OR sku LIKE ?"
    - params: matching parameters for where_sql
    When allow_select=True, user can type: sel 87:200,205,206
    and this function returns a dict describing the selection context.
    """
    params = params or []
    page_sizes = [10, 25, 50, 100]
    page_size = 25
    page = 1

    base_where = f" {where_sql} " if where_sql else ""
    dyn_where = ""          # additional WHERE/AND clauses
    dyn_params: list = []   # params for dyn_where

    # Sticky filter state (Enter keeps; * clears a field)
    flt_vendor: str | None = None
    flt_term: str | None = None
    flt_min_hand: float | None = None
    flt_max_cost: float | None = None
    flt_inv: str | None = None

    def _combined_where() -> str:
        if not dyn_where.strip():
            return base_where
        if base_where.strip():
            # base_where is expected to include WHERE ...
            return base_where.rstrip() + " AND " + dyn_where.strip() + " "
        return " WHERE " + dyn_where.strip() + " "

    def total_rows() -> int:
        return int(
            db.scalar(
                f"SELECT COUNT(*) FROM inventory_view{_combined_where()}",
                params + dyn_params
            ) or 0
        )

    def fetch_page(p: int, ps: int):
        offset = (p - 1) * ps
        sql = f"""
            SELECT part_key, vendor, sku, label_short, on_hand, avg_unit_cost, last_invoice
            FROM inventory_view
            {_combined_where()}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """
        return db.rows(sql, params + dyn_params + [ps, offset])

    while True:
        console.clear()
        header()
        console.print(f"[bold]{title}[/bold]\n")

        total = total_rows()
        if total == 0:
            console.print("[yellow]No rows found.[/yellow]")
            pause()
            return None

        max_page = max(1, (total + page_size - 1) // page_size)
        page = max(1, min(page, max_page))

        rows = fetch_page(page, page_size)

        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("#", justify="right", style="dim", width=4)
        t.add_column("vendor", width=20)
        t.add_column("sku", width=16)
        t.add_column("label_short")
        t.add_column("on_hand", justify="right", width=8)
        t.add_column("avg_cost", justify="right", width=10)

        for i, r in enumerate(rows):
            row_num = (page - 1) * page_size + i + 1
            t.add_row(
                str(row_num),
                safe_str(r["vendor"]),
                safe_str(r["sku"]),
                shorten(r["label_short"], 60),
                safe_str(r["on_hand"]),
                fmt_money(r["avg_unit_cost"]),
            )

        console.print(t)

        cmd_line = (
            f"\nPage [cyan]{page}[/cyan] / [cyan]{max_page}[/cyan]  |  "
            f"Rows: [cyan]{total}[/cyan]  |  Page size: [cyan]{page_size}[/cyan]  |  "
            f"Sort: [cyan]{order_by}[/cyan]\n"
            "[dim]Commands:[/dim] "
            "[bold]n[/bold] next  [bold]p[/bold] prev  [bold]g[/bold] goto  "
            "[bold]s[/bold] size  "
            "[bold]v[/bold] vendor  [bold]h[/bold] on_hand  [bold]c[/bold] cost  [bold]o[/bold] last_invoice  "
            "[bold]f[/bold] filter  "
            "[bold]q[/bold] back  [bold]<row#>[/bold] details"
        )
        if allow_select:
            cmd_line += "  [bold]sel <spec>[/bold] select rows"
        console.print(cmd_line)

        cmd = Prompt.ask(">", default="n").strip()
        cmd_l = cmd.lower()

        if cmd_l == "q":
            return None

        elif cmd_l == "n":
            if page < max_page:
                page += 1

        elif cmd_l == "p":
            if page > 1:
                page -= 1

        elif cmd_l == "g":
            page = IntPrompt.ask("Go to page", default=page)

        elif cmd_l == "s":
            page_size = IntPrompt.ask(f"Page size {page_sizes}", default=page_size)
            if page_size not in page_sizes:
                page_size = min(page_sizes, key=lambda x: abs(x - page_size))
            page = 1

        # sort hotkeys
        elif cmd_l == "v":
            order_by = "vendor, sku"
            page = 1

        elif cmd_l == "h":
            order_by = "on_hand DESC, vendor, sku"
            page = 1

        elif cmd_l == "c":
            order_by = "avg_unit_cost DESC, vendor, sku"
            page = 1

        elif cmd_l == "o":
            order_by = "last_invoice DESC"
            page = 1

        # filters
        elif cmd_l == "f":
            console.print("\n[bold]Filter inventory[/bold]  [dim](Enter keeps current, '*' clears a field)[/dim]")

            # Read raw inputs
            vendor_in = Prompt.ask("Vendor contains", default=(flt_vendor or "")).strip()
            term_in = Prompt.ask("Search term (vendor/sku/desc/label)", default=(flt_term or "")).strip()
            min_in = Prompt.ask("Min on_hand", default=("" if flt_min_hand is None else str(flt_min_hand))).strip()
            max_in = Prompt.ask("Max avg_cost", default=("" if flt_max_cost is None else str(flt_max_cost))).strip()
            inv_in = Prompt.ask("Last invoice contains", default=(flt_inv or "")).strip()

            def _update_str(cur: str | None, new: str) -> str | None:
                if new == "":
                    return cur  # Enter keeps current
                if new == "*":
                    return None  # explicit clear
                return new

            flt_vendor = _update_str(flt_vendor, vendor_in)
            flt_term = _update_str(flt_term, term_in)
            flt_inv = _update_str(flt_inv, inv_in)

            # numbers: Enter keeps, * clears, otherwise parse
            if min_in != "":
                if min_in == "*":
                    flt_min_hand = None
                else:
                    try:
                        flt_min_hand = float(min_in)
                    except ValueError:
                        console.print("[yellow]Min on_hand ignored (not a number).[/yellow]")

            if max_in != "":
                if max_in == "*":
                    flt_max_cost = None
                else:
                    try:
                        flt_max_cost = float(max_in)
                    except ValueError:
                        console.print("[yellow]Max avg_cost ignored (not a number).[/yellow]")

            # Rebuild dyn_where from the sticky state
            clauses = []
            new_params: list = []

            if flt_vendor:
                clauses.append("vendor LIKE ? COLLATE NOCASE")
                new_params.append(f"%{flt_vendor}%")

            if flt_term:
                like = f"%{flt_term}%"
                search_cols = [
                    "vendor", "sku", "part_key", "description", "desc_clean",
                    "label_line1", "label_line2", "label_short",
                    "purchase_url", "last_invoice",
                ]
                clauses.append(
                    "(" + " OR ".join([f"COALESCE({c}, '') LIKE ? COLLATE NOCASE" for c in search_cols]) + ")")
                new_params.extend([like] * len(search_cols))

            if flt_min_hand is not None:
                clauses.append("on_hand >= ?")
                new_params.append(flt_min_hand)

            if flt_max_cost is not None:
                clauses.append("avg_unit_cost <= ?")
                new_params.append(flt_max_cost)

            if flt_inv:
                clauses.append("last_invoice LIKE ? COLLATE NOCASE")
                new_params.append(f"%{flt_inv}%")

            dyn_where = " AND ".join(clauses) if clauses else ""
            dyn_params = new_params
            page = 1


        # selection mode
        elif allow_select and cmd_l.startswith("sel"):
            spec = cmd[3:].strip()
            row_nums = parse_row_spec(spec)
            return {
                "row_nums": row_nums,
                "base_where": base_where,
                "base_params": params,
                "dyn_where": dyn_where,
                "dyn_params": dyn_params,
                "order_by": order_by,
            }

        # Drill-in by absolute row number
        elif cmd.isdigit():
            idx = int(cmd) - 1  # absolute row index (0-based)
            target_page = idx // page_size + 1
            target_offset = idx % page_size

            page = target_page
            rows = fetch_page(page, page_size)

            if 0 <= target_offset < len(rows):
                part_key = rows[target_offset]["part_key"]
                inv_show(db, part_key=part_key)
            continue

def inv_search(db: DB):
    console.clear()
    header()
    console.print("[bold]Search inventory[/bold]\n")

    term = Prompt.ask("Search (part_key / sku / description / label_short / vendor)", default="").strip()
    if not term:
        return

    like = f"%{term}%"
    where_sql = """
    WHERE (
          part_key LIKE ? COLLATE NOCASE
       OR sku LIKE ? COLLATE NOCASE
       OR vendor LIKE ? COLLATE NOCASE
       OR description LIKE ? COLLATE NOCASE
       OR label_short LIKE ? COLLATE NOCASE
    )
    """

    inv_browse(
        db,
        where_sql=where_sql,
        params=[like, like, like, like, like],
        title=f"Search: {term}",
        order_by="on_hand DESC, vendor, sku",
    )

def inv_show(db: DB, part_key: str | None = None):
    console.clear()
    header()
    console.print("[bold]Show inventory item[/bold]\n")

    if not part_key:
        part_key = Prompt.ask("part_key (e.g. mcmaster:1234K56)", default="").strip()
    if not part_key:
        return

    rows = db.rows("SELECT * FROM inventory_view WHERE part_key = ?", [part_key])
    if not rows:
        console.print("[yellow]No item found in inventory_view.[/yellow]")
        pause()
        return

    r = rows[0]
    t = Table(show_header=False, box=None)
    for k in r.keys():
        t.add_row(f"[dim]{k}[/dim]", safe_str(r[k]))
    console.print(t)

    # Recent audit notes (if available)
    try:
        ev = db.rows(
            "SELECT ts_utc, event_type, qty, unit_cost, project, note "
            "FROM inventory_events WHERE part_key = ? ORDER BY ts_utc DESC LIMIT 10",
            [part_key],
        )
        if ev:
            console.print("\n[bold]Recent events[/bold]")
            et = Table(show_header=True, header_style="bold cyan")
            et.add_column("ts_utc", width=20)
            et.add_column("type", width=8)
            et.add_column("qty", justify="right", width=8)
            et.add_column("unit_cost", justify="right", width=10)
            et.add_column("project", width=16)
            et.add_column("note")
            for e in ev:
                et.add_row(
                    safe_str(e["ts_utc"]),
                    safe_str(e["event_type"]),
                    safe_str(e["qty"]),
                    fmt_money(e["unit_cost"]),
                    shorten(safe_str(e["project"]), 16),
                    shorten(safe_str(e["note"]), 60),
                )
            console.print(et)
    except Exception:
        pass

    pause()

def pick_part_keys_from_browser(db: DB, title: str) -> list[str]:
    console.clear()
    header()
    console.print(f"[bold]{title}[/bold]\n")
    console.print("[dim]Use sort keys (v/h/c/o), filter (f), type a row # for details, then select: sel 205 or sel 87:200,205[/dim]\n")
    sel = inv_browse(db, title=title, allow_select=True)
    if not sel:
        return []
    return _fetch_selected_part_keys(db, sel)

def inv_remove(db: DB):
    console.clear()
    header()
    console.print("[bold]Remove stock[/bold] (logs to parts_removed)\n")

    use_browser = Confirm.ask("Pick part(s) from inventory browser?", default=True)
    if use_browser:
        part_keys = pick_part_keys_from_browser(db, "Remove parts")
        if not part_keys:
            return
    else:
        pk = Prompt.ask("part_key").strip()
        if not pk:
            return
        part_keys = [pk]

    qty = FloatPrompt.ask("Qty removed (applies to each selected item)", default=1.0)
    if qty <= 0:
        console.print("[yellow]Qty must be > 0[/yellow]")
        pause()
        return

    project = Prompt.ask("Project (optional)", default="").strip()
    note = Prompt.ask("Note (why?) (optional)", default="").strip()

    ts = utc_now_iso()
    ensure_inventory_events_table(db)

    skipped = 0
    for part_key in part_keys:
        exists = db.scalar("SELECT 1 FROM parts_received WHERE part_key = ? LIMIT 1", [part_key])
        if not exists:
            skipped += 1
            continue

        removal_uid = str(uuid4())
        db.execute(
            """
            INSERT INTO parts_removed (removal_uid, part_key, qty_removed, ts_utc, project, note, updated_utc)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [removal_uid, part_key, qty, ts, project, note, ts],
        )

        # Unified event log (qty negative for remove)
        db.execute(
            """
            INSERT INTO inventory_events (event_uid, ts_utc, event_type, part_key, qty, unit_cost, total_cost, project, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [str(uuid4()), ts, "remove", part_key, -qty, None, None, project, note],
        )

    if skipped:
        console.print(f"[yellow]Skipped {skipped} item(s) not found in parts_received.[/yellow]")

    console.print("[green]Logged removal.[/green] inventory_view on_hand will update automatically.")
    pause()

def inv_receive(db: DB):
    console.clear()
    header()
    console.print("[bold]Receive stock (manual)[/bold] (upserts parts_received)\n")

    use_browser = Confirm.ask("Pick existing part(s) from inventory browser?", default=True)
    if use_browser:
        part_keys = pick_part_keys_from_browser(db, "Receive parts")
        if not part_keys:
            return
    else:
        pk = Prompt.ask("part_key (recommended format: vendor:sku)").strip()
        if not pk:
            return
        part_keys = [pk]

    qty = FloatPrompt.ask("Qty received (applies to each selected item)", default=1.0)
    if qty <= 0:
        console.print("[yellow]Qty must be > 0[/yellow]")
        pause()
        return

    unit_cost = Prompt.ask("Unit cost (optional)", default="").strip()
    try:
        unit_cost_f = float(unit_cost) if unit_cost else 0.0
    except ValueError:
        unit_cost_f = 0.0

    project = Prompt.ask("Project (optional)", default="").strip()
    note = Prompt.ask("Note (why?) (optional)", default="").strip()

    added_spend_each = qty * unit_cost_f
    ts = utc_now_iso()
    ensure_inventory_events_table(db)

    for part_key in part_keys:
        exists = db.scalar("SELECT 1 FROM parts_received WHERE part_key = ? LIMIT 1", [part_key])

        if exists:
            db.execute(
                """
                UPDATE parts_received
                SET
                  units_received = COALESCE(units_received, 0) + ?,
                  total_spend = COALESCE(total_spend, 0) + ?,
                  avg_unit_cost =
                    CASE
                      WHEN (COALESCE(units_received, 0) + ?) > 0 AND (COALESCE(total_spend, 0) + ?) > 0
                      THEN (COALESCE(total_spend, 0) + ?) / (COALESCE(units_received, 0) + ?)
                      ELSE avg_unit_cost
                    END,
                  updated_utc = ?
                WHERE part_key = ?
                """,
                [qty, added_spend_each, qty, added_spend_each, added_spend_each, qty, ts, part_key],
            )

            db.execute(
                """
                INSERT INTO inventory_events (event_uid, ts_utc, event_type, part_key, qty, unit_cost, total_cost, project, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [str(uuid4()), ts, "receive", part_key, qty, unit_cost_f or None, added_spend_each or None, project, note],
            )
            continue

        if use_browser:
            console.print(f"[yellow]Skipping part_key not found in parts_received:[/yellow] {part_key}")
            continue

        console.print("\n[bold]New part_key[/bold] — enter basic metadata (you can refine later).")
        vendor = Prompt.ask("vendor", default=part_key.split(":", 1)[0] if ":" in part_key else "")
        sku = Prompt.ask("sku", default=part_key.split(":", 1)[1] if ":" in part_key else "")
        description = Prompt.ask("description", default="")
        label_short = Prompt.ask("label_short", default=description or part_key)

        label_line1 = Prompt.ask("label_line1 (optional)", default="")
        label_line2 = Prompt.ask("label_line2 (optional)", default="")
        purchase_url = Prompt.ask("purchase_url (optional)", default="")
        airtable_url = Prompt.ask("airtable_url (optional)", default="")
        label_qr_url = Prompt.ask("label_qr_url (optional)", default="")
        label_qr_text = Prompt.ask("label_qr_text (optional)", default="")

        desc_clean = description.strip()
        avg_unit_cost = (added_spend_each / qty) if (qty > 0 and added_spend_each > 0) else 0.0

        db.execute(
            """
            INSERT INTO parts_received (
                part_key, vendor, sku, description, desc_clean,
                label_line1, label_line2, label_short,
                purchase_url, airtable_url, label_qr_url, label_qr_text,
                units_received, total_spend, last_invoice, avg_unit_cost, updated_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                part_key, vendor, sku, description, desc_clean,
                label_line1, label_line2, label_short,
                purchase_url, airtable_url, label_qr_url, label_qr_text,
                qty, added_spend_each, None, avg_unit_cost, ts,
            ],
        )

        db.execute(
            """
            INSERT INTO inventory_events (event_uid, ts_utc, event_type, part_key, qty, unit_cost, total_cost, project, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [str(uuid4()), ts, "receive", part_key, qty, unit_cost_f or None, added_spend_each or None, project, note],
        )

    console.print("[green]Receive complete.[/green]")
    pause()

def inv_edit_labels(db: DB):
    console.clear()
    header()
    console.print("[bold]Edit label fields[/bold] (updates parts_received)\n")

    part_key = Prompt.ask("part_key").strip()
    if not part_key:
        return

    rows = db.rows("SELECT * FROM parts_received WHERE part_key = ?", [part_key])
    if not rows:
        console.print("[yellow]No row found in parts_received for that part_key.[/yellow]")
        pause()
        return

    r = rows[0]
    console.print("[dim]Edit values; leave as-is to keep current.[/dim]\n")

    def ask_keep(field: str) -> str:
        cur = safe_str(r[field])
        return Prompt.ask(field, default=cur).strip()

    label_line1 = ask_keep("label_line1")
    label_line2 = ask_keep("label_line2")
    label_short = ask_keep("label_short")
    purchase_url = ask_keep("purchase_url")
    airtable_url = ask_keep("airtable_url")
    label_qr_url = ask_keep("label_qr_url")
    label_qr_text = ask_keep("label_qr_text")

    ts = utc_now_iso()
    db.execute("""
        UPDATE parts_received
        SET
          label_line1 = ?,
          label_line2 = ?,
          label_short = ?,
          purchase_url = ?,
          airtable_url = ?,
          label_qr_url = ?,
          label_qr_text = ?,
          updated_utc = ?
        WHERE part_key = ?
    """, [label_line1, label_line2, label_short, purchase_url, airtable_url, label_qr_url, label_qr_text, ts, part_key])

    console.print("[green]Updated label fields.[/green]")
    pause()


# ----------------------------
# DB diagnostics
# ----------------------------


def menu_db_diagnostics():
    db_path = default_db_path()

    def _show_header() -> None:
        console.clear()
        header()
        console.print("[bold]DB diagnostics[/bold]\n")
        console.print(f"DB path: [cyan]{db_path}[/cyan]")
        console.print(f"DB exists: {'✅' if db_path.exists() else '❌'}\n")

    # If the DB doesn't exist yet, offer to create it.
    if not db_path.exists():
        _show_header()
        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Create empty database (init schema)")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "0"], default="0")
        if choice == "1":
            ok = Confirm.ask("Create a new empty database now?", default=True)
            if ok:
                from studio_inventory.main import init_inventory_db
                init_inventory_db(db_path)
                db = get_db()
                ensure_inventory_events_table(db)
                console.print("[green]Database created.[/green]")
                pause()
        return

    db = get_db()

    while True:
        _show_header()

        tables = db.rows("""
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('table','view')
              AND name NOT LIKE 'sqlite_%'
            ORDER BY type, name
        """)

        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("type", width=6)
        t.add_column("name")
        t.add_column("rows", justify="right", width=8)

        for row in tables:
            name = row["name"]
            typ = row["type"]
            count = ""
            if typ == "table":
                try:
                    count = str(db.scalar(f'SELECT COUNT(*) FROM "{name}"') or 0)
                except Exception:
                    count = "?"
            t.add_row(typ, name, count)

        console.print(t)

        console.print("\n")
        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Reset database contents (truncate tables; keep schema)")
        menu.add_row("2.", "Hard reset database file (delete DB; recreate schema)")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "0"], default="0")
        if choice == "0":
            return

        if choice == "1":
            console.print("\n[red][bold]DANGER[/bold][/red] This will permanently delete ALL data in your DB (schema stays).")
            ok = Confirm.ask("Continue?", default=False)
            if not ok:
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue
            token = Prompt.ask("Type RESET to confirm", default="")
            if token.strip() != "RESET":
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue

            try:
                _reset_database_contents(db)
                ensure_inventory_events_table(db)
                console.print("[green]Database cleared.[/green]")
            except Exception as e:
                console.print(f"[red]Reset failed:[/red] {e}")
            pause()
            continue

        if choice == "2":
            console.print("\n[red][bold]DANGER[/bold][/red] This will DELETE the SQLite file on disk and recreate an empty schema.")
            ok = Confirm.ask("Continue?", default=False)
            if not ok:
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue
            token = Prompt.ask("Type RESET to confirm", default="")
            if token.strip() != "RESET":
                console.print("[yellow]Cancelled.[/yellow]")
                pause()
                continue

            try:
                # Close any open connections by dropping the DB wrapper and deleting files.
                db = None  # type: ignore

                # Remove main DB and sidecar WAL/SHM files (best effort).
                for p in [db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")]:
                    try:
                        if p.exists():
                            p.unlink()
                    except Exception:
                        pass

                from studio_inventory.main import init_inventory_db
                init_inventory_db(db_path)

                db = get_db()
                ensure_inventory_events_table(db)

                console.print("[green]Database recreated.[/green]")
            except Exception as e:
                console.print(f"[red]Hard reset failed:[/red] {e}")
            pause()



def _reset_database_contents(db: DB) -> None:
    """Delete all rows from all user tables (keeps schema).

    Notes:
      - This does NOT drop tables; it truncates them.
      - Also clears AUTOINCREMENT counters (sqlite_sequence) when present.
    """
    # First pass: delete rows with FK checks disabled (best effort), then restore FK checks.
    with db.connect() as con:
        try:
            con.execute("PRAGMA foreign_keys = OFF;")

            tables = con.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name NOT LIKE 'sqlite_%'
                """
            ).fetchall()

            # deterministic order helps with debugging
            names = sorted([r[0] for r in tables])
            for name in names:
                con.execute(f'DELETE FROM "{name}";')

            # Reset AUTOINCREMENT sequences if the internal table exists
            try:
                con.execute("DELETE FROM sqlite_sequence;")
            except Exception:
                pass

            con.commit()
        finally:
            # Always attempt to re-enable FK checks for this connection
            try:
                con.execute("PRAGMA foreign_keys = ON;")
            except Exception:
                pass

    # Optional: compact the DB file after large deletes (best effort)
    try:
        with db.connect() as con2:
            con2.execute("VACUUM;")
    except Exception:
        pass

# ----------------------------
# Future stubs

# ----------------------------
def menu_vendors():
    console.clear()
    header()
    console.print("[bold]Vendors[/bold]\n")
    console.print("Next: DigiKey OAuth + product/media enrichment, then McMaster cert-based API enrichment.")
    pause()

# ----------------------------
# Labels
# ----------------------------

LABEL_SOURCES = [
    ("label_line1", "Label line 1"),
    ("label_line2", "Label line 2"),
    ("label_short", "Label short"),
    ("vendor", "Vendor"),
    ("sku", "SKU"),
    ("vendor_sku", "Vendor:SKU"),
    ("purchase_url", "Purchase URL"),
    ("label_qr_text", "Label QR text"),
    ("part_key", "Part key"),
]

ANCHORS = ["UL","UC","UR","ML","MC","MR","LL","LC","LR"]
ALIGNS = ["left","center","right"]
STYLES = ["normal","bold","italic"]


def _open_pdf(path: Path) -> None:
    try:
        if sys.platform.startswith("darwin"):
            subprocess.run(["open", str(path)], check=False)
        elif os.name == "nt":
            os.startfile(str(path))  # type: ignore[attr-defined]
        else:
            subprocess.run(["xdg-open", str(path)], check=False)
    except Exception:
        pass

def list_label_templates() -> list[Path]:
    d = project_root() / "label_templates"
    if not d.exists():
        return []
    return sorted(d.glob("*.json"))

def pick_label_template() -> Path | None:
    templates = list_label_templates()
    if not templates:
        console.print("[red]No templates found in label_templates/*.json[/red]")
        pause()
        return None

    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("#", justify="right", width=3)
    t.add_column("template")
    t.add_column("label size", justify="right")
    t.add_column("grid", justify="right")

    for i, p in enumerate(templates, 1):
        try:
            tpl = LabelTemplate.from_json(p)
            size = f'{tpl.label_w/72:.2f}"×{tpl.label_h/72:.2f}"'
            grid = f"{tpl.cols}×{tpl.rows}"
            name = tpl.name
        except Exception:
            name = p.name
            size = ""
            grid = ""
        t.add_row(str(i), name + f" [dim]({p.name})[/dim]", size, grid)

    console.print("[bold]Choose label template[/bold]\n")
    console.print(t)
    choice = IntPrompt.ask("Template #", default=1)
    if not (1 <= choice <= len(templates)):
        console.print("[red]Invalid choice[/red]")
        pause()
        return None
    return templates[choice - 1]

def _combine_where(base_where: str, dyn_where: str) -> str:
    base_where = base_where or ""
    dyn_where = dyn_where or ""
    if not dyn_where.strip():
        return base_where
    if base_where.strip():
        return base_where.rstrip() + " AND " + dyn_where.strip() + " "
    return " WHERE " + dyn_where.strip() + " "

def _fetch_selected_part_keys(db: DB, sel: dict) -> list[str]:
    row_nums: list[int] = sel.get("row_nums", []) or []
    if not row_nums:
        return []
    base_where = sel.get("base_where", "") or ""
    dyn_where = sel.get("dyn_where", "") or ""
    order_by = sel.get("order_by", "vendor, sku") or "vendor, sku"
    base_params = sel.get("base_params", []) or []
    dyn_params = sel.get("dyn_params", []) or []

    where = _combine_where(base_where, dyn_where)
    max_n = max(row_nums)
    key_rows = db.rows(f"""
        SELECT part_key
        FROM inventory_view
        {where}
        ORDER BY {order_by}
        LIMIT ?
    """, list(base_params) + list(dyn_params) + [max_n])

    part_keys: list[str] = []
    for n in row_nums:
        if 1 <= n <= len(key_rows):
            part_keys.append(key_rows[n - 1]["part_key"])
    return part_keys

def _fetch_label_rows(db: DB, part_keys: list[str]) -> list[dict]:
    if not part_keys:
        return []
    qmarks = ",".join(["?"] * len(part_keys))
    got = db.rows(f"""
        SELECT
            part_key, vendor, sku,
            label_line1, label_line2, label_short,
            purchase_url, label_qr_text
        FROM parts_received
        WHERE part_key IN ({qmarks})
    """, part_keys)
    by_key = {r["part_key"]: dict(r) for r in got}
    return [by_key[k] for k in part_keys if k in by_key]

def _default_layout_for_template(tpl_path: Path) -> dict:
    try:
        t = LabelTemplate.from_json(tpl_path)
        base_size = int(t.font_size)
    except Exception:
        base_size = 8

    return {
        "elements": [
            {"source": "label_line1", "style": "bold", "size": base_size, "pos": "UL", "align": "left", "wrap": False, "max_lines": 1},
            {"source": "vendor_sku", "style": "normal", "size": base_size, "pos": "LL", "align": "left", "wrap": False, "max_lines": 1},
        ],
        "qr": {"enabled": True, "source": "purchase_url", "pos": "UR", "span": 1, "span_y": 1, "fit": False, "pad_rel": 0.06, "size_rel": 0.45},
    }

def _pick_or_create_layout(tpl_path: Path) -> tuple[dict, str | None]:
    presets = list_label_presets(project_root(), tpl_path)
    console.print("\n[bold]Layout preset[/bold]")
    if presets:
        t = Table(show_header=True, header_style="bold magenta")
        t.add_column("#", justify="right", width=3)
        t.add_column("preset")
        for i, p in enumerate(presets, 1):
            t.add_row(str(i), p.stem)
        console.print(t)
        console.print("[dim]0 = start from default layout[/dim]")
        idx = IntPrompt.ask("Preset #", default=0)
        if idx == 0:
            return _default_layout_for_template(tpl_path), None
        if 1 <= idx <= len(presets):
            p = presets[idx - 1]
            try:
                return load_label_preset(p), p.stem
            except Exception:
                return _default_layout_for_template(tpl_path), None
        return _default_layout_for_template(tpl_path), None
    else:
        console.print("[dim]No presets yet. Starting from default.[/dim]")
        return _default_layout_for_template(tpl_path), None

def _edit_elements(layout: dict, tpl_font_size: int) -> None:
    console.print("\n[bold]Available elements[/bold]")
    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("#", justify="right", width=3)
    t.add_column("source")
    t.add_column("meaning")
    for i, (k, label) in enumerate(LABEL_SOURCES, 1):
        t.add_row(str(i), k, label)
    console.print(t)

    current = layout.get("elements", []) or []
    default_raw = ",".join([str(e.get("source","")).strip() for e in current if str(e.get("source","")).strip()])
    if not default_raw:
        default_raw = "label_line1,vendor_sku"

    raw = Prompt.ask(
        "Choose elements in order (comma list of # or names)",
        default=default_raw
    ).strip()

    if not raw:
        return

    # parse selection
    chosen: list[str] = []
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    for p in parts:
        if p.isdigit():
            i = int(p)
            if 1 <= i <= len(LABEL_SOURCES):
                chosen.append(LABEL_SOURCES[i - 1][0])
        else:
            chosen.append(p)

    # defaults from existing layout (by source)
    by_source: dict[str, dict] = {}
    for e in current:
        s = str(e.get("source","")).strip()
        if s and s not in by_source:
            by_source[s] = e

    elems: list[dict] = []

    for idx, src in enumerate(chosen, 1):
        # try to inherit defaults from existing element
        existing = by_source.get(src)
        if existing is None and idx - 1 < len(current):
            existing = current[idx - 1]

        d_style = str((existing or {}).get("style", "bold" if idx == 1 else "normal"))
        d_size = int((existing or {}).get("size", tpl_font_size))
        d_pos = str((existing or {}).get("pos", "UL" if idx == 1 else "LL"))
        d_align = str((existing or {}).get("align", "left"))
        d_wrap = bool((existing or {}).get("wrap", False))
        d_lines = int((existing or {}).get("max_lines", 2 if d_wrap else 1))
        d_lines = max(1, d_lines)

        d_span = int((existing or {}).get("span", 1))
        d_span = max(1, min(3, d_span))

        console.print(f"\n[bold]Element {idx}[/bold] source=[cyan]{src}[/cyan]")
        style = Prompt.ask("Style", choices=STYLES, default=d_style)
        size = IntPrompt.ask("Font size", default=d_size)
        pos = Prompt.ask("Position", choices=ANCHORS, default=d_pos)
        align = Prompt.ask("Justification", choices=ALIGNS, default=d_align)
        span = IntPrompt.ask("Span columns (1–3)", default=d_span)
        span = max(1, min(3, int(span)))
        wrap = Confirm.ask("Wrap text?", default=d_wrap)
        max_lines = IntPrompt.ask("Max lines", default=(d_lines if wrap else 1))
        max_lines = max(1, int(max_lines))

        elems.append({
            "source": src,
            "style": style,
            "size": int(size),
            "pos": pos,
            "align": align,
            "span": int(span),
            "wrap": bool(wrap),
            "max_lines": int(max_lines),
        })

    layout["elements"] = elems

def _edit_qr(layout: dict) -> None:
    qr_cfg = dict(layout.get("qr", {}) or {})

    enabled = Confirm.ask("QR enabled?", default=bool(qr_cfg.get("enabled", True)))
    qr_cfg["enabled"] = enabled
    if not enabled:
        layout["qr"] = qr_cfg
        return

    # pick source (allow # or name)
    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("#", justify="right", width=3)
    t.add_column("source")
    for i, (k, _) in enumerate(LABEL_SOURCES, 1):
        t.add_row(str(i), k)
    console.print("[bold]QR source[/bold]")
    console.print(t)

    src_raw = Prompt.ask("QR source (# or name)", default=str(qr_cfg.get("source", "purchase_url"))).strip() or "purchase_url"
    if src_raw.isdigit():
        i = int(src_raw)
        if 1 <= i <= len(LABEL_SOURCES):
            src = LABEL_SOURCES[i - 1][0]
        else:
            src = "purchase_url"
    else:
        src = src_raw

    pos = Prompt.ask("QR position", choices=ANCHORS, default=str(qr_cfg.get("pos", "UR")))

    # span across the template grid (X = columns, Y = rows)
    span = IntPrompt.ask("QR span columns (1–3)", default=int(qr_cfg.get("span", 1)))
    span = max(1, min(3, int(span)))

    span_y = IntPrompt.ask("QR span rows (1–3)", default=int(qr_cfg.get("span_y", 1)))
    span_y = max(1, min(3, int(span_y)))

    fit = Confirm.ask("Fit QR to spanned cell?", default=bool(qr_cfg.get("fit", False)))

    qr_cfg.update({"source": src, "pos": pos, "span": span, "span_y": span_y, "fit": fit})

    if fit:
        pad_rel = FloatPrompt.ask("QR padding (fraction, 0.00–0.15)", default=float(qr_cfg.get("pad_rel", 0.06)))
        pad_rel = max(0.0, min(0.15, float(pad_rel)))
        qr_cfg["pad_rel"] = pad_rel
        # keep size_rel as fallback if fit is turned off later
        if "size_rel" not in qr_cfg:
            qr_cfg["size_rel"] = 0.45
    else:
        size_rel = FloatPrompt.ask("QR size (0.2–0.9)", default=float(qr_cfg.get("size_rel", 0.45)))
        size_rel = max(0.2, min(0.9, float(size_rel)))
        qr_cfg["size_rel"] = size_rel

    layout["qr"] = qr_cfg

def _layout_summary(layout: dict) -> None:
    elems = layout.get("elements", []) or []
    qr_cfg = layout.get("qr", {}) or {}
    console.print("\n[bold]Current layout[/bold]")
    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("#", justify="right", width=3)
    t.add_column("source")
    t.add_column("style")
    t.add_column("size", justify="right")
    t.add_column("pos")
    t.add_column("span", justify="right")
    t.add_column("align")
    t.add_column("wrap")
    t.add_column("lines", justify="right")
    for i, e in enumerate(elems, 1):
        t.add_row(
            str(i),
            str(e.get("source","")),
            str(e.get("style","normal")),
            str(e.get("size","")),
            str(e.get("pos","UL")),
            str(e.get("span", 1)),
            str(e.get("align","left")),
            "yes" if row_get(e, "wrap") else "no",
            str(e.get("max_lines", 1)),
        )
    console.print(t)
    console.print(
        f"[dim]QR: enabled={qr_cfg.get('enabled', False)} "
        f"source={qr_cfg.get('source','')} pos={qr_cfg.get('pos','')} "
        f"span={qr_cfg.get('span', 1)} span_y={qr_cfg.get('span_y', 1)} "
        f"fit={qr_cfg.get('fit', False)} "
        f"pad_rel={qr_cfg.get('pad_rel','')} size_rel={qr_cfg.get('size_rel','')}[/dim]"
    )

def labels_generate(db: DB):
    console.clear()
    header()
    console.print("[bold]Labels → Generate PDF[/bold]\n")

    tpl_path = pick_label_template()
    if not tpl_path:
        return

    # select items
    console.print("[bold]Select items from inventory[/bold]")
    console.print("[dim]Use sort keys (v/h/c/o), filter (f), then select: sel 87:200,205,206[/dim]\n")
    sel = inv_browse(db, title="Inventory (select rows for labels)", allow_select=True)
    if not sel:
        return

    part_keys = _fetch_selected_part_keys(db, sel)
    if not part_keys:
        console.print("[yellow]No valid rows selected.[/yellow]")
        pause()
        return

    rows = _fetch_label_rows(db, part_keys)
    if not rows:
        console.print("[yellow]No label data found for selected items.[/yellow]")
        pause()
        return

    # layout preset
    layout, loaded_name = _pick_or_create_layout(tpl_path)

    # template font size for defaults
    try:
        tpl_obj = LabelTemplate.from_json(tpl_path)
        tpl_font_size = int(tpl_obj.font_size)
        per_sheet = int(tpl_obj.cols * tpl_obj.rows)
    except Exception:
        tpl_font_size = 8
        per_sheet = 1

    used = 0

    while True:
        console.clear()
        header()
        console.print("[bold]Labels → Layout & Preview[/bold]")
        console.print(f"[dim]Template:[/dim] {tpl_path.name}  |  [dim]Selected:[/dim] {len(rows)}  |  [dim]Used labels on sheet:[/dim] {used} / {per_sheet}")
        if loaded_name:
            console.print(f"[dim]Preset:[/dim] {loaded_name}")
        _layout_summary(layout)

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Edit elements")
        menu.add_row("2.", "Edit QR")
        menu.add_row("3.", "Set used labels on sheet")
        menu.add_row("4.", "Preview (opens PDF)")
        menu.add_row("5.", "Save preset")
        menu.add_row("6.", "Export final PDF")
        menu.add_row("0.", "Back")
        console.print("\n", menu)

        choice = Prompt.ask("Choose", choices=["1","2","3","4","5","6","0"], default="4")
        if choice == "0":
            return
        elif choice == "1":
            _edit_elements(layout, tpl_font_size)
        elif choice == "2":
            _edit_qr(layout)
        elif choice == "3":
            used = IntPrompt.ask(f"How many labels are already used on this sheet? (0–{max(0, per_sheet-1)})", default=used)
            used = max(0, min(max(0, per_sheet-1), int(used)))
        elif choice == "5":
            name = Prompt.ask("Preset name", default=(loaded_name or "my_layout")).strip()
            try:
                p = save_label_preset(project_root(), tpl_path, name, layout)
                loaded_name = p.stem
                console.print(f"[green]Saved preset:[/green] {p}")
            except Exception as e:
                console.print(f"[red]Failed to save preset:[/red] {e}")
            pause()
        elif choice == "4":
            preview_path = exports_dir() / "_labels_preview.pdf"
            make_labels_pdf(
                template_path=tpl_path,
                out_pdf=preview_path,
                rows=rows,
                start_pos=used + 1,
                include_qr=False,
                layout=layout,
                draw_boxes=False,
            )
            _open_pdf(preview_path)
            pause()
        elif choice == "6":
            default_name = f"labels_{timestamp_slug()}.pdf"
            name = Prompt.ask("Export filename", default=default_name).strip()
            if not name.lower().endswith(".pdf"):
                name += ".pdf"
            out_pdf = exports_dir() / name
            make_labels_pdf(
                template_path=tpl_path,
                out_pdf=out_pdf,
                rows=rows,
                start_pos=used + 1,
                include_qr=False,
                layout=layout,
                draw_boxes=False,
            )
            _open_pdf(out_pdf)
            console.print(f"[green]Exported:[/green] {out_pdf}")
            pause()

def menu_labels():
    db = get_db()
    while True:
        console.clear()
        header()
        console.print("[bold]Labels[/bold]\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Generate labels PDF (with preview)")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "0"], default="1")
        if choice == "0":
            return
        if choice == "1":
            labels_generate(db)

@app.command()
def inventory():
    """Interactive inventory browser (browse/search/receive/remove/labels fields)."""
    ensure_workspace()
    menu_inventory()

@app.command()
def labels():
    """Interactive label generator (PDF)."""
    ensure_workspace()
    menu_labels()

@app.command()
def diagnostics():
    """Database diagnostics (schema checks, quick stats)."""
    ensure_workspace()
    menu_db_diagnostics()


# ----------------------------
# Subcommands
# ----------------------------


@app.command()
def ingest(
    workspace: Optional[Path] = typer.Option(
        None,
        "--workspace",
        "-w",
        help="Workspace root (defaults to ~/StudioInventory or STUDIO_INV_HOME).",
    )
):
    """Ingest receipts / packing lists (interactive)."""
    if workspace:
        os.environ["STUDIO_INV_HOME"] = str(Path(workspace).expanduser().resolve())
    ensure_workspace()
    run_ingest()


@app.command()
def export(
    object_name: Optional[str] = typer.Option(
        None,
        "--object",
        "-o",
        help="SQLite table/view name to export as CSV (e.g. parts, orders, line_items).",
    ),
    out: Optional[Path] = typer.Option(
        None,
        "--out",
        "-O",
        help="Output CSV path. Default: <workspace>/exports/<object>_<timestamp>.csv",
    ),
    list_objects: bool = typer.Option(
        False,
        "--list",
        help="List SQLite tables/views, then exit.",
    ),
    db_path: Optional[Path] = typer.Option(
        None,
        "--db",
        help="Path to SQLite database. Default: <workspace>/studio_inventory.sqlite",
    ),
):
    """Export a table/view to CSV (non-interactive)."""
    ensure_workspace()
    db = get_db(db_path)

    if list_objects:
        objs = db.rows(
            """
            SELECT name, type
            FROM sqlite_master
            WHERE type IN ('table','view')
              AND name NOT LIKE 'sqlite_%'
            ORDER BY type, name
            """
        )
        t = Table(title="SQLite objects")
        t.add_column("Type", style="dim")
        t.add_column("Name")
        for r in objs:
            t.add_row(str(r["type"]), str(r["name"]))
        console.print(t)
        return

    if not object_name:
        console.print("[red]Missing --object.[/red] Try: studio-inventory export --list")
        raise typer.Exit(code=2)

    out_path = (
        Path(out).expanduser().resolve()
        if out
        else exports_dir() / f"{object_name}_{timestamp_slug()}.csv"
    )

    export_sqlite_object_to_csv(db, object_name, out_path)

@app.command()
def init():
    """
    Initialize the StudioInventory workspace (DB + folders + default label templates).
    """
    ensure_workspace()
    root = workspace_root()
    console.print(f"📁 Location: {root}")

    # Seed packaged templates into the user workspace on first run (pipx/wheel safe)
    dst = label_templates_dir()
    copied = 0
    try:
        from importlib import resources

        pkg_dir = resources.files("studio_inventory") / "label_templates"
        if pkg_dir.is_dir():
            for src in pkg_dir.iterdir():
                if src.is_file() and src.name.endswith(".json"):
                    out = dst / src.name
                    if not out.exists():
                        with resources.as_file(src) as src_path:
                            out.write_bytes(src_path.read_bytes())
                        copied += 1
    except Exception:
        copied = 0

    # Keep your dim styling in the Rich console output
    if copied:
        console.print(f"[dim]Seeded {copied} default label template(s).[/dim]")

    # Keep your existing Typer echo UX (same lines)
    typer.echo("✅ StudioInventory workspace initialized")
    typer.echo(f"📁 Location: {root}")
    if copied:
        typer.echo(f"🏷️  Seeded {copied} default label templates into label_templates/")
    typer.echo("")
    typer.echo("Created (if missing):")
    typer.echo("  - receipts/")
    typer.echo("  - imports/")
    typer.echo("  - exports/")
    typer.echo("  - log/")
    typer.echo("  - duplicates/")
    typer.echo("  - label_templates/")
    typer.echo("  - label_presets/")
    typer.echo("  - secrets/")


