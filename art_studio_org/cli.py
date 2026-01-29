from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence, Any
from uuid import uuid4

import csv
import json
import os
import subprocess
import sys

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, FloatPrompt, Confirm
from rich.table import Table

from art_studio_org.db import DB, default_db_path, project_root

app = typer.Typer(add_completion=False, no_args_is_help=False)
console = Console()


# ----------------------------
# Helpers
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def header():
    console.print(Panel.fit("[bold]Studio Inventory[/bold]\nMenu-first CLI", border_style="cyan"))

def pause():
    console.print()
    input("Press Enter to continue...")

def get_db(db_path: Optional[Path] = None) -> DB:
    return DB(path=db_path or default_db_path())

def safe_str(v) -> str:
    return "" if v is None else str(v)

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

def exports_dir() -> Path:
    d = project_root() / "exports"
    d.mkdir(parents=True, exist_ok=True)
    return d

def timestamp_slug() -> str:
    # local time is fine for filenames
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# ----------------------------
# Legacy ingest runner (subprocess)
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
        proc = subprocess.run(cmd, cwd=str(project_root()))
        return proc.returncode
    except FileNotFoundError:
        console.print("[red]Python executable not found.[/red]")
        return 1
    except KeyboardInterrupt:
        console.print("\n[yellow]Cancelled.[/yellow]")
        return 130


def run_legacy_ingest() -> None:
    """
    Tries the most likely current entrypoint first.
    """
    # 1) Try the existing "main" flow
    rc = run_module_in_subprocess("art_studio_org.main")
    if rc == 0:
        console.print("[green]Ingest completed.[/green]")
        return

    console.print(f"[yellow]art_studio_org.main exited with code {rc}. Trying fallback…[/yellow]")

    # 2) Fallback: ingest_all
    rc2 = run_module_in_subprocess("art_studio_org.ingest_all")
    if rc2 == 0:
        console.print("[green]Ingest completed.[/green]")
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
    if ctx.invoked_subcommand is None:
        run_menu()


def run_menu():
    while True:
        console.clear()
        header()

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "[bold]Ingest[/bold] receipts / packing lists")
        menu.add_row("2.", "[bold]Export[/bold] data (CSV / reports)")
        menu.add_row("3.", "[bold]Inventory[/bold] browse / search / receive / remove")
        menu.add_row("4.", "[bold]Vendors[/bold] enrich (DigiKey / McMaster) [dim](coming soon)[/dim]")
        menu.add_row("5.", "[bold]Labels[/bold] generate PDFs [dim](coming soon)[/dim]")
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
        menu.add_row("1.", "Run ingest (legacy flow)")
        menu.add_row("2.", "Show recent ingested files")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "0"], default="1")
        if choice == "0":
            return
        if choice == "1":
            run_legacy_ingest()
            pause()
        elif choice == "2":
            show_recent_ingests(db)


def show_recent_ingests(db: DB):
    console.clear()
    header()
    console.print("[bold]Recent ingests[/bold]\n")

    try:
        rows = db.rows("""
            SELECT first_seen_utc, vendor, order_ref, original_path
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
    t.add_column("vendor", width=14)
    t.add_column("order_ref", width=14)
    t.add_column("original_path")

    for r in rows:
        t.add_row(
            safe_str(r["first_seen_utc"]),
            safe_str(r["vendor"]),
            safe_str(r["order_ref"]),
            shorten(r["original_path"], 90),
        )

    console.print(t)
    pause()


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
        t.add_column("vendor", width=22)
        t.add_column("sku", width=14)
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
            console.print("\n[bold]Filter inventory (blank clears each field)[/bold]")
            vendor_f = Prompt.ask("Vendor contains", default="").strip()
            term_f = Prompt.ask("Search term (sku/label_short)", default="").strip()
            min_hand = Prompt.ask("Min on_hand", default="").strip()
            max_cost = Prompt.ask("Max avg_cost", default="").strip()
            inv_term = Prompt.ask("Last invoice contains", default="").strip()

            clauses = []
            new_params: list = []

            if vendor_f:
                clauses.append("vendor LIKE ?")
                new_params.append(f"%{vendor_f}%")
            if term_f:
                clauses.append("(sku LIKE ? OR label_short LIKE ?)")
                new_params.extend([f"%{term_f}%", f"%{term_f}%"])
            if min_hand:
                try:
                    clauses.append("on_hand >= ?")
                    new_params.append(float(min_hand))
                except ValueError:
                    console.print("[yellow]Min on_hand ignored (not a number).[/yellow]")
            if max_cost:
                try:
                    clauses.append("avg_unit_cost <= ?")
                    new_params.append(float(max_cost))
                except ValueError:
                    console.print("[yellow]Max avg_cost ignored (not a number).[/yellow]")
            if inv_term:
                clauses.append("last_invoice LIKE ?")
                new_params.append(f"%{inv_term}%")

            if clauses:
                dyn_where = " AND ".join(clauses)
                dyn_params = new_params
            else:
                dyn_where = ""
                dyn_params = []

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
    WHERE part_key LIKE ? COLLATE NOCASE
       OR sku LIKE ? COLLATE NOCASE
       OR vendor LIKE ? COLLATE NOCASE
       OR description LIKE ? COLLATE NOCASE
       OR label_short LIKE ? COLLATE NOCASE
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

    if part_key is None:
        part_key = Prompt.ask("part_key").strip()

    part_key = Prompt.ask("part_key (e.g. mcmaster:1234K56)").strip()
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

    console.print("\n[bold]Recent removals[/bold]")
    rem = db.rows("""
        SELECT ts_utc, qty_removed, project, note
        FROM parts_removed
        WHERE part_key = ?
        ORDER BY ts_utc DESC
        LIMIT 10
    """, [part_key])

    if not rem:
        console.print("[dim](none)[/dim]")
    else:
        rt = Table(show_header=True, header_style="bold cyan")
        rt.add_column("ts_utc", style="dim")
        rt.add_column("qty_removed", justify="right", width=10)
        rt.add_column("project", width=18)
        rt.add_column("note")
        for rr in rem:
            rt.add_row(safe_str(rr["ts_utc"]), safe_str(rr["qty_removed"]), shorten(rr["project"], 18), shorten(rr["note"], 60))
        console.print(rt)

    Prompt.ask("\nPress Enter to go back", default="")
    return


def inv_remove(db: DB):
    console.clear()
    header()
    console.print("[bold]Remove stock[/bold] (logs to parts_removed)\n")

    part_key = Prompt.ask("part_key").strip()
    if not part_key:
        return

    exists = db.scalar("SELECT 1 FROM parts_received WHERE part_key = ? LIMIT 1", [part_key])
    if not exists:
        console.print("[red]part_key not found in parts_received.[/red]")
        pause()
        return

    qty = FloatPrompt.ask("Qty removed", default=1.0)
    if qty <= 0:
        console.print("[yellow]Qty must be > 0[/yellow]")
        pause()
        return

    project = Prompt.ask("Project (optional)", default="").strip()
    note = Prompt.ask("Note (optional)", default="").strip()

    ts = utc_now_iso()
    removal_uid = str(uuid4())

    db.execute("""
        INSERT INTO parts_removed (removal_uid, part_key, qty_removed, ts_utc, project, note, updated_utc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [removal_uid, part_key, qty, ts, project, note, ts])

    console.print("[green]Logged removal.[/green] inventory_view on_hand will update automatically.")
    pause()


def inv_receive(db: DB):
    console.clear()
    header()
    console.print("[bold]Receive stock (manual)[/bold] (upserts parts_received)\n")

    part_key = Prompt.ask("part_key (recommended format: vendor:sku)").strip()
    if not part_key:
        return

    qty = FloatPrompt.ask("Qty received", default=1.0)
    if qty <= 0:
        console.print("[yellow]Qty must be > 0[/yellow]")
        pause()
        return

    unit_cost = Prompt.ask("Unit cost (optional)", default="").strip()
    try:
        unit_cost_f = float(unit_cost) if unit_cost else 0.0
    except ValueError:
        unit_cost_f = 0.0

    added_spend = qty * unit_cost_f

    exists = db.scalar("SELECT 1 FROM parts_received WHERE part_key = ? LIMIT 1", [part_key])
    ts = utc_now_iso()

    if exists:
        db.execute("""
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
        """, [qty, added_spend, qty, added_spend, added_spend, qty, ts, part_key])

        console.print("[green]Updated parts_received.[/green]")
        pause()
        return

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
    avg_unit_cost = (added_spend / qty) if (qty > 0 and added_spend > 0) else 0.0

    db.execute("""
        INSERT INTO parts_received (
            part_key, vendor, sku, description, desc_clean,
            label_line1, label_line2, label_short,
            purchase_url, airtable_url, label_qr_url, label_qr_text,
            units_received, total_spend, last_invoice, avg_unit_cost, updated_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        part_key, vendor, sku, description, desc_clean,
        label_line1, label_line2, label_short,
        purchase_url, airtable_url, label_qr_url, label_qr_text,
        qty, added_spend, None, avg_unit_cost, ts
    ])

    console.print("[green]Inserted new part into parts_received.[/green]")
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

    console.clear()
    header()
    console.print("[bold]DB diagnostics[/bold]\n")
    console.print(f"DB path: [cyan]{db_path}[/cyan]")
    console.print(f"DB exists: {'✅' if db_path.exists() else '❌'}\n")

    if not db_path.exists():
        pause()
        return

    db = get_db()

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
                count = str(db.scalar(f"SELECT COUNT(*) FROM {name}") or 0)
            except Exception:
                count = "?"
        t.add_row(typ, name, count)

    console.print(t)
    pause()


# ----------------------------
# Future stubs
# ----------------------------
def menu_vendors():
    console.clear()
    header()
    console.print("[bold]Vendors[/bold]\n")
    console.print("Next: DigiKey OAuth + product/media enrichment, then McMaster cert-based API enrichment.")
    pause()


def menu_labels():
    console.clear()
    header()
    console.print("[bold]Labels[/bold]\n")
    console.print("Paused for now. Once vendor enrichment is in, labels become DB-driven.")
    pause()


# ----------------------------
# Placeholder subcommands
# ----------------------------
@app.command()
def ingest():
    console.print("Use the menu for now. (Subcommands coming soon.)")


@app.command()
def export():
    console.print("Use the menu for now. (Subcommands coming soon.)")


if __name__ == "__main__":
    app()