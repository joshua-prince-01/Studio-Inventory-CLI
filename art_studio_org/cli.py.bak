from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Sequence
from uuid import uuid4

import csv
import subprocess
import sys

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, FloatPrompt, Confirm
from rich.table import Table

from art_studio_org.db import DB, default_db_path, project_root
from art_studio_org.vendors.mcmaster_api import McMasterClient, McMasterCreds

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

def exports_dir() -> Path:
    d = project_root() / "exports"
    d.mkdir(parents=True, exist_ok=True)
    return d

def timestamp_slug() -> str:
    # local time is fine for filenames
    return datetime.now().strftime("%Y%m%d_%H%M%S")

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

    console.print("[bold]Choose template[/bold]\n")
    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("#", justify="right", width=3)
    t.add_column("file")
    for i, p in enumerate(templates, 1):
        t.add_row(str(i), p.name)
    console.print(t)

    choice = IntPrompt.ask("Template #", default=1)
    if not (1 <= choice <= len(templates)):
        console.print("[red]Invalid choice[/red]")
        pause()
        return None

    return templates[choice - 1]

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
    t.add_column("vendor", width=12)
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

    # de-dupe preserving order
    seen = set()
    uniq: list[int] = []
    for n in out:
        if n not in seen:
            uniq.append(n)
            seen.add(n)
    return uniq


def fetch_parts_received_for_part_keys(db: DB, part_keys: list[str]) -> list[dict]:
    """
    Fetch label-relevant fields from parts_received for a list of part_keys,
    preserving the order of part_keys.
    """
    if not part_keys:
        return []

    qmarks = ",".join(["?"] * len(part_keys))
    got = db.rows(
        f"""
        SELECT
            part_key, vendor, sku,
            label_line1, label_line2, label_short,
            purchase_url, label_qr_text
        FROM parts_received
        WHERE part_key IN ({qmarks})
        """,
        part_keys,
    )
    by_key = {r["part_key"]: dict(r) for r in got}
    return [by_key[k] for k in part_keys if k in by_key]

def inv_browse(
    db: DB,
    where_sql: str | None = None,
    params: list | None = None,
    title: str = "Inventory browse",
    order_by: str = "vendor, sku",
    allow_select: bool = False,
):
    """
    Paged browser for inventory_view.

    Commands:
      n / p / g / s  : paging
      v / h / c / o  : sort (vendor / on_hand / cost / last_invoice)
      f              : filter (vendor/sku/label_short/description + thresholds)
      <row#>         : open details
      sel 87:200,205 : (when allow_select=True) return selected part_keys
      q              : back
    """
    params = params or []
    page_sizes = [10, 25, 50, 100]
    page_size = 25
    page = 1

    base_where_sql = where_sql.strip() if where_sql else ""
    if base_where_sql and not base_where_sql.lower().lstrip().startswith("where"):
        base_where_sql = "WHERE " + base_where_sql

    # dynamic filter state (set via 'f')
    dyn_clause = ""
    dyn_params: list = []

    def combined_where() -> tuple[str, list]:
        if base_where_sql and dyn_clause:
            return f"{base_where_sql} AND ({dyn_clause})", params + dyn_params
        if base_where_sql:
            return base_where_sql, params
        if dyn_clause:
            return f"WHERE {dyn_clause}", dyn_params
        return "", []

    def total_rows() -> int:
        wh, p = combined_where()
        return int(db.scalar(f"SELECT COUNT(*) FROM inventory_view {wh}", p) or 0)

    def fetch_page(pageno: int, ps: int):
        wh, p = combined_where()
        offset = (pageno - 1) * ps
        sql = f"""
            SELECT part_key, vendor, sku, label_short, on_hand, avg_unit_cost, last_invoice
            FROM inventory_view
            {wh}
            ORDER BY {order_by}
            LIMIT ? OFFSET ?
        """
        return db.rows(sql, p + [ps, offset])

    def fetch_all_part_keys() -> list[str]:
        wh, p = combined_where()
        rows = db.rows(
            f"""
            SELECT part_key
            FROM inventory_view
            {wh}
            ORDER BY {order_by}
            """,
            p,
        )
        return [r["part_key"] for r in rows]

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
        t.add_column("vendor", width=14)
        t.add_column("sku", width=22)
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

        # status + commands
        cmd_line = (
            f"\nPage [cyan]{page}[/cyan] / [cyan]{max_page}[/cyan]  |  "
            f"Rows: [cyan]{total}[/cyan]  |  Page size: [cyan]{page_size}[/cyan]  |  "
            f"Sort: [cyan]{order_by}[/cyan]"
        )
        console.print(cmd_line)

        cmds = (
            "[dim]Commands:[/dim] "
            "[bold]n[/bold] next  [bold]p[/bold] prev  [bold]g[/bold] goto  "
            "[bold]s[/bold] size  "
            "[bold]v[/bold] sort-vendor  [bold]h[/bold] sort-on_hand  "
            "[bold]c[/bold] sort-cost  [bold]o[/bold] sort-last_invoice  "
            "[bold]f[/bold] filter  "
            "[bold]q[/bold] back  [bold]<row#>[/bold] details"
        )
        if allow_select:
            cmds += "  [bold]sel 87:200,205[/bold] select"
        console.print(cmds)

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
            order_by = "last_invoice DESC, vendor, sku"
            page = 1

        # filter builder
        elif cmd_l == "f":
            console.print("\n[bold]Filter[/bold] (blank = no filter for that field)")

            vendor_s = Prompt.ask("Vendor contains", default="").strip()
            term_s = Prompt.ask("Search term (sku/label_short/description)", default="").strip()
            min_on_hand_s = Prompt.ask("Min on_hand", default="").strip()
            max_cost_s = Prompt.ask("Max avg_cost", default="").strip()
            invoice_s = Prompt.ask("Last invoice contains", default="").strip()

            clauses = []
            new_params = []

            if vendor_s:
                clauses.append("vendor LIKE ?")
                new_params.append(f"%{vendor_s}%")
            if term_s:
                clauses.append("(sku LIKE ? OR label_short LIKE ? OR description LIKE ?)")
                like = f"%{term_s}%"
                new_params.extend([like, like, like])
            if invoice_s:
                clauses.append("last_invoice LIKE ?")
                new_params.append(f"%{invoice_s}%")
            if min_on_hand_s:
                try:
                    clauses.append("CAST(on_hand AS REAL) >= ?")
                    new_params.append(float(min_on_hand_s))
                except ValueError:
                    console.print("[yellow]Min on_hand ignored (not a number).[/yellow]")
            if max_cost_s:
                try:
                    clauses.append("avg_unit_cost <= ?")
                    new_params.append(float(max_cost_s))
                except ValueError:
                    console.print("[yellow]Max avg_cost ignored (not a number).[/yellow]")

            dyn_clause = " AND ".join(clauses)
            dyn_params = new_params
            page = 1

        # selection
        elif allow_select and cmd_l.startswith("sel"):
            spec = cmd[3:].strip()
            if not spec:
                console.print("[yellow]Usage: sel 87:200,205,206[/yellow]")
                pause()
                continue

            row_nums = parse_row_spec(spec)
            all_keys = fetch_all_part_keys()

            part_keys: list[str] = []
            for n in row_nums:
                if 1 <= n <= len(all_keys):
                    part_keys.append(all_keys[n - 1])

            if not part_keys:
                console.print("[yellow]No valid rows selected.[/yellow]")
                pause()
                continue

            return part_keys

        # drill-in by absolute row number
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

        else:
            # ignore unknown commands
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
        order_by="vendor, sku",
    )


def inv_show(db: DB, part_key: str | None = None):
    console.clear()
    header()
    console.print("[bold]Show inventory item[/bold]\n")

    # Only prompt if caller didn't supply a part_key
    if part_key is None:
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
            rt.add_row(
                safe_str(rr["ts_utc"]),
                safe_str(rr["qty_removed"]),
                shorten(rr["project"], 18),
                shorten(rr["note"], 60),
            )
        console.print(rt)

    # "Click out" back to browse
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
    while True:
        console.clear()
        header()
        console.print("[bold]Vendors[/bold]\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "McMaster – enrich ONE part")
        menu.add_row("2.", "McMaster – enrich missing (later)")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "0"], default="1")
        if choice == "0":
            return
        if choice == "1":
            vendor_mcmaster_enrich_one()


def vendor_mcmaster_enrich_one():
    console.clear()
    header()
    console.print("[bold]McMaster enrich ONE[/bold]\n")

    part_key = Prompt.ask("part_key (mcmaster:1234K56)").strip()
    if not part_key.startswith("mcmaster:"):
        console.print("[red]part_key must start with mcmaster:[/red]")
        pause()
        return

    sku = part_key.split(":", 1)[1]
    db = get_db()

    try:
        client = McMasterClient(McMasterCreds.from_env())
        client.add_product(sku)
        info = client.product_info(sku)

        title = info.get("Title")
        desc = info.get("Description")
        url = info.get("ProductUrl")

        image_url = None
        for link in info.get("Links", []):
            if link.get("Rel") == "Image":
                image_url = link.get("Href")

        specs = info.get("Specifications")

        db.upsert_vendor_enrichment(
            part_key=part_key,
            vendor="mcmaster",
            sku=sku,
            source="mcmaster",
            title=title,
            description=desc,
            product_url=url,
            image_url=image_url,
            specs_json=specs,
            raw_json=info,
        )

        console.print("[green]Enriched and saved to vendor_enrichment.[/green]")
    except Exception as e:
        console.print(f"[red]Failed:[/red] {e}")

    pause()


def menu_labels():
    db = get_db()

    while True:
        console.clear()
        header()
        console.print("[bold]Labels[/bold]\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "Make labels PDF (from parts_received)")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "0"], default="1")
        if choice == "0":
            return
        if choice == "1":
            labels_make_pdf(db)

from art_studio_org.labels.make_pdf import make_labels_pdf
from pathlib import Path

def open_path_in_default_app(p: Path) -> None:
    try:
        if sys.platform.startswith("darwin"):
            subprocess.Popen(["open", str(p)])
        elif sys.platform.startswith("win"):
            subprocess.Popen(["cmd", "/c", "start", "", str(p)])
        else:
            subprocess.Popen(["xdg-open", str(p)])
    except Exception:
        # best-effort only
        return


def labels_make_pdf(db: DB):
    console.clear()
    header()
    console.print("[bold]Make labels PDF[/bold]\n")

    # template (pick from list)
    tpl_path = pick_label_template()
    if not tpl_path:
        return

    console.print("[bold]Select items from inventory[/bold]")
    console.print("[dim]Use sort keys (v/h/c/o), filter (f), then select: sel 87:200,205,206[/dim]\n")

    part_keys = inv_browse(
        db,
        title="Inventory (select rows for labels)",
        order_by="vendor, sku",
        allow_select=True,
    )
    if not part_keys:
        return

    rows = fetch_parts_received_for_part_keys(db, part_keys)
    if not rows:
        console.print("[yellow]No items found for selection.[/yellow]")
        pause()
        return

    start_pos = IntPrompt.ask("Start label position (1 = first label top-left)", default=1)
    include_qr = Confirm.ask("Include QR code?", default=True)

    outdir = exports_dir()
    preview_pdf = outdir / "_labels_preview.pdf"

    make_labels_pdf(
        template_path=tpl_path,
        out_pdf=preview_pdf,
        rows=rows,
        start_pos=start_pos,
        include_qr=include_qr,
        draw_boxes=False,
    )

    console.print(f"Preview written: [cyan]{preview_pdf}[/cyan]")
    open_path_in_default_app(preview_pdf)

    if not Confirm.ask("Export final PDF?", default=False):
        console.print("[yellow]Not exported. Preview kept.[/yellow]")
        pause()
        return

    out_pdf = outdir / f"labels_{timestamp_slug()}.pdf"
    make_labels_pdf(
        template_path=tpl_path,
        out_pdf=out_pdf,
        rows=rows,
        start_pos=start_pos,
        include_qr=include_qr,
        draw_boxes=False,
    )

    console.print(f"[green]Wrote[/green] {out_pdf}")
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