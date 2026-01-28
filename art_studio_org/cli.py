from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm

from art_studio_org.db import DB, default_db_path

app = typer.Typer(add_completion=False, no_args_is_help=False)
console = Console()


# ----------------------------
# Utilities
# ----------------------------
def get_db(db_path: Optional[Path] = None) -> DB:
    return DB(path=db_path or default_db_path())


def header():
    console.print(Panel.fit("[bold]Studio Inventory[/bold]\nMenu-first CLI", border_style="cyan"))


def pause():
    console.print()
    input("Press Enter to continue...")


# ----------------------------
# Menu-first entry
# ----------------------------
@app.callback(invoke_without_command=True)
def main(ctx: typer.Context):
    """
    Menu-first launcher. If you later pass subcommands, it won't show the menu.
    """
    if ctx.invoked_subcommand is None:
        run_menu()


def run_menu():
    while True:
        console.clear()
        header()

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "[bold]Ingest[/bold] receipts / packing lists")
        menu.add_row("2.", "[bold]Export[/bold] data (CSV / reports)")
        menu.add_row("3.", "[bold]Parts[/bold] browse / search / update")
        menu.add_row("4.", "[bold]Vendors[/bold] enrich (DigiKey / McMaster) [dim](coming soon)[/dim]")
        menu.add_row("5.", "[bold]Labels[/bold] generate PDFs [dim](coming soon)[/dim]")
        menu.add_row("6.", "Settings / Diagnostics")
        menu.add_row("0.", "Quit")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "3", "4", "5", "6", "0"], default="3")

        if choice == "1":
            menu_ingest()
        elif choice == "2":
            menu_export()
        elif choice == "3":
            menu_parts()
        elif choice == "4":
            menu_vendors()
        elif choice == "5":
            menu_labels()
        elif choice == "6":
            menu_settings()
        elif choice == "0":
            console.print("\nBye.\n")
            return


# ----------------------------
# Menu actions (stubs to wire into existing code)
# ----------------------------
def menu_ingest():
    console.clear()
    header()
    console.print("[bold]Ingest[/bold]\n")

    console.print(
        "- Hook this to your existing ingest flow (likely in art_studio_org/main.py or ingest_all.py).\n"
        "- We’ll keep this as the menu entrypoint, but later it will call a function.\n"
    )

    # Example placeholder:
    if Confirm.ask("Run ingest now? (placeholder)", default=False):
        console.print("[yellow]TODO:[/yellow] call your ingest runner here.")
    pause()


def menu_export():
    console.clear()
    header()
    console.print("[bold]Export[/bold]\n")

    console.print(
        "- Hook this to your existing export logic.\n"
        "- Later: export parts table, orders, vendor-enriched attributes, label-ready views.\n"
    )
    pause()


def menu_parts():
    db = get_db()

    while True:
        console.clear()
        header()
        console.print("[bold]Parts[/bold]\n")

        menu = Table(show_header=False, box=None)
        menu.add_row("1.", "List recent parts")
        menu.add_row("2.", "Search parts")
        menu.add_row("3.", "Show part details (by id)")
        menu.add_row("4.", "Update inventory (qty/bin) [dim](schema-dependent)[/dim]")
        menu.add_row("0.", "Back")
        console.print(menu)

        choice = Prompt.ask("\nChoose", choices=["1", "2", "3", "4", "0"], default="2")
        if choice == "0":
            return
        if choice == "1":
            parts_list_recent(db)
        elif choice == "2":
            parts_search(db)
        elif choice == "3":
            parts_show(db)
        elif choice == "4":
            parts_update_stub(db)


def parts_list_recent(db: DB):
    console.clear()
    header()
    console.print("[bold]Recent parts[/bold]\n")

    # NOTE: This assumes you have a table called "parts".
    # If your schema differs, we’ll adapt these queries.
    try:
        rows = db.rows("SELECT id, name, qty, bin FROM parts ORDER BY id DESC LIMIT 30")
    except Exception as e:
        console.print(f"[red]Query failed:[/red] {e}")
        console.print("\n[yellow]TODO:[/yellow] update queries to match your actual schema.")
        pause()
        return

    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("id", style="dim", width=6)
    t.add_column("name")
    t.add_column("qty", justify="right", width=6)
    t.add_column("bin", width=10)

    for r in rows:
        t.add_row(str(r["id"]), str(r["name"]), str(r["qty"]), str(r["bin"]))
    console.print(t)
    pause()


def parts_search(db: DB):
    console.clear()
    header()
    console.print("[bold]Search parts[/bold]\n")
    term = Prompt.ask("Search term (name/sku/etc)", default="")

    if not term.strip():
        return

    try:
        rows = db.rows(
            "SELECT id, name, qty, bin FROM parts WHERE name LIKE ? ORDER BY id DESC LIMIT 50",
            [f"%{term}%"],
        )
    except Exception as e:
        console.print(f"[red]Search failed:[/red] {e}")
        pause()
        return

    t = Table(show_header=True, header_style="bold magenta")
    t.add_column("id", style="dim", width=6)
    t.add_column("name")
    t.add_column("qty", justify="right", width=6)
    t.add_column("bin", width=10)

    for r in rows:
        t.add_row(str(r["id"]), str(r["name"]), str(r["qty"]), str(r["bin"]))
    console.print(t)
    pause()


def parts_show(db: DB):
    console.clear()
    header()
    console.print("[bold]Show part[/bold]\n")
    part_id = IntPrompt.ask("Part id", default=1)

    try:
        rows = db.rows("SELECT * FROM parts WHERE id = ?", [part_id])
    except Exception as e:
        console.print(f"[red]Lookup failed:[/red] {e}")
        pause()
        return

    if not rows:
        console.print("[yellow]No part found.[/yellow]")
        pause()
        return

    r = rows[0]
    t = Table(show_header=False, box=None)
    for k in r.keys():
        t.add_row(f"[dim]{k}[/dim]", str(r[k]))
    console.print(t)
    pause()


def parts_update_stub(db: DB):
    console.clear()
    header()
    console.print("[bold]Update inventory (stub)[/bold]\n")

    console.print(
        "This is schema-dependent. Once we confirm your table columns, we’ll implement:\n"
        "- increment/decrement qty\n"
        "- set bin\n"
        "- set notes\n"
        "- quick receive workflow\n"
    )
    pause()


def menu_vendors():
    console.clear()
    header()
    console.print("[bold]Vendors[/bold]\n")
    console.print(
        "Next we’ll add:\n"
        "- DigiKey OAuth + product/media enrichment\n"
        "- McMaster cert auth + product subscriptions + attribute/media pull\n"
    )
    pause()


def menu_labels():
    console.clear()
    header()
    console.print("[bold]Labels[/bold]\n")
    console.print(
        "We paused label generation, but the menu entry is ready.\n"
        "Once vendor enrichment is in place, labels become DB-driven.\n"
    )
    pause()


def menu_settings():
    console.clear()
    header()
    console.print("[bold]Settings / Diagnostics[/bold]\n")

    db_path = default_db_path()
    console.print(f"DB path: [cyan]{db_path}[/cyan]")
    console.print(f"DB exists: {'✅' if db_path.exists() else '❌'}")

    if db_path.exists():
        db = get_db()
        # Try to detect tables
        try:
            tables = db.rows("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            t = Table(title="SQLite tables")
            t.add_column("name")
            for r in tables:
                t.add_row(str(r["name"]))
            console.print(t)
        except Exception as e:
            console.print(f"[red]Could not list tables:[/red] {e}")

    pause()


# ----------------------------
# Future subcommands (stubs)
# ----------------------------
@app.command()
def parts():
    """Non-interactive entrypoint later: inventory parts ..."""
    console.print("Use the menu for now. (Subcommands coming soon.)")


@app.command()
def ingest():
    """Non-interactive entrypoint later: inventory ingest ..."""
    console.print("Use the menu for now. (Subcommands coming soon.)")


@app.command()
def export():
    """Non-interactive entrypoint later: inventory export ..."""
    console.print("Use the menu for now. (Subcommands coming soon.)")


@app.command()
def vendors():
    """Non-interactive entrypoint later: inventory vendors ..."""
    console.print("Use the menu for now. (Subcommands coming soon.)")


@app.command()
def labels():
    """Non-interactive entrypoint later: inventory labels ..."""
    console.print("Use the menu for now. (Subcommands coming soon.)")


if __name__ == "__main__":
    app()
