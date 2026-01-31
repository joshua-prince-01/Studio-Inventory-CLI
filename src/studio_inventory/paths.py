from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, date

APP_NAME = "StudioInventory"

# Single source of truth for all file locations.
# Workspace is where the DB, imports, exports, and logs live.
# Code repo stays clean.

DEFAULT_WORKSPACE = Path.home() / "StudioInventory"


def workspace_root() -> Path:
    """
    Return the workspace root directory.

    Priority:
      1) STUDIO_INV_HOME env var (absolute or relative path)
      2) ~/StudioInventory
    """
    raw = os.environ.get("STUDIO_INV_HOME", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return DEFAULT_WORKSPACE.resolve()


def db_path() -> Path:
    """SQLite database file path inside the workspace root."""
    return workspace_root() / "studio_inventory.sqlite"


def imports_dir() -> Path:
    """Where PDFs are staged / stored for ingest."""
    return workspace_root() / "imports"


def exports_dir() -> Path:
    """Where CSV/PDF label exports go."""
    return workspace_root() / "exports"


def log_dir() -> Path:
    """Where logs go."""
    return workspace_root() / "log"


def duplicates_dir() -> Path:
    """Where duplicate PDFs are moved."""
    return workspace_root() / "duplicates"


def receipts_dir() -> Path:
    """Optional: where raw receipt PDFs can live."""
    d = workspace_root() / "receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def label_presets_dir() -> Path:
    d = workspace_root() / "label_presets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def secrets_dir() -> Path:
    d = workspace_root() / "secrets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def imports_run_dir(run_date: date | None = None) -> Path:
    """Date-stamped ingest folder inside imports/, e.g. imports/2026-01-30."""
    stamp = run_date.isoformat() if run_date else datetime.now().strftime("%Y-%m-%d")
    d = imports_dir() / stamp
    d.mkdir(parents=True, exist_ok=True)
    return d


def ensure_workspace() -> None:
    """
    Create all workspace folders (does not create anything in the code repo).
    """
    workspace_root().mkdir(parents=True, exist_ok=True)
    for d in (
        imports_dir(),
        exports_dir(),
        log_dir(),
        duplicates_dir(),
        receipts_dir(),
        label_presets_dir(),
        secrets_dir(),
    ):
        d.mkdir(parents=True, exist_ok=True)


def project_root() -> Path:
    """
    Best-effort repo root when running from source.

    Note: once installed into site-packages, this points inside the install
    location and should NOT be used for writable data paths.
    """
    p = Path(__file__).resolve()
    # .../src/studio_inventory/paths.py -> parents[2] == repo root
    return p.parents[2] if len(p.parents) >= 3 else p.parent
