from __future__ import annotations

from pathlib import Path
import os
from datetime import datetime, date

APP_NAME = "StudioInventory"


def workspace_root() -> Path:
    """Runtime data folder.

    Defaults to ~/StudioInventory.
    Override with STUDIO_INV_HOME=/path.
    """
    env = os.getenv("STUDIO_INV_HOME")
    if env:
        return Path(env).expanduser().resolve()
    return (Path.home() / APP_NAME).resolve()


def ensure_workspace() -> Path:
    """Create the workspace folder structure if missing; return workspace root."""
    root = workspace_root()
    root.mkdir(parents=True, exist_ok=True)
    for sub in ["receipts", "exports", "imports", "log", "label_presets", "secrets"]:
        (root / sub).mkdir(exist_ok=True)
    return root


def receipts_dir() -> Path:
    d = workspace_root() / "receipts"
    d.mkdir(parents=True, exist_ok=True)
    return d


def exports_dir() -> Path:
    d = workspace_root() / "exports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def log_dir() -> Path:
    d = workspace_root() / "log"
    d.mkdir(parents=True, exist_ok=True)
    return d


def label_presets_dir() -> Path:
    d = workspace_root() / "label_presets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def label_templates_dir() -> Path:
    d = workspace_root() / "label_templates"
    d.mkdir(parents=True, exist_ok=True)
    return d


def secrets_dir() -> Path:
    d = workspace_root() / "secrets"
    d.mkdir(parents=True, exist_ok=True)
    return d


def imports_dir() -> Path:
    """Workspace archive folder for original PDFs copied at ingest time."""
    d = workspace_root() / "imports"
    d.mkdir(parents=True, exist_ok=True)
    return d


def imports_run_dir(run_date: date | None = None) -> Path:
    """Date-stamped ingest folder inside imports/, e.g. imports/2026-01-30."""
    stamp = run_date.isoformat() if run_date else datetime.now().strftime("%Y-%m-%d")
    d = imports_dir() / stamp
    d.mkdir(parents=True, exist_ok=True)
    return d


def project_root() -> Path:
    """Best-effort repo root when running from source.

    Note: once installed into site-packages, this points inside the install
    location and should NOT be used for writable data paths.
    """
    p = Path(__file__).resolve()
    # .../src/studio_inventory/paths.py -> parents[2] == repo root
    return p.parents[2] if len(p.parents) >= 3 else p.parent
