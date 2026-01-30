# Studio Inventory

A deployable, installable CLI for managing studio inventory: ingesting receipts and packing lists, storing structured data in SQLite, exporting CSVs, and generating labels.

This project is designed with a **clean separation between code and data**:
- **Code** lives in the repository and is installable via `pip`.
- **Runtime data** (database, receipts, exports, logs) lives in a user workspace at `~/StudioInventory` by default.

---

## Features

- 📥 Interactive receipt / packing-list ingest
- 🗄 SQLite-backed inventory database
- 📤 Non-interactive CSV exports
- 🏷 Label generation (Avery templates)
- 🧭 Workspace-aware CLI (safe for pip / pipx installs)

---

## Requirements

- Python **3.11+**
- macOS or Linux (Windows should work but is untested)

---

## Installation (Development / Editable)

Clone the repo and install in editable mode:

```bash
git clone https://github.com/joshua-prince-01/PythonProject_studio_inventory.git
cd PythonProject_studio_inventory

python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .
```

Verify the CLI is available:

```bash
studio-inventory --help
```

---

## First-Time Setup

Initialize the workspace (creates folders under `~/StudioInventory`):

```bash
studio-inventory init
```

This creates:

```
~/StudioInventory/
├─ receipts/
├─ exports/
├─ log/
├─ label_presets/
├─ secrets/
└─ studio_inventory.sqlite
```

You can override the workspace location with:

```bash
export STUDIO_INV_HOME=/path/to/workspace
```

---

## Usage

### Ingest receipts / packing lists

```bash
studio-inventory ingest
```

- Starts browsing in `~/StudioInventory/receipts`
- Interactive folder + PDF picker
- Writes structured data to SQLite

Optional:

```bash
studio-inventory ingest --workspace ~/StudioInventory
```

---

### Export data to CSV

List tables/views:

```bash
studio-inventory export --list
```

Export a table:

```bash
studio-inventory export --object parts
```

Custom output path:

```bash
studio-inventory export --object orders --out ~/Desktop/orders.csv
```

---

## Project Structure

```
StudioInventory/
├─ pyproject.toml
├─ README.md
├─ src/
│  └─ studio_inventory/
│     ├─ cli.py
│     ├─ main.py
│     ├─ db.py
│     ├─ paths.py
│     ├─ vendors/
│     └─ labels/
└─ .gitignore
```

- `paths.py` is the **single source of truth** for workspace paths
- No runtime data is written into the repo or site-packages

---

## Git Hygiene

Runtime data is intentionally ignored via `.gitignore`:

- `.venv/`
- `exports/`
- `receipts/`
- `log/`
- `label_presets/`
- `studio_inventory.sqlite`

This keeps the repo clean and portable.

---

## Roadmap

- Vendor API enrichment (McMaster, DigiKey)
- Label templates as package resources
- `studio-inventory doctor` diagnostics command
- Tests and fixtures

---

## License

Private / internal (update as needed).

