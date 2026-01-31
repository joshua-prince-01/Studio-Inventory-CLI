# Studio Inventory CLI

A menu-driven CLI for ingesting receipts, managing inventory, and generating labels.

---

## Installation (Recommended: pipx)

`pipx` installs Python CLI tools in isolated environments and makes them available globally. This keeps the application code separate from your data and avoids dependency conflicts.

### Requirements

* macOS or Linux
* Python 3.10+
* Homebrew (macOS)

---

## 1. Install pipx

### Option A (recommended – works everywhere)
```bash
python3 -m pip install --user pipx
python3 -m pipx ensurepath
```
Restart your terminal, then verify:
```bash
pipx --version
```
### Option B (macOS with Homebrew)
```bash
brew install pipx
pipx ensurepath
```

## 2. Install Studio Inventory from GitHub

This installs the CLI from the `main` branch (latest code):

```bash
pipx install "git+https://github.com/joshua-prince-01/Studio-Inventory-CLI.git@main"
```

Verify the install:

```bash
studio-inventory --help
```

You should see the menu and available commands.

---

## Workspace & Database Location

Studio Inventory stores **all user data** (database, imports, exports, logs, label presets) in a single **workspace directory**.

The workspace location is controlled by the environment variable:

```bash
STUDIO_INV_HOME
```

### Recommended workspace location

```bash
~/StudioInventory
```

---

## 3. Create and initialize the workspace

```bash
mkdir -p ~/StudioInventory
export STUDIO_INV_HOME="$HOME/StudioInventory"
```

Initialize the workspace and database:

```bash
studio-inventory init
studio-inventory diagnostics
```

This creates:

```text
~/StudioInventory/
├── studio_inventory.sqlite
├── imports/
├── exports/
├── log/
├── duplicates/
├── label_presets/
└── secrets/
```

---

## 4. Make the workspace setting permanent (recommended)

Add this line to your shell configuration:

```bash
echo 'export STUDIO_INV_HOME="$HOME/StudioInventory"' >> ~/.zshrc
source ~/.zshrc
```

From now on, the application will always use this workspace.

---

## Running the Application

```bash
studio-inventory
```

You’ll see the interactive menu for ingesting receipts, browsing inventory, exporting data, and generating labels.

---

## Updating the Application

To pull the latest version from GitHub:

```bash
pipx upgrade studio-inventory
```

If you installed from a Git branch and want to force a refresh:

```bash
pipx reinstall "git+https://github.com/joshua-prince-01/Studio-Inventory-CLI.git@main" --force
```

---

## Development vs Production (Optional)

If you are developing the code locally (for example, in PyCharm), you can keep separate workspaces:

* **Development workspace** (used in PyCharm):

```bash
export STUDIO_INV_HOME="$HOME/StudioInventory_DEV"
```

* **Production workspace** (used by pipx install):

```bash
export STUDIO_INV_HOME="$HOME/StudioInventory"
```

This allows testing new features without touching your production database.

---

## Uninstall

```bash
pipx uninstall studio-inventory
```

Your workspace and database are **not deleted**.

---

## Where is my data?

All user data lives inside:

```bash
$STUDIO_INV_HOME
```

The application code itself lives inside pipx’s isolated environment and can be updated or removed without affecting your data.
