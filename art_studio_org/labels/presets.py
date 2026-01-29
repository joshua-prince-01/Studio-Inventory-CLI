from __future__ import annotations

import json
from pathlib import Path


def _preset_dir(project_root: Path, template_path: Path) -> Path:
    tpl_stem = template_path.stem
    d = project_root / "label_presets" / tpl_stem
    d.mkdir(parents=True, exist_ok=True)
    return d


def list_label_presets(project_root: Path, template_path: Path) -> list[Path]:
    d = _preset_dir(project_root, template_path)
    return sorted(d.glob("*.json"))


def load_label_preset(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def save_label_preset(project_root: Path, template_path: Path, preset_name: str, layout: dict) -> Path:
    d = _preset_dir(project_root, template_path)
    name = preset_name.strip()
    if not name:
        raise ValueError("preset_name is empty")
    if not name.lower().endswith(".json"):
        name += ".json"
    p = d / name
    payload = dict(layout)
    payload.setdefault("meta", {})
    payload["meta"].update({
        "preset_name": preset_name,
        "template_file": template_path.name,
    })
    p.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return p
