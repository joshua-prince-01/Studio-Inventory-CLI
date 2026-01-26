from __future__ import annotations

from typing import Optional

from . import digikey, mcmaster

# Order matters: more-specific detectors first if needed
PARSERS = [
    digikey,
    mcmaster,
]

def pick_parser(pdf_path: str):
    for mod in PARSERS:
        try:
            if mod.detect(pdf_path):
                return mod
        except Exception:
            continue
    return None
