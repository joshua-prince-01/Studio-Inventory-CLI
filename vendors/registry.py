from __future__ import annotations

from typing import Optional

from . import arduino, digikey, mcmaster

# Order matters: more-specific detectors first if needed
PARSERS = [
    arduino,
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
