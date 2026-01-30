from __future__ import annotations

from . import stepperonline, arduino, digikey, mcmaster, sendcutsend

# Order matters: more-specific detectors first if needed
PARSERS = [
    stepperonline,
    arduino,
    digikey,
    sendcutsend,
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
