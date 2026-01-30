from __future__ import annotations

import re
from typing import Optional

import pdfplumber


# -------------------------------------------------
# Detection
# -------------------------------------------------

def detect(pdf_path: str) -> bool:
    with pdfplumber.open(pdf_path) as pdf:
        t0 = (pdf.pages[0].extract_text() or "").upper()
    return "OMC CORPORATION LIMITED" in t0 or "STEPPERONLINE" in t0


# -------------------------------------------------
# Order-level parsing
# -------------------------------------------------

def parse_order(pdf_path: str, debug: bool = False) -> dict:
    text = _all_text(pdf_path)

    invoice_date = _find(r"Date Added:\s*([0-9/]+)", text)
    order_id = _find(r"Order ID:\s*(\d+)", text)

    subtotal = _money_after(r"Sub-Total:", text)
    shipping = _money_after(r"USPS Ground:", text)
    packing = _money_after(r"Packing Fee:", text)
    total = _money_after(r"^Total:", text, multiline=True)

    # StepperOnline invoices generally do NOT include tax as a line item
    merchandise = subtotal
    shipping_total = (shipping or 0.0) + (packing or 0.0)

    if debug:
        print(
            f"[STEPPERONLINE] order={order_id} date={invoice_date} "
            f"sub={subtotal} ship={shipping_total} total={total}"
        )

    return {
        "vendor": "stepperonline",
        "invoice": order_id,
        "purchase_order": order_id,
        "invoice_date": invoice_date,
        "account_number": None,
        "payment_date": None,
        "credit_card": None,
        "merchandise": merchandise,
        "shipping": shipping_total,
        "sales_tax": None,
        "total": total,
    }


# -------------------------------------------------
# Line items
# -------------------------------------------------

# Header trigger
TABLE_HEADER_RE = re.compile(r"^Product Name\s+Model\s+Price\s+Total ex\. tax$", re.I)
STOP_RE = re.compile(r"^Sub-Total:", re.I)

# Qty + description line, e.g.:
# 2 x Nema 17 Stepper Motor ...
QTY_DESC_RE = re.compile(r"^(?P<qty>\d+)\s*x\s+(?P<desc>.+)$", re.I)

# "Ships from: United States"
SHIP_FROM_RE = re.compile(r"Ships from:\s*(.+)", re.I)

# Price tail, e.g.:
# DM332T $20.47 $40.94
# ... 17HS19-1684S-PG27 $36.59 $73.18
PRICE_TAIL_RE = re.compile(r"\$(?P<unit>\d+\.\d{2})\s+\$(?P<ext>\d+\.\d{2})\s*$")


def parse_line_items(pdf_path: str, debug: bool = False) -> list[dict]:
    lines = [ln.strip() for ln in _all_text(pdf_path).splitlines() if ln.strip()]

    items: list[dict] = []
    in_table = False

    pending_qty: Optional[int] = None
    pending_desc: Optional[str] = None
    pending_ship_from: Optional[str] = None

    seen: set[tuple] = set()

    def emit(sku: str, desc: str, qty: int, unit: float, ext: float, ship_from: Optional[str]):
        key = (sku, qty, unit, ext, desc)
        if key in seen:
            return
        seen.add(key)
        items.append({
            "line": None,
            "sku": sku,
            "part": sku,
            "description": desc,
            "ordered": qty,
            "shipped": qty,
            "balance": 0,
            "unit_price": unit,
            "line_total": ext,
            "mfg": "StepperOnline",
            "mfg_pn": sku,
            "coo": ship_from,  # "Ships from: United States" (not strictly COO, but useful provenance)
        })

    for ln in lines:
        if STOP_RE.match(ln):
            break

        if TABLE_HEADER_RE.match(ln):
            in_table = True
            continue

        if not in_table:
            continue

        qm = QTY_DESC_RE.match(ln)
        if qm:
            pending_qty = int(qm.group("qty"))
            pending_desc = qm.group("desc").strip()
            pending_ship_from = None
            continue

        sm = SHIP_FROM_RE.search(ln)
        if sm:
            pending_ship_from = sm.group(1).strip()
            continue

        pm = PRICE_TAIL_RE.search(ln)
        if pm:
            unit = float(pm.group("unit"))
            ext = float(pm.group("ext"))

            # Everything before the unit-price is "Model column + maybe trailing description"
            pre = ln[:pm.start()].strip()

            # Model/SKU is the last token before the price columns
            parts = pre.split()
            if not parts:
                continue
            sku = parts[-1]
            desc_suffix = " ".join(parts[:-1]).strip()

            # Build description robustly (handles multi-line product name)
            if pending_desc and desc_suffix:
                desc = f"{pending_desc} {desc_suffix}".strip()
            elif pending_desc:
                desc = pending_desc.strip()
            else:
                # Fallback if invoice has single-line rows without a leading "QTY x ..."
                desc = desc_suffix.strip()

            qty = pending_qty if pending_qty is not None else 1

            emit(sku=sku, desc=desc, qty=qty, unit=unit, ext=ext, ship_from=pending_ship_from)

            # reset pending state for next item
            pending_qty = None
            pending_desc = None
            pending_ship_from = None
            continue

    if debug:
        print(f"[STEPPERONLINE] parsed {len(items)} line items")

    return items


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _all_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def _find(pattern: str, text: str, group: int = 1) -> Optional[str]:
    m = re.search(pattern, text, re.I)
    return m.group(group).strip() if m else None


def _money_after(label_pattern: str, text: str, multiline: bool = False) -> Optional[float]:
    flags = re.I | (re.M if multiline else 0)
    m = re.search(label_pattern + r"\s*\$([0-9]+\.[0-9]{2})", text, flags)
    return float(m.group(1)) if m else None
