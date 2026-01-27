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

    subtotal = _money_after("Sub-Total:", text)
    shipping = _money_after("USPS Ground:", text)
    packing = _money_after("Packing Fee:", text)
    total = _money_after("Total:", text)

    merchandise = subtotal
    shipping_total = (shipping or 0) + (packing or 0)

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

SKU_PRICE_RE = re.compile(
    r"^(?P<sku>[A-Z0-9\-]+)\s+\$(?P<unit>\d+\.\d{2})\s+\$(?P<ext>\d+\.\d{2})$"
)

QTY_DESC_RE = re.compile(r"^(?P<qty>\d+)\s+x\s+(?P<desc>.+)$", re.I)

SHIP_FROM_RE = re.compile(r"Ships from:\s*(.+)", re.I)


def parse_line_items(pdf_path: str, debug: bool = False) -> list[dict]:
    lines = [ln.strip() for ln in _all_text(pdf_path).splitlines() if ln.strip()]

    items = []
    pending_desc = None
    pending_qty = None
    pending_coo = None

    for ln in lines:
        # description + qty
        m = QTY_DESC_RE.match(ln)
        if m:
            pending_qty = int(m.group("qty"))
            pending_desc = m.group("desc").strip()
            pending_coo = None
            continue

        # country of origin
        sm = SHIP_FROM_RE.search(ln)
        if sm:
            pending_coo = sm.group(1).strip()
            continue

        # SKU + prices
        pm = SKU_PRICE_RE.match(ln)
        if pm and pending_desc:
            sku = pm.group("sku")

            items.append({
                "line": None,
                "sku": sku,
                "part": sku,
                "description": pending_desc,
                "ordered": pending_qty,
                "shipped": pending_qty,
                "balance": 0,
                "unit_price": float(pm.group("unit")),
                "line_total": float(pm.group("ext")),
                "mfg": "StepperOnline",
                "mfg_pn": sku,
                "coo": pending_coo,
            })

            pending_desc = None
            pending_qty = None
            pending_coo = None
            continue

        if ln.startswith("Sub-Total"):
            break

    if debug:
        print(f"[STEPPERONLINE] parsed {len(items)} line items")

    return items


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _all_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def _find(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text, re.I)
    return m.group(1).strip() if m else None


def _money_after(label: str, text: str) -> Optional[float]:
    m = re.search(label + r"\s*\$([0-9]+\.[0-9]{2})", text, re.I)
    return float(m.group(1)) if m else None
