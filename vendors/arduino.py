from __future__ import annotations

import re
from typing import Optional

import pdfplumber


# -------------------------------------------------
# Detection
# -------------------------------------------------

def detect(pdf_path: str) -> bool:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            t0 = (pdf.pages[0].extract_text() or "").upper()
        # Arduino receipts/invoices usually have "ARDUINO, LLC" and "store-usa@arduino.cc"
        return ("ARDUINO" in t0) and ("ARDUINO.CC" in t0 or "STORE-" in t0 or "CASH SALE" in t0 or "INVOICE" in t0)
    except Exception:
        return False


# -------------------------------------------------
# Order-level parsing
# -------------------------------------------------

TOTALS_BLOCK_RE = re.compile(
    r"Total Value\s+Shipping Cost\s+Total Tax\s+Final Amount\s*\n"
    r"\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})",
    re.I,
)

def parse_order(pdf_path: str, debug: bool = False) -> dict:
    text = _all_text(pdf_path)

    # Cash sale / invoice numbers
    invoice = _find(r"(?:CASH SALE n\.|INVOICE n\.)\s*([A-Z0-9/]+)", text)
    sales_order = _find(r"Sales Order\s*#\s*([A-Z0-9]+)", text)

    invoice_date = (
        _find(r"Receipt Date:\s*([0-9/]+)", text)
        or _find(r"Invoice Date:\s*([0-9/]+)", text)
    )

    merchandise = shipping = tax = total = None
    tm = TOTALS_BLOCK_RE.search(text)
    if tm:
        merchandise = float(tm.group(1))
        shipping = float(tm.group(2))
        tax = float(tm.group(3))
        total = float(tm.group(4))

    if debug:
        print(f"[ARDUINO] invoice={invoice} so={sales_order} date={invoice_date} "
              f"value={merchandise} ship={shipping} tax={tax} total={total}")

    return {
        "vendor": "arduino",
        "invoice": invoice,
        "purchase_order": sales_order,
        "invoice_date": invoice_date,
        "account_number": None,
        "payment_date": None,
        "credit_card": None,
        "merchandise": merchandise,
        "shipping": shipping,
        "sales_tax": tax,
        "total": total,
    }


# -------------------------------------------------
# Line items
# -------------------------------------------------

# Row looks like:
# ASX00061 Nano Connector Carrier 2.00 $ 11.80 $ 23.60 6%
ITEM_ROW_RE = re.compile(
    r"^([A-Z]{3}\d{5})\s+(.*?)\s+(\d+\.\d{2})\s+\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})\s+(\d+%?)\s*$",
    re.I,
)

COO_RE = re.compile(r"^COO:\s*(.+)$", re.I)

def parse_line_items(pdf_path: str, debug: bool = False) -> list[dict]:
    lines = _all_text(pdf_path).splitlines()

    items: list[dict] = []
    current: Optional[dict] = None

    def flush():
        nonlocal current
        if current:
            items.append(current)
            current = None

    in_items = False
    for raw in lines:
        ln = raw.strip()
        if not ln:
            continue

        # Start of the items table is marked by header
        if ln.startswith("SKU Description Qty Unit Price Total Value Tax"):
            in_items = True
            continue

        if not in_items:
            continue

        # End of items block
        if ln.startswith("Total Value Shipping Cost Total Tax Final Amount"):
            break

        m = ITEM_ROW_RE.match(ln)
        if m:
            flush()
            sku = m.group(1).upper()
            desc = m.group(2).strip()
            qty = float(m.group(3))
            unit_price = float(m.group(4))
            line_total = float(m.group(5))

            current = {
                "line": None,
                "sku": sku,
                "part": sku,
                "description": desc,
                "ordered": int(round(qty)),
                "shipped": int(round(qty)),
                "balance": 0,
                "unit_price": unit_price,
                "line_total": line_total,
                "mfg": "Arduino",
                "mfg_pn": sku,
                "coo": None,
            }
            continue

        if current is None:
            continue

        cm = COO_RE.match(ln)
        if cm:
            current["coo"] = cm.group(1).strip()
            continue

    flush()

    if debug:
        print(f"[ARDUINO] parsed {len(items)} line items")

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
