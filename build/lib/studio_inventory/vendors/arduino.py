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
    # Invoices + cash sales both contain Arduino branding
    return "ARDUINO" in t0 and ("CASH SALE" in t0 or "INVOICE" in t0)


# -------------------------------------------------
# Order-level parsing
# -------------------------------------------------

def parse_order(pdf_path: str, debug: bool = False) -> dict:
    text = _all_text(pdf_path)

    invoice = _find(r"(CASH SALE n\.|INVOICE n\.)\s*([A-Z0-9/]+)", text, group=2)
    sales_order = _find(r"Sales Order\s*#\s*([A-Z0-9]+)", text)

    invoice_date = (
        _find(r"Receipt Date:\s*([0-9/]+)", text)
        or _find(r"Invoice Date:\s*([0-9/]+)", text)
    )

    # Totals in BOTH layouts:
    # Total Value Shipping Cost Total Tax Final Amount
    # $ 70.30 $ 0.00 $ 5.63 $ 75.93
    total_value, shipping, tax, total = _parse_totals_block(text)

    if debug:
        print(f"[ARDUINO] invoice={invoice} so={sales_order} date={invoice_date} "
              f"value={total_value} ship={shipping} tax={tax} total={total}")

    return {
        "vendor": "arduino",
        "invoice": invoice,
        "purchase_order": sales_order,
        "invoice_date": invoice_date,
        "account_number": None,
        "payment_date": None,
        "credit_card": None,
        "merchandise": total_value,
        "shipping": shipping,
        "sales_tax": tax,
        "total": total,
    }


# -------------------------------------------------
# Line items
# -------------------------------------------------
# Invoice example:
# ASX00061 Nano Connector Carrier 2.00 $ 11.80 $ 23.60 8%
# Cash sale example is similar.
ITEM_RE = re.compile(
    r"^(?P<sku>[A-Z]{3}\d{5})\s+"
    r"(?P<desc>.+?)\s+"
    r"(?P<qty>\d+(?:\.\d+)?)\s+\$\s*(?P<unit>\d+\.\d{2})\s+\$\s*(?P<ext>\d+\.\d{2})"
    r"(?:\s+(?P<tax>\d+)%\s*)?$"
)

COO_RE = re.compile(r"^COO:\s*(?P<coo>[A-Z]{2,})\s*$", re.I)

# Start parsing line-items after we hit the table header
TABLE_HEADER_RE = re.compile(r"^SKU\s+Description\b", re.I)
STOP_RE = re.compile(r"^Total Value\b", re.I)


def parse_line_items(pdf_path: str, debug: bool = False) -> list[dict]:
    lines = [ln.strip() for ln in _all_text(pdf_path).splitlines() if ln.strip()]

    items: list[dict] = []
    current: Optional[dict] = None
    in_table = False

    # Dedupe because some pdfplumber extractions can repeat blocks
    seen: set[tuple] = set()

    def flush():
        nonlocal current
        if not current:
            return
        key = (current.get("sku"), current.get("ordered"), current.get("unit_price"), current.get("line_total"))
        if key not in seen:
            seen.add(key)
            items.append(current)
        current = None

    for ln in lines:
        if STOP_RE.match(ln):
            break

        if TABLE_HEADER_RE.match(ln) or "SKU Description PO Ref." in ln:
            in_table = True
            continue

        if not in_table:
            continue

        m = ITEM_RE.match(ln)
        if m:
            flush()
            sku = m.group("sku")
            desc = m.group("desc").strip()
            qty = int(float(m.group("qty")))
            unit_price = float(m.group("unit"))
            line_total = float(m.group("ext"))

            current = {
                "line": None,
                "sku": sku,
                "part": sku,
                "description": desc,
                "ordered": qty,
                "shipped": qty,
                "balance": 0,
                "unit_price": unit_price,
                "line_total": line_total,
                "mfg": "Arduino",
                "mfg_pn": sku,
                "coo": None,
            }
            continue

        if current is not None:
            cm = COO_RE.match(ln)
            if cm:
                current["coo"] = cm.group("coo").strip().upper()
                continue

    flush()

    if debug:
        print(f"[ARDUINO] parsed {len(items)} line items")

    return items


# -------------------------------------------------
# Helpers
# -------------------------------------------------

TOTALS_BLOCK_RE = re.compile(
    r"Total Value\s+Shipping Cost\s+Total Tax\s+Final Amount\s*\n"
    r"\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})\s+\$\s*([0-9]+\.[0-9]{2})",
    re.I
)

def _parse_totals_block(text: str):
    m = TOTALS_BLOCK_RE.search(text)
    if not m:
        return None, None, None, None
    return float(m.group(1)), float(m.group(2)), float(m.group(3)), float(m.group(4))


def _all_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join(page.extract_text() or "" for page in pdf.pages)


def _find(pattern: str, text: str, group: int = 1) -> Optional[str]:
    m = re.search(pattern, text, re.I)
    return m.group(group).strip() if m else None
