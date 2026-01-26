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
    return "ARDUINO" in t0


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

    total_value = _money_after("Total Value", text)
    shipping = _money_after("Shipping Cost", text)
    tax = _money_after("Total Tax", text)
    total = _money_after("Final Amount", text)

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

SKU_HEADER_RE = re.compile(r"^([A-Z0-9]+)\s+(.*)$")
QTY_PRICE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s+\$\s*([0-9.]+)\s+\$\s*([0-9.]+)")


def parse_line_items(pdf_path: str, debug: bool = False) -> list[dict]:
    lines = _all_text(pdf_path).splitlines()

    items = []
    current: Optional[dict] = None

    for raw in lines:
        ln = raw.strip()
        if not ln:
            continue

        if ln.startswith("Total Value"):
            break

        # SKU + description
        m = SKU_HEADER_RE.match(ln)
        if m and len(m.group(1)) >= 5:
            if current:
                items.append(current)

            current = {
                "line": None,
                "sku": m.group(1),
                "part": m.group(1),
                "description": m.group(2).strip(),
                "ordered": None,
                "shipped": None,
                "balance": None,
                "unit_price": None,
                "line_total": None,
                "mfg": "Arduino",
                "mfg_pn": m.group(1),
                "coo": None,
            }
            continue

        if current is None:
            continue

        if ln.startswith("COO:"):
            current["coo"] = ln.split(":", 1)[1].strip()
            continue

        qm = QTY_PRICE_RE.search(ln.replace("  ", " "))
        if qm:
            qty = float(qm.group(1))
            current["ordered"] = int(qty)
            current["shipped"] = int(qty)
            current["balance"] = 0
            current["unit_price"] = float(qm.group(2))
            current["line_total"] = float(qm.group(3))
            continue

    if current:
        items.append(current)

    if debug:
        print(f"[ARDUINO] parsed {len(items)} line items")

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


def _money_after(label: str, text: str) -> Optional[float]:
    m = re.search(label + r".*?\$\s*([0-9]+\.[0-9]{2})", text, re.I)
    if not m:
        return None
    return float(m.group(1))
