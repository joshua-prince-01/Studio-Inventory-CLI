from __future__ import annotations

import re
from typing import Optional

import pdfplumber


# -------------------------------------------------
# Detection
# -------------------------------------------------

def detect(pdf_path: str) -> bool:
    with pdfplumber.open(pdf_path) as pdf:
        txt = (pdf.pages[0].extract_text() or "").upper()
    return ("DIGI-KEY ELECTRONICS" in txt) or ("DIGIKEY" in txt and "PO ACKNOWLEDGEMENT" in txt)


# -------------------------------------------------
# Order-level parsing
# -------------------------------------------------

def parse_order(pdf_path: str, debug: bool = False) -> dict:
    text = _all_text(pdf_path)

    po_ack = _find(r"PO\s*Acknowledgement\s*([0-9]+)", text)
    web_id = _find(r"WEB\s*ORDER\s*ID:\s*([0-9]+)", text)

    # "Order Date:" sometimes exists; otherwise the header uses e.g. 01-SEP-2025
    order_date = _find(r"Order\s*Date:\s*([0-9A-Z\-]+)", text)
    if not order_date:
        order_date = _find(r"\b([0-9]{2}-[A-Z]{3}-[0-9]{4})\b", text)

    sales = _money_after("Sales Amount", text)
    shipping = _money_after("Shipping charges applied", text)
    tax = _money_after("Sales Tax", text)
    total = _money_after("Total", text)

    if debug:
        print(f"[DIGIKEY] invoice(po_ack)={po_ack} web_order_id={web_id} date={order_date} sales={sales} ship={shipping} tax={tax} total={total}")

    return {
        "vendor": "digikey",
        "invoice": po_ack,
        "purchase_order": web_id,
        "invoice_date": order_date,
        "account_number": None,
        "payment_date": None,
        "credit_card": None,
        "merchandise": sales,
        "shipping": shipping,
        "sales_tax": tax,
        "total": total,
    }


# -------------------------------------------------
# Line-item parsing
# -------------------------------------------------

# Example extracted line:
#   1 2 2 0 PART: 1528-6066-ND DESC: ADAFRUIT PIXEL SHIFTER - FOR ADD 4.50000 9.00
PART_LINE_RE = re.compile(
    r"^(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+PART:\s*([A-Z0-9\-]+)\s+DESC:\s*(.+?)\s+(\d+\.\d{2,5})\s+(\d+\.\d{2,5})\s*$",
    re.I,
)

# Example:
#   MFG : ADAFRUIT INDUSTRIES LLC / 6066
MFG_RE = re.compile(r"^MFG\s*:\s*(.+?)\s*/\s*([A-Z0-9\-\.\_]+)\s*$", re.I)

# Example:
#   COO : HONG KONG ECCN: EAR99 HTSUS: 8543.70.9860
COO_RE = re.compile(r"^COO\s*:\s*(.+?)(?:\s+ECCN:|\s+HTSUS:|$)", re.I)


def parse_line_items(pdf_path: str, debug: bool = False) -> list[dict]:
    lines = _all_text(pdf_path).splitlines()

    items: list[dict] = []
    current: Optional[dict] = None

    # DigiKey PDFs can duplicate blocks across pages during text extraction;
    # dedupe using a stable key.
    seen: set[tuple] = set()

    def flush():
        nonlocal current
        if not current:
            return
        key = (
            current.get("line"),
            current.get("sku"),
            current.get("ordered"),
            current.get("shipped"),
            current.get("unit_price"),
            current.get("line_total"),
        )
        if key not in seen:
            seen.add(key)
            items.append(current)
        current = None

    for raw in lines:
        ln = raw.strip()
        if not ln:
            continue

        if ln.startswith("Sales Amount") or ln.startswith("Shipping charges applied") or ln.startswith("Total"):
            break

        m = PART_LINE_RE.match(ln)
        if m:
            flush()
            current = {
                "line": int(m.group(1)),
                "ordered": int(m.group(2)),
                "shipped": int(m.group(3)),
                "balance": int(m.group(4)),
                "sku": m.group(5).strip(),
                "part": m.group(5).strip(),
                "description": m.group(6).strip(),   # ✅ DESC -> description
                "unit_price": float(m.group(7)),
                "line_total": float(m.group(8)),
                "mfg": None,
                "mfg_pn": None,
                "coo": None,
            }
            continue

        if current is None:
            continue

        mm = MFG_RE.match(ln)
        if mm:
            current["mfg"] = mm.group(1).strip()
            current["mfg_pn"] = mm.group(2).strip()
            continue

        cm = COO_RE.match(ln)
        if cm:
            current["coo"] = cm.group(1).strip()
            continue

    flush()

    if debug:
        print(f"[DIGIKEY] parsed {len(items)} line items")

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
    m = re.search(label + r"\s*([0-9]+(?:,[0-9]{3})*\.[0-9]{2})", text, re.I)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))
