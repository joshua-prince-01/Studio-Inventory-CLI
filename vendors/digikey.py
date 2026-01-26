# vendors/digikey.py
from __future__ import annotations

import re
from typing import Optional, List, Dict, Any
import pdfplumber


# ----------------------------
# Detection
# ----------------------------

def detect(pdf_path: str) -> bool:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            t0 = (pdf.pages[0].extract_text() or "").upper()
        return ("DIGI-KEY ELECTRONICS" in t0) or ("WWW.DIGIKEY.COM" in t0) or ("DIGIKEY" in t0)
    except Exception:
        return False


def _all_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def _find_first(pattern: re.Pattern, text: str) -> Optional[str]:
    m = pattern.search(text)
    return m.group(1).strip() if m else None


def _find_money_after_label(text: str, label: str) -> Optional[float]:
    """
    Works for BOTH of these DigiKey styles:
      - 'Sales Tax 7.84'
      - 'Sales Amount' on one line, then '130.59' on next line
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    target = label.lower()

    for i, ln in enumerate(lines):
        if ln.lower().startswith(target):
            # same-line number
            m = re.search(r"(\d+\.\d{2})", ln)
            if m:
                return float(m.group(1))

            # next-line number-only
            if i + 1 < len(lines) and re.fullmatch(r"\d+\.\d{2}", lines[i + 1]):
                return float(lines[i + 1])

    return None


# ----------------------------
# Regexes (TOLERANT)
# ----------------------------

# Accept both "Acknowledgement" and "Acknowledgment", optional punctuation, odd whitespace
PO_ACK_RE = re.compile(r"PO\s+Acknowledg(?:e)?ment[:\s]+(\d+)", re.I)

WEB_ID_RE = re.compile(r"WEB\s+ORDER\s+ID[:\s]+(\d+)", re.I)

ORDER_DATE_RE = re.compile(r"Order\s+Date[:\s]+([0-9]{2}-[A-Z]{3}-[0-9]{4})", re.I)

# IMPORTANT: do NOT anchor to end-of-line; allow trailing spaces/junk
PART_LINE_RE = re.compile(
    r"\b(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+PART:\s*([A-Z0-9\-]+)\b",
    re.I
)

# DESC line ends with: "<unit_price> <amount>"
DESC_TAIL_RE = re.compile(r"^(.*)\s+(\d+\.\d{5})\s+(\d+\.\d{2})\s*$")


# ----------------------------
# Parse order
# ----------------------------

def parse_order(pdf_path: str, debug: bool = False) -> Dict[str, Any]:
    text = _all_text(pdf_path)

    po_ack = _find_first(PO_ACK_RE, text)
    web_id = _find_first(WEB_ID_RE, text)
    order_date = _find_first(ORDER_DATE_RE, text)

    sales = _find_money_after_label(text, "Sales Amount")
    shipping = _find_money_after_label(text, "Shipping charges applied")
    tax = _find_money_after_label(text, "Sales Tax")
    total = _find_money_after_label(text, "Total")

    if debug:
        print(f"[DIGIKEY] invoice(po_ack)={po_ack} web_order_id={web_id} date={order_date} "
              f"sales={sales} ship={shipping} tax={tax} total={total}")

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


# ----------------------------
# Parse line items
# ----------------------------

def parse_line_items(pdf_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    text = _all_text(pdf_path)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    items: List[Dict[str, Any]] = []
    current: Optional[Dict[str, Any]] = None

    for ln in lines:
        # Start of an item
        m = PART_LINE_RE.search(ln)
        if m:
            if current:
                items.append(current)

            current = {
                "line": int(m.group(1)),
                "ordered": int(m.group(2)),
                "shipped": int(m.group(3)),
                "balance": int(m.group(4)),
                "part": m.group(5),        # DigiKey part number (e.g., 1528-6066-ND)
                "sku": m.group(5),         # keep sku as same for now
                "mfg": None,               # manufacturer name
                "mfg_pn": None,            # manufacturer part number
                "coo": None,               # country of origin
                "description": "",
                "unit_price": None,
                "line_total": None,
            }
            continue

        if current is None:
            continue

        # Manufacturer line
        if ln.startswith("MFG"):
            # Example: "MFG : ADAFRUIT INDUSTRIES LLC / 6066"
            rhs = ln.split(":", 1)[-1].strip() if ":" in ln else ln.replace("MFG", "").strip()
            if "/" in rhs:
                left, right = rhs.split("/", 1)
                current["mfg"] = left.strip()
                current["mfg_pn"] = right.strip()
            else:
                current["mfg"] = rhs.strip()
            continue

        # Country of origin line
        if ln.startswith("COO"):
            # Example: "COO : HONG KONG ECCN: EAR99 HTSUS: ..."
            rhs = ln.split(":", 1)[-1].strip() if ":" in ln else ln.replace("COO", "").strip()
            # Stop COO at first known following token
            for stop in [" ECCN", " HTSUS", " ROHS", " REACH"]:
                idx = rhs.upper().find(stop.strip())
                if idx != -1:
                    rhs = rhs[:idx].strip()
                    break
            current["coo"] = rhs.strip()
            continue

        # Description line (DESC: ... unit ext)
        if ln.startswith("DESC:"):
            desc_body = ln.replace("DESC:", "", 1).strip()
            mt = DESC_TAIL_RE.match(desc_body)
            if mt:
                current["description"] = mt.group(1).strip()
                current["unit_price"] = float(mt.group(2))
                current["line_total"] = float(mt.group(3))
            else:
                current["description"] = desc_body
            continue

    if current:
        items.append(current)

    if debug:
        print(f"[DIGIKEY] parsed {len(items)} line items")

    return items
