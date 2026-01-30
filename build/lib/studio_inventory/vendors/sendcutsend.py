from __future__ import annotations

import re
from typing import Optional, List, Dict, Any

import pdfplumber


# -------------------------------------------------
# Detection
# -------------------------------------------------

def detect(pdf_path: str) -> bool:
    """
    SendCutSend invoices typically include:
      - support@sendcutsend.com
      - sendcutsend.com
      - "Invoice" header with an order id like SC93C716
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            txt = (pdf.pages[0].extract_text() or "").lower()
        return ("sendcutsend" in txt) or ("support@sendcutsend.com" in txt)
    except Exception:
        return False


# -------------------------------------------------
# Order-level parsing
# -------------------------------------------------

def parse_order(pdf_path: str, debug: bool = False) -> Dict[str, Any]:
    text = _all_text(pdf_path)
    # Normalize odd glyph placeholders (\x00) seen in some PDFs
    text = re.sub(r"\x00(?=\d)", "(", text)
    text = text.replace("\x00", " ")

    invoice = _find(r"\b(S[A-Z0-9]{7})\b", text)  # e.g., SC93C716, SZ47Z879, SW194224, SV74V197

    # Dates come in two common formats:
    #   "Invoice Date: Aug 25, 2025"
    #   "May 6, 2025 6:12 PM"
    invoice_date = _find(r"Invoice\s*Date:\s*([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})", text)
    if not invoice_date:
        invoice_date = _find(r"\b([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4}(?:\s+\d{1,2}:\d{2}\s*[AP]M)?)\b", text)

    subtotal = _money_after(r"Subtotal:\s*", text)
    shipping = _money_or_free_after(r"Shipping\s*(?:\+|and)?\s*Handling:\s*", text)
    tax = _money_after(r"Tax:\s*", text)
    total = _money_after(r"(?<!Item\s)\bTotal:\s*", text)

    credit_card = None
    cc = re.search(r"\b(MasterCard|Visa|Discover|American\s*Express|AmEx)\s*\(x(\d{4})", text, re.I)
    if cc:
        brand = cc.group(1).strip()
        brand = re.sub(r"\s+", " ", brand)
        credit_card = f"{brand} (x{cc.group(2)})"

    if debug:
        print(f"[SENDCUTSEND] invoice={invoice} date={invoice_date} sub={subtotal} ship={shipping} tax={tax} total={total} cc={credit_card}")

    return {
        "vendor": "sendcutsend",
        "invoice": invoice,
        "purchase_order": None,
        "invoice_date": invoice_date,
        "account_number": None,
        "payment_date": None,
        "credit_card": credit_card,
        "merchandise": subtotal,
        "shipping": shipping,
        "sales_tax": tax,
        "total": total,
    }


# -------------------------------------------------
# Line-item parsing
# -------------------------------------------------

_FILENAME_EXT_RE = re.compile(r"\.(step|stp|dxf|dwg|iges|igs|sldprt|sldasm|pdf)\b", re.I)
_ITEM_TOTAL_RE = re.compile(r"Item\s*total:\s*\$?\s*([0-9]+(?:,[0-9]{3})*\.[0-9]{2})", re.I)
_QTY_RE = re.compile(r"\bQty:\s*(\d+)\b", re.I)

# Operation keywords (optional; we keep them in description if present)
_OP_KWS = ("Bending", "Tapping", "Deburring", "Countersink", "Welding", "Forming", "Powder", "Anodize", "Finish")


def parse_line_items(pdf_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    """
    Extracts items that look like:

      <material line ... Qty: N>
      <dimensions>
      <line number>
      <filename possibly wrapped across lines>
      Item total: $XX.XX

    The extracted "description" is three lines:
      material
      dimensions
      filename

    Your global label logic then detects the filename and sets:
      label_line1 = filename
      label_line2 = material — dimensions
    """
    raw_lines = _all_text(pdf_path).splitlines()

    # Normalize odd glyph placeholders:
    #   \x00 often shows up where "(" or "+" should have been.
    #   - if followed by a digit => "("
    #   - otherwise => " "
    lines: List[str] = []
    for ln in raw_lines:
        s = ln.rstrip("\n")
        s = re.sub(r"\x00(?=\d)", "(", s)
        s = s.replace("\x00", " ")
        s = re.sub(r"\s+", " ", s).strip()
        if s:
            lines.append(s)

    # Start scanning after the "Line" header if present
    start_idx = 0
    for i, ln in enumerate(lines):
        if ln.strip().lower() == "line":
            start_idx = i + 1
            break

    items: List[Dict[str, Any]] = []
    i = start_idx

    while i < len(lines):
        ln = lines[i]

        # Stop at footer-ish sections
        if ln.lower().startswith("ship to:") or ln.lower().startswith("bill to:") or ln.lower().startswith("subtotal:"):
            break

        # MATERIAL line should contain thickness and often "Qty:"
        material_line = ln
        # If we hit something that is clearly not material, advance
        if material_line.isdigit() or material_line.lower().startswith("invoice"):
            i += 1
            continue

        # Expect next line to be dimensions (often has "x" and a unit)
        if i + 1 >= len(lines):
            break
        dims_line = lines[i + 1]
        i += 2

        # Next should be line number
        if i >= len(lines):
            break
        line_no = None
        if lines[i].isdigit():
            line_no = int(lines[i])
            i += 1
        else:
            # occasionally extraction swaps order; try to find a line number soon
            for j in range(i, min(i + 3, len(lines))):
                if lines[j].isdigit():
                    line_no = int(lines[j])
                    i = j + 1
                    break

        # Collect filename lines until we see "Item total:"
        filename_parts: List[str] = []
        while i < len(lines):
            t = lines[i]
            if _ITEM_TOTAL_RE.search(t):
                break
            # If we hit start of next item (material line) prematurely, bail
            if t.lower().startswith("ship to:") or t.lower().startswith("subtotal:"):
                break
            filename_parts.append(t)
            i += 1

        # Parse item total
        line_total = None
        if i < len(lines):
            m = _ITEM_TOTAL_RE.search(lines[i])
            if m:
                line_total = float(m.group(1).replace(",", ""))
                i += 1  # consume item total line

        # Quantity can appear on material line or anywhere in the filename block
        qty = None
        mqty = _QTY_RE.search(material_line)
        if mqty:
            qty = int(mqty.group(1))
        else:
            for t in filename_parts:
                mm = _QTY_RE.search(t)
                if mm:
                    qty = int(mm.group(1))
                    break
        if qty is None:
            qty = 1

        # Pull a CAD filename from the collected text (handles wrapped lines)
        joined = "".join(filename_parts)
        fname = None
        fm = re.search(r"([A-Za-z0-9][A-Za-z0-9_\-\.]{0,240}" + _FILENAME_EXT_RE.pattern + r")", joined, re.I)
        if fm:
            # fm.group(0) includes extension
            fname = fm.group(1)
        else:
            # fallback: if any line itself looks like a filename
            for t in filename_parts:
                if _FILENAME_EXT_RE.search(t):
                    fname = t.strip()
                    break

        # Clean material line: remove ops and qty from material
        material_clean = re.sub(r"\bQty:\s*\d+\b", "", material_line, flags=re.I).strip()
        # Strip trailing operation descriptors to keep material readable
        material_clean = re.sub(r"\b(" + "|".join(_OP_KWS) + r")\b.*$", "", material_clean, flags=re.I).strip(" -;,")

        # Collect ops (optional) from material line + dims line (they sometimes include Deburring)
        ops = []
        for kw in _OP_KWS:
            if re.search(r"\b" + re.escape(kw) + r"\b", material_line, re.I) or re.search(r"\b" + re.escape(kw) + r"\b", dims_line, re.I):
                ops.append(kw)
        ops_txt = "; ".join(dict.fromkeys(ops))  # preserve order, dedupe

        # SKU: use filename if we have one, else a stable fallback
        sku = fname or f"sendcutsend_line_{line_no or len(items)+1}"

        # Description: multi-line for label logic
        # Keep the CAD filename as the LAST line so global label rules can pick it as label_line1.
        desc_lines = [material_clean, dims_line]
        if ops_txt:
            desc_lines.append(f"Ops: {ops_txt}")
        if fname:
            desc_lines.append(fname)
        description = "\n".join([d for d in desc_lines if d])

        unit_price = (line_total / qty) if (line_total is not None and qty) else None

        items.append({
            "line": line_no,
            "sku": sku,
            "part": sku,
            "description": description,
            "ordered": qty,
            "shipped": qty,
            "balance": 0,
            "unit_price": unit_price,
            "line_total": line_total,
            "mfg": None,
            "mfg_pn": None,
            "coo": None,
        })

    if debug:
        print(f"[SENDCUTSEND] parsed {len(items)} line items")

    return items


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def _all_text(pdf_path: str) -> str:
    with pdfplumber.open(pdf_path) as pdf:
        return "\n".join((page.extract_text() or "") for page in pdf.pages)


def _find(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text, re.I)
    return m.group(1).strip() if m else None


def _money_after(prefix_pattern: str, text: str) -> Optional[float]:
    """
    Find a money value after a label, where the label may appear on the same line
    as other labels (SendCutSend compacts the footer).
    """
    m = re.search(prefix_pattern + r"\$?\s*([0-9]+(?:,[0-9]{3})*\.[0-9]{2})", text, re.I)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def _money_or_free_after(prefix_pattern: str, text: str) -> Optional[float]:
    m = re.search(prefix_pattern + r"(FREE|\$?\s*[0-9]+(?:,[0-9]{3})*\.[0-9]{2})", text, re.I)
    if not m:
        return None
    val = m.group(1).strip()
    if val.upper().startswith("FREE"):
        return 0.0
    val = val.replace("$", "").replace(",", "").strip()
    try:
        return float(val)
    except ValueError:
        return None
