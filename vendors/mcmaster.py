from __future__ import annotations

from typing import List, Dict, Any
import pdfplumber

from Read_Order_Details import extract_order_info_by_page
from Read_Line_Items import parse_receipt


def detect(pdf_path: str) -> bool:
    try:
        with pdfplumber.open(pdf_path) as pdf:
            t0 = (pdf.pages[0].extract_text() or "").lower()
        # cheap but effective
        return ("mcmaster" in t0) or ("mcmaster.com" in t0)
    except Exception:
        return False


def parse_order(pdf_path: str, debug: bool = False) -> Dict[str, Any]:
    info = extract_order_info_by_page(pdf_path, debug=debug)
    return {
        "vendor": "mcmaster",
        "invoice": str(info.invoice) if info.invoice is not None else None,
        "purchase_order": info.purchase_order,
        "invoice_date": info.invoice_date.isoformat() if getattr(info, "invoice_date", None) else None,
        "account_number": info.account_number,
        "payment_date": info.payment_date.isoformat() if getattr(info, "payment_date", None) else None,
        "credit_card": info.credit_card,
        "merchandise": info.merchandise,
        "shipping": info.shipping,
        "sales_tax": info.sales_tax,
        "total": info.total,
    }


def parse_line_items(pdf_path: str, debug: bool = False) -> List[Dict[str, Any]]:
    items = parse_receipt(pdf_path, page_num=0, debug=debug) or []
    out: List[Dict[str, Any]] = []
    for d in items:
        out.append({
            "line": d.get("line"),
            "sku": d.get("sku"),
            "description": d.get("description"),
            "ordered": d.get("ordered"),
            "shipped": d.get("shipped"),
            "balance": d.get("balance"),
            "unit_price": d.get("price"),
            "line_total": d.get("total"),
        })
    return out
