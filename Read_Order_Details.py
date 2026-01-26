import re
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional, Union

import pdfplumber


@dataclass
class OrderInfo:
    purchase_order: Optional[str] = None
    invoice: Optional[Union[int, str]] = None
    invoice_date: Optional[datetime] = None
    account_number: Optional[str] = None

    payment_date: Optional[datetime] = None
    credit_card: Optional[str] = None  # e.g. "Amex ****2008"

    merchandise: Optional[float] = None
    shipping: Optional[float] = None
    sales_tax: Optional[float] = None
    total: Optional[float] = None


_MONEY = re.compile(r"\$?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]{2})?)")


def normalize_text(txt: str) -> str:
    txt = (txt or "").replace("\r", "\n")
    txt = re.sub(r"[ \t]+", " ", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()


def money_to_float(s: str) -> Optional[float]:
    if not s:
        return None
    m = _MONEY.search(s)
    if not m:
        return None
    return float(m.group(1).replace(",", ""))


def parse_mmddyy(s: str) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    return None


def extract_first(pattern: str, text: str, flags=re.I) -> Optional[str]:
    m = re.search(pattern, text, flags)
    return m.group(1).strip() if m else None


def extract_invoice(text: str) -> Optional[Union[int, str]]:
    raw = extract_first(r"\bInvoice\b\s*([A-Z0-9\-]+)\b", text)
    if raw is None:
        return None
    return int(raw) if raw.isdigit() else raw


def extract_purchase_order(text: str) -> Optional[str]:
    return extract_first(r"\bPurchase\s+Order\b\s*([A-Z0-9\-]+)\b", text)


def extract_invoice_date(text: str) -> Optional[datetime]:
    raw = extract_first(r"\bInvoice\s+Date\b\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})\b", text)
    return parse_mmddyy(raw) if raw else None


def extract_account_number(text: str) -> Optional[str]:
    return extract_first(r"\bYour\s+Account\b\s*([A-Z0-9\-]+)\b", text)


def extract_credit_card(text: str) -> Optional[str]:
    # Matches: "Credit Card Amex Ending- 2008"
    m = re.search(r"\bCredit\s+Card\s+([A-Za-z]+)\s+Ending-\s*([0-9]{4})\b", text, re.I)
    if m:
        brand = m.group(1).strip().title()
        last4 = m.group(2)
        return f"{brand} ****{last4}"
    return None


def extract_payment_date(text: str) -> Optional[datetime]:
    # Prefer: "Payment Received 11/11/25 (146.41)"
    m = re.search(r"\bPayment\s+Received\b\s+([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})\b", text, re.I)
    if m:
        return parse_mmddyy(m.group(1))

    # Fallback: within payment-info block
    block_idx = text.lower().find("information about your payment")
    if block_idx != -1:
        window = text[block_idx:block_idx + 500]
        m2 = re.search(r"\bDate\b\s*([0-9]{1,2}/[0-9]{1,2}/[0-9]{2,4})\b", window, re.I)
        if m2:
            return parse_mmddyy(m2.group(1))

    return None


def extract_totals(text: str):
    merch = ship = tax = total = None
    for line in text.splitlines():
        low = line.strip().lower()

        if merch is None and low.startswith("merchandise"):
            merch = money_to_float(line)

        elif ship is None and (low.startswith("shipping") or low.startswith("freight")):
            ship = money_to_float(line)

        elif tax is None and ("sales tax" in low):
            val = money_to_float(line)
            if val is not None:
                tax = val

        elif total is None and low.startswith("total"):
            total = money_to_float(line)

    return merch, ship, tax, total


def is_complete(info: OrderInfo) -> bool:
    # your required set (sales_tax might legitimately be None)
    return all([
        info.purchase_order is not None,
        info.invoice is not None,
        info.invoice_date is not None,
        info.account_number is not None,
        info.payment_date is not None,
        info.credit_card is not None,
        info.shipping is not None,
        info.merchandise is not None,
        info.total is not None,
    ])


def merge_if_missing(info: OrderInfo, **kwargs):
    """
    Fill fields only if they are currently None.
    """
    for k, v in kwargs.items():
        if getattr(info, k) is None and v is not None:
            setattr(info, k, v)


def extract_order_info_by_page(pdf_path: str, debug: bool = False) -> OrderInfo:
    """
    Scan each page independently and fill fields as they are found.
    Stops early when all "required" fields are present.
    """
    info = OrderInfo()

    with pdfplumber.open(pdf_path) as p:
        for i, page in enumerate(p.pages):
            text = normalize_text(page.extract_text() or "")
            if not text:
                continue

            # Pull candidates from THIS page
            po = extract_purchase_order(text)
            inv = extract_invoice(text)
            inv_date = extract_invoice_date(text)
            acct = extract_account_number(text)
            cc = extract_credit_card(text)
            pay_date = extract_payment_date(text)
            merch, ship, tax, total = extract_totals(text)

            # Merge into the global info (only fill missing fields)
            merge_if_missing(
                info,
                purchase_order=po,
                invoice=inv,
                invoice_date=inv_date,
                account_number=acct,
                credit_card=cc,
                payment_date=pay_date,
                merchandise=merch,
                shipping=ship,
                sales_tax=tax,   # keep None if not present
                total=total,
            )

            if debug:
                print(f"page {i}: filled -> "
                      f"po={po is not None}, inv={inv is not None}, inv_date={inv_date is not None}, "
                      f"acct={acct is not None}, cc={cc is not None}, pay_date={pay_date is not None}, "
                      f"merch={merch is not None}, ship={ship is not None}, tax={tax is not None}, total={total is not None}")

            if is_complete(info):
                break

    return info


if __name__ == "__main__":
    import os
    pdf_path = os.path.expanduser(
        "~/PycharmProjects/PythonProject_studio_inventory/McMaster_Items/receipts/Receipt 55152414.PDF"
    )
    info = extract_order_info_by_page(pdf_path, debug=True)


    print("\n--- ORDER INFO ---")
    for k, v in asdict(info).items():
        print(f"{k}: {v}")