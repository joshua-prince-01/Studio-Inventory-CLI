from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, Optional, List, Dict, Any


@dataclass
class ParsedOrder:
    vendor: str
    invoice: Optional[str] = None
    purchase_order: Optional[str] = None
    invoice_date: Optional[str] = None  # keep as string; caller may parse
    account_number: Optional[str] = None
    payment_date: Optional[str] = None
    credit_card: Optional[str] = None
    merchandise: Optional[float] = None
    shipping: Optional[float] = None
    sales_tax: Optional[float] = None
    total: Optional[float] = None


@dataclass
class ParsedLineItem:
    line: Optional[int]
    sku: Optional[str]
    description: Optional[str]
    ordered: Optional[int]
    shipped: Optional[int]
    balance: Optional[int]
    unit_price: Optional[float]
    line_total: Optional[float]


class VendorParser(Protocol):
    def detect(self, pdf_path: str) -> bool: ...
    def parse_order(self, pdf_path: str, debug: bool = False) -> Dict[str, Any]: ...
    def parse_line_items(self, pdf_path: str, debug: bool = False) -> List[Dict[str, Any]]: ...
