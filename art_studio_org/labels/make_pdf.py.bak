from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from reportlab.pdfgen import canvas
from reportlab.lib.units import inch

from reportlab.graphics.barcode import qr
from reportlab.graphics.shapes import Drawing
from reportlab.graphics import renderPDF


@dataclass
class LabelTemplate:
    name: str
    page_w: float
    page_h: float
    label_w: float
    label_h: float
    cols: int
    rows: int
    margin_left: float
    margin_top: float
    pitch_x: float
    pitch_y: float
    pad_x: float
    pad_y: float
    font_name: str
    font_size: int

    @classmethod
    def from_json(cls, path: Path) -> "LabelTemplate":
        d = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            name=d["name"],
            page_w=d["page"]["width_in"] * inch,
            page_h=d["page"]["height_in"] * inch,
            label_w=d["label"]["width_in"] * inch,
            label_h=d["label"]["height_in"] * inch,
            cols=int(d["grid"]["cols"]),
            rows=int(d["grid"]["rows"]),
            margin_left=d["margins_in"]["left"] * inch,
            margin_top=d["margins_in"]["top"] * inch,
            pitch_x=d["pitch_in"]["x"] * inch,
            pitch_y=d["pitch_in"]["y"] * inch,
            pad_x=d["padding_in"]["x"] * inch,
            pad_y=d["padding_in"]["y"] * inch,
            font_name=d["font"]["name"],
            font_size=int(d["font"]["size"]),
        )


def _label_xy(t: LabelTemplate, index0: int) -> tuple[float, float]:
    """
    index0: 0-based within a page, left-to-right then top-to-bottom.
    Returns bottom-left origin (x, y) for the label.
    """
    col = index0 % t.cols
    row = index0 // t.cols
    x = t.margin_left + col * t.pitch_x
    # y measured from bottom; top margin defines first row top edge
    top_y = t.page_h - t.margin_top - row * t.pitch_y
    y = top_y - t.label_h
    return x, y


def _draw_qr(c: canvas.Canvas, x: float, y: float, size: float, text: str) -> None:
    code = qr.QrCodeWidget(text)
    bounds = code.getBounds()
    w = bounds[2] - bounds[0]
    h = bounds[3] - bounds[1]
    d = Drawing(size, size, transform=[size / w, 0, 0, size / h, 0, 0])
    d.add(code)
    renderPDF.draw(d, c, x, y)


def make_labels_pdf(
    *,
    template_path: Path,
    out_pdf: Path,
    rows: list[dict],
    start_pos: int = 1,
    include_qr: bool = False,
) -> None:
    """
    rows: list of dicts with keys like:
      label_line1, label_line2, label_short, purchase_url, label_qr_text, part_key
    start_pos: 1-based label position on sheet (Avery style). 1 = first label top-left.
    """
    t = LabelTemplate.from_json(template_path)
    out_pdf.parent.mkdir(parents=True, exist_ok=True)

    c = canvas.Canvas(str(out_pdf), pagesize=(t.page_w, t.page_h))
    c.setFont(t.font_name, t.font_size)

    per_page = t.cols * t.rows
    pos = max(1, int(start_pos)) - 1  # 0-based

    for item in rows:
        page_pos = pos % per_page
        if page_pos == 0 and pos != 0:
            c.showPage()
            c.setFont(t.font_name, t.font_size)

        x0, y0 = _label_xy(t, page_pos)
        # DEBUG: outline label box
        c.rect(x0, y0, t.label_w, t.label_h, stroke=1, fill=0)

        # padding box
        x = x0 + t.pad_x
        y = y0 + t.pad_y
        w = t.label_w - 2 * t.pad_x
        h = t.label_h - 2 * t.pad_y

        # Choose what to print (simple + reliable)
        line1 = (item.get("label_line1") or item.get("label_short") or item.get("part_key") or "").strip()
        line2 = (item.get("label_line2") or f'{item.get("vendor", "")}:{item.get("sku", "")}' or "").strip()

        # Do NOT print URL text line at all
        line3 = ""

        qr_text = (item.get("label_qr_text") or item.get("purchase_url") or item.get("part_key") or "").strip()

        # Layout: 2–3 lines left, optional QR right
        text_left_w = w
        qr_size = 0.0
        if include_qr and qr_text:
            qr_size = min(h, w * 0.45)
            text_left_w = w - qr_size - (0.06 * inch)

        # Write lines (top-down)
        cur_y = y + h - t.font_size
        for s in [line1, line2]:
            if s:
                c.drawString(x, cur_y, s[:22])
                cur_y -= (t.font_size + 1)

        if line3:
            # smaller for URL-ish
            c.setFont(t.font_name, max(6, t.font_size - 1))
            c.drawString(x, max(y, cur_y), line3[:70])
            c.setFont(t.font_name, t.font_size)

        if include_qr and qr_text:
            qr_x = x0 + t.label_w - t.pad_x - qr_size
            qr_y = y + (h - qr_size) / 2
            _draw_qr(c, qr_x, qr_y, qr_size, qr_text)

        pos += 1

    c.save()
