from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.pdfbase import pdfmetrics

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
    layout: Optional[dict] = None,
    draw_boxes: bool = False,
) -> None:
    """
    Generate label sheets.
    - rows: list of dicts with keys like: vendor, sku, label_line1, label_short, purchase_url, label_qr_text, part_key
    - start_pos: 1-based label position on sheet; 1 = first label top-left.
    - layout: optional layout preset dict (elements + qr settings)
    - draw_boxes: if True, outlines each label (calibration/debug)
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
        if draw_boxes:
            c.rect(x0, y0, t.label_w, t.label_h, stroke=1, fill=0)

        # padding box
        x = x0 + t.pad_x
        y = y0 + t.pad_y
        w = t.label_w - 2 * t.pad_x
        h = t.label_h - 2 * t.pad_y

        if layout:
            _render_layout(c, item, x, y, w, h, t, layout)
        else:
            # fallback simple layout
            line1 = (item.get("label_line1") or item.get("label_short") or item.get("part_key") or "").strip()
            line2 = (item.get("label_line2") or f'{item.get("vendor", "")}:{item.get("sku", "")}' or "").strip()
            qr_text = (item.get("label_qr_text") or item.get("purchase_url") or item.get("part_key") or "").strip()

            cur_y = y + h - t.font_size
            for s in [line1, line2]:
                if s:
                    c.drawString(x, cur_y, _truncate_to_width(s, w, t.font_name, t.font_size))
                    cur_y -= (t.font_size + 1)

            if include_qr and qr_text:
                qr_size = min(h, w * 0.45)
                qr_x = x0 + t.label_w - t.pad_x - qr_size
                qr_y = y + (h - qr_size) / 2
                _draw_qr(c, qr_x, qr_y, qr_size, qr_text)

        pos += 1

    c.save()


def _font_for_style(base_font: str, style: str) -> str:
    style = (style or "normal").lower()
    if base_font.lower().startswith("helvetica"):
        return {
            "normal": "Helvetica",
            "bold": "Helvetica-Bold",
            "italic": "Helvetica-Oblique",
            "bolditalic": "Helvetica-BoldOblique",
        }.get(style, "Helvetica")
    return base_font


def _source_value(item: dict, source: str) -> str:
    source = (source or "").strip()
    if source == "vendor_sku":
        v = (item.get("vendor") or "").strip()
        s = (item.get("sku") or "").strip()
        if v and s:
            return f"{v}:{s}"
        return s or v
    if source in ("on_hand", "avg_unit_cost"):
        val = item.get(source, "")
        return "" if val is None else str(val)
    val = item.get(source, "")
    return "" if val is None else str(val)


def _truncate_to_width(text: str, max_width: float, font: str, size: int) -> str:
    if not text:
        return ""
    if pdfmetrics.stringWidth(text, font, size) <= max_width:
        return text
    ell = "…"
    t = text
    while t and pdfmetrics.stringWidth(t + ell, font, size) > max_width:
        t = t[:-1]
    return (t + ell) if t else ""


def _wrap_lines(text: str, max_width: float, font: str, size: int, max_lines: int) -> list[str]:
    if not text:
        return []
    words = text.split()
    lines: list[str] = []
    cur = ""
    for w in words:
        test = (cur + " " + w).strip()
        if pdfmetrics.stringWidth(test, font, size) <= max_width or not cur:
            cur = test
        else:
            lines.append(cur)
            cur = w
            if len(lines) >= max_lines:
                break
    if len(lines) < max_lines and cur:
        lines.append(cur)
    if lines:
        lines[-1] = _truncate_to_width(lines[-1], max_width, font, size)
    return lines[:max_lines]


def _cell_box(pos: str, x: float, y: float, w: float, h: float, span_x: int = 1, span_y: int = 1) -> tuple[float, float, float, float, str]:
    """
    Divide the padded label content box (x,y,w,h) into a 3x3 grid.
    pos like UL/UC/UR/ML/MC/MR/LL/LC/LR selects the anchor cell.

    span_x spans columns (1–3), span_y spans rows (1–3). Spans are clamped to fit.

    Anchoring rules:
      - L anchors span to the right
      - R anchors span to the left
      - C anchors span centered as best as possible (for span=2 => cols 1–2)
      - U anchors span downward
      - L anchors span upward
      - M anchors span centered as best as possible (for span=2 => rows 1–2)

    Returns (cx, cy, cw, ch, row) where (cx,cy) is bottom-left of the spanned box.
    """
    pos = (pos or "UL").upper()
    row = pos[0] if len(pos) >= 1 else "U"
    col = pos[1] if len(pos) >= 2 else "L"

    cell_w = w / 3.0
    cell_h = h / 3.0

    span_x = max(1, min(3, int(span_x)))
    span_y = max(1, min(3, int(span_y)))

    # Column start index (0..2)
    if col == "L":
        start_c = 0
    elif col == "R":
        start_c = 3 - span_x
    else:  # C
        start_c = 1 - ((span_x - 1) // 2)
    start_c = max(0, min(3 - span_x, start_c))

    # Row start index (0..2), where 0 is bottom, 2 is top
    if row == "L":
        start_r = 0
    elif row == "U":
        start_r = 3 - span_y
    else:  # M
        start_r = 1 - ((span_y - 1) // 2)
    start_r = max(0, min(3 - span_y, start_r))

    cx = x + start_c * cell_w
    cy = y + start_r * cell_h
    cw = cell_w * span_x
    ch = cell_h * span_y

    return cx, cy, cw, ch, row



def _draw_aligned(c: canvas.Canvas, align: str, x: float, y: float, text: str) -> None:
    align = (align or "left").lower()
    if align == "center":
        c.drawCentredString(x, y, text)
    elif align == "right":
        c.drawRightString(x, y, text)
    else:
        c.drawString(x, y, text)


def _render_layout(c: canvas.Canvas, item: dict, x: float, y: float, w: float, h: float, t: LabelTemplate, layout: dict) -> None:
    elems = layout.get("elements", []) or []
    qr_cfg = layout.get("qr", {}) or {}
    qr_enabled = bool(qr_cfg.get("enabled", False))

    for e in elems:
        source = e.get("source", "")
        text = _source_value(item, source).strip()
        if not text:
            continue

        style = e.get("style", "normal")
        size = int(e.get("size", t.font_size))
        pos = e.get("pos", "UL")
        align = e.get("align", "left")
        span = int(e.get("span", e.get("span_x", 1)) or 1)
        span = max(1, min(3, span))
        wrap = bool(e.get("wrap", False))
        max_lines = int(e.get("max_lines", 1))

        font = _font_for_style(t.font_name, style)
        c.setFont(font, size)

        leading = max(1, int(size * 1.15))

        # Bound each element to a 3x3 cell inside the padded label area
        cx, cy, cw, ch, row = _cell_box(pos, x, y, w, h, span_x=span)

        # Cap lines by what fits vertically in the cell
        max_lines_cell = max(1, int(ch // leading))
        ml = max(1, min(max_lines, max_lines_cell))

        max_w = cw
        lines = _wrap_lines(text, max_w, font, size, ml) if wrap else [_truncate_to_width(text, max_w, font, size)]

        align_l = (align or "left").lower()
        if align_l == "center":
            ax = cx + cw / 2
        elif align_l == "right":
            ax = cx + cw
        else:
            ax = cx

        if row == "U":
            cur_y = (cy + ch) - size
            for ln in lines:
                _draw_aligned(c, align, ax, cur_y, ln)
                cur_y -= leading

        elif row == "M":
            total_h = len(lines) * leading
            cur_y = (cy + ch / 2) + (total_h / 2) - size
            for ln in lines:
                _draw_aligned(c, align, ax, cur_y, ln)
                cur_y -= leading

        else:
            for i, ln in enumerate(lines):
                yy = (cy + size) + (len(lines) - 1 - i) * leading
                _draw_aligned(c, align, ax, yy, ln)

    if qr_enabled:
        qr_source = qr_cfg.get("source", "purchase_url")
        qr_text = _source_value(item, qr_source).strip()
        if qr_text:
            size_rel = float(qr_cfg.get("size_rel", 0.45))
            qr_size = max(10, min(w, h) * max(0.2, min(0.9, size_rel)))
            pos = qr_cfg.get("pos", "UR")
            cx, cy, cw, ch, _row = _cell_box(pos, x, y, w, h)

            base = min(cw, ch)
            size_rel = float(qr_cfg.get("size_rel", 0.85))
            size_rel = max(0.2, min(0.95, size_rel))

            qr_size = base * size_rel
            qr_size = max(10, min(qr_size, cw, ch))  # keep inside the cell

            qr_x = cx + (cw - qr_size) / 2
            qr_y = cy + (ch - qr_size) / 2

            _draw_qr(c, qr_x, qr_y, qr_size, qr_text)

    c.setFont(t.font_name, t.font_size)
