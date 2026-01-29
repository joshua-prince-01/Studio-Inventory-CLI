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


def _anchor_xy(pos: str, x: float, y: float, w: float, h: float) -> tuple[float, float, str]:
    pos = (pos or "UL").upper()
    row = pos[0]
    col = pos[1]
    if row == "U":
        y0 = y + h
        v = "top"
    elif row == "M":
        y0 = y + h / 2
        v = "mid"
    else:
        y0 = y
        v = "bot"

    if col == "L":
        x0 = x
        hc = "left"
    elif col == "C":
        x0 = x + w / 2
        hc = "center"
    else:
        x0 = x + w
        hc = "right"
    return x0, y0, f"{v}-{hc}"


def _draw_aligned(c: canvas.Canvas, align: str, x: float, y: float, text: str) -> None:
    align = (align or "left").lower()
    if align == "center":
        c.drawCentredString(x, y, text)
    elif align == "right":
        c.drawRightString(x, y, text)
    else:
        c.drawString(x, y, text)


def _cell_rect(pos: str, x: float, y: float, w: float, h: float, span: int = 1) -> tuple[float, float, float, float, str]:
    """Return a bounded rect for a 3x3 anchor cell (with optional horizontal span).

    Returns: (cx, cy, cw, ch, row_tag) where row_tag in {"U","M","L"}.
    """
    pos = (pos or "UL").upper()
    row = pos[0] if len(pos) >= 1 else "U"
    col = pos[1] if len(pos) >= 2 else "L"

    cell_w = w / 3.0
    cell_h = h / 3.0

    col_idx = {"L": 0, "C": 1, "R": 2}.get(col, 0)
    row_idx = {"L": 0, "M": 1, "U": 2}.get(row, 2)  # bottom=0, top=2

    span = max(1, min(3, int(span or 1)))
    if span == 3:
        start_col = 0
    elif span == 2:
        start_col = 1 if col_idx == 2 else 0
    else:
        start_col = col_idx

    cx = x + start_col * cell_w
    cy = y + row_idx * cell_h
    cw = cell_w * span
    ch = cell_h
    return cx, cy, cw, ch, row


def _flow_rects(x: float, y: float, w: float, h: float, qr_cfg: dict) -> tuple[tuple[float,float,float,float], Optional[tuple[float,float,float]]]:
    """Compute (text_rect, qr_box) for FLOW mode.

    text_rect: (tx, ty, tw, th)
    qr_box: (qx, qy, qsize) or None
    """
    enabled = bool(qr_cfg.get("enabled", False))
    if not enabled:
        return (x, y, w, h), None

    orientation = str(qr_cfg.get("orientation", "horizontal")).lower()
    side = str(qr_cfg.get("side", "left")).lower()
    size_rel = float(qr_cfg.get("size_rel", 0.45))
    size_rel = max(0.2, min(0.9, size_rel))
    gap_rel = float(qr_cfg.get("gap_rel", 0.08))
    gap_rel = max(0.0, min(0.3, gap_rel))

    gap = min(w, h) * gap_rel

    if orientation == "vertical":
        qr_size = min(w, h * size_rel)
        qr_size = max(10, min(qr_size, min(w, h)))
        # ensure text area remains usable
        if h - qr_size - gap < 20:
            qr_size = max(10, h - gap - 20)

        qx = x + (w - qr_size) / 2
        if side == "bottom":
            qy = y
            ty = y + qr_size + gap
            th = h - qr_size - gap
        else:  # top
            qy = y + h - qr_size
            ty = y
            th = h - qr_size - gap
        tx, tw = x, w
        return (tx, ty, tw, th), (qx, qy, qr_size)

    # horizontal
    qr_size = min(h, w * size_rel)
    qr_size = max(10, min(qr_size, min(w, h)))
    if w - qr_size - gap < 20:
        qr_size = max(10, w - gap - 20)

    qy = y + (h - qr_size) / 2
    if side == "right":
        qx = x + w - qr_size
        tx, tw = x, w - qr_size - gap
    else:  # left
        qx = x
        tx, tw = x + qr_size + gap, w - qr_size - gap
    ty, th = y, h
    return (tx, ty, tw, th), (qx, qy, qr_size)


def _render_flow_layout(c: canvas.Canvas, item: dict, x: float, y: float, w: float, h: float, t: LabelTemplate, layout: dict) -> None:
    elems = layout.get("elements", []) or []
    qr_cfg = layout.get("qr", {}) or {}
    text_rect, qr_box = _flow_rects(x, y, w, h, qr_cfg)

    tx, ty, tw, th = text_rect

    # draw QR first
    if qr_box and bool(qr_cfg.get("enabled", False)):
        qr_source = qr_cfg.get("source", "purchase_url")
        qr_text = _source_value(item, qr_source).strip()
        if qr_text:
            qx, qy, qs = qr_box
            _draw_qr(c, qx, qy, qs, qr_text)

    # text stack (top-down)
    y_cursor = ty + th
    for e in elems:
        src = e.get("source", "")
        text = _source_value(item, src).strip()
        if not text:
            continue

        style = e.get("style", "normal")
        size = int(e.get("size", t.font_size))
        align = e.get("align", "left")
        wrap = bool(e.get("wrap", False))
        max_lines = int(e.get("max_lines", 1))

        font = _font_for_style(t.font_name, style)
        c.setFont(font, size)

        lines = _wrap_lines(text, tw, font, size, max_lines) if wrap else [_truncate_to_width(text, tw, font, size)]
        leading = max(1, int(size * 1.15))

        # start at top line baseline
        y_cursor -= size
        for ln in lines:
            if y_cursor < ty:
                break
            if align == "center":
                c.drawCentredString(tx + tw / 2, y_cursor, ln)
            elif align == "right":
                c.drawRightString(tx + tw, y_cursor, ln)
            else:
                c.drawString(tx, y_cursor, ln)
            y_cursor -= leading

        # small gap between elements
        y_cursor -= max(0, int(size * 0.1))

        if y_cursor < ty:
            break

    c.setFont(t.font_name, t.font_size)


def _render_grid_layout(c: canvas.Canvas, item: dict, x: float, y: float, w: float, h: float, t: LabelTemplate, layout: dict) -> None:
    elems = layout.get("elements", []) or []
    qr_cfg = layout.get("qr", {}) or {}

    for e in elems:
        source = e.get("source", "")
        text = _source_value(item, source).strip()
        if not text:
            continue

        style = e.get("style", "normal")
        size = int(e.get("size", t.font_size))
        pos = e.get("pos", "UL")
        align = e.get("align", "left")
        wrap = bool(e.get("wrap", False))
        max_lines = int(e.get("max_lines", 1))
        span = int(e.get("span", 1))

        font = _font_for_style(t.font_name, style)
        c.setFont(font, size)

        cx, cy, cw, ch, row = _cell_rect(pos, x, y, w, h, span=span)
        max_w = cw

        lines = _wrap_lines(text, max_w, font, size, max_lines) if wrap else [_truncate_to_width(text, max_w, font, size)]
        leading = max(1, int(size * 1.15))

        if align == "center":
            ax = cx + cw / 2
        elif align == "right":
            ax = cx + cw
        else:
            ax = cx

        if row == "U":
            cur_y = cy + ch - size
            for ln in lines:
                _draw_aligned(c, align, ax, cur_y, ln)
                cur_y -= leading
        elif row == "M":
            total_h = len(lines) * leading
            cur_y = (cy + ch / 2) + (total_h / 2) - size
            for ln in lines:
                _draw_aligned(c, align, ax, cur_y, ln)
                cur_y -= leading
        else:  # L
            cur_y = cy + size + (len(lines) - 1) * leading
            for ln in lines:
                _draw_aligned(c, align, ax, cur_y, ln)
                cur_y -= leading

    # QR in grid mode (legacy: uses pos)
    if bool(qr_cfg.get("enabled", False)):
        qr_source = qr_cfg.get("source", "purchase_url")
        qr_text = _source_value(item, qr_source).strip()
        if qr_text:
            size_rel = float(qr_cfg.get("size_rel", 0.45))
            qr_size = max(10, min(w, h) * max(0.2, min(0.9, size_rel)))
            pos = qr_cfg.get("pos", "UR")
            qx, qy, qw, qh, _row = _cell_rect(pos, x, y, w, h, span=1)
            # place within that cell, top-left of cell by default
            # but keep consistent with old behavior: align to cell based on anchor
            ax, ay, anchor = _anchor_xy(pos, qx, qy, qw, qh)
            if anchor.startswith("top"):
                qr_y = (qy + qh) - qr_size
            elif anchor.startswith("mid"):
                qr_y = (qy + qh/2) - qr_size/2
            else:
                qr_y = qy
            if anchor.endswith("left"):
                qr_x = qx
            elif anchor.endswith("center"):
                qr_x = (qx + qw/2) - qr_size/2
            else:
                qr_x = (qx + qw) - qr_size
            _draw_qr(c, qr_x, qr_y, qr_size, qr_text)

    c.setFont(t.font_name, t.font_size)


def _render_layout(c: canvas.Canvas, item: dict, x: float, y: float, w: float, h: float, t: LabelTemplate, layout: dict) -> None:
    """Render a label using either FLOW or GRID mode.

    FLOW mode is the new default:
      - QR + text are arranged horizontally or vertically.
      - Text elements are stacked in the remaining text area.

    GRID mode is kept for backward compatibility with older presets that use pos/span/qr.pos.
    """
    mode = str(layout.get("mode", "") or "").lower()
    elems = layout.get("elements", []) or []
    qr_cfg = layout.get("qr", {}) or {}

    # Auto-detect FLOW if QR has orientation/side or if elements have no 'pos'
    if mode == "flow" or ("orientation" in qr_cfg) or (elems and "pos" not in elems[0]):
        _render_flow_layout(c, item, x, y, w, h, t, layout)
    else:
        _render_grid_layout(c, item, x, y, w, h, t, layout)
    return
