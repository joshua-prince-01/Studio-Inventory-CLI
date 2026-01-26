import os
import re
import pdfplumber

LINE_ITEM_RE = re.compile(r"^\s*(\d+)\s+([A-Z0-9]+)\s*(.*)$", re.I)

def group_words_into_lines(words, y_tol=2):
    words = sorted(words, key=lambda w: (w["top"], w["x0"]))
    lines = []
    for w in words:
        if not lines or abs(w["top"] - lines[-1]["y"]) > y_tol:
            lines.append({"y": w["top"], "words": [w]})
        else:
            lines[-1]["words"].append(w)
    return lines

def line_text(ln):
    return " ".join(w["text"] for w in ln["words"]).strip()

def find_header_line(words, y_tol=2):
    for ln in group_words_into_lines(words, y_tol):
        toks = [w["text"].lower().strip() for w in ln["words"]]
        if any(t == "line" or t.startswith("line") for t in toks):
            return ln
    return None

def build_bounds(header_line, page_width):
    hdr = {w["text"].lower().strip(): w for w in header_line["words"]}
    required = ["ordered", "shipped", "balance", "price", "total"]
    if not all(k in hdr for k in required):
        return None

    x_ordered = hdr["ordered"]["x0"]
    x_shipped = hdr["shipped"]["x0"]
    x_balance = hdr["balance"]["x0"]
    x_price   = hdr["price"]["x0"]
    x_total   = hdr["total"]["x0"]

    # --- KEY FIX: split price vs total at midpoint of their header anchors ---
    x_split = (x_price + x_total) / 2.0

    return {
        "text":    (0, x_ordered),
        "ordered": (x_ordered, x_shipped),
        "shipped": (x_shipped, x_balance),
        "balance": (x_balance, x_price),
        "price":   (x_price, x_split),
        "total":   (x_split, page_width),
    }

def col_for_x(x, bounds):
    for k, (x0, x1) in bounds.items():
        if x0 <= x < x1:
            return k
    return None

def find_stop_line(words, y_tol=2):
    # Anything BELOW this line is ignored
    for ln in group_words_into_lines(words, y_tol):
        t = line_text(ln).lower()
        if "packing list" in t or "merchandise" in t:
            return ln
    return None

_moneyish = re.compile(r"^\$?\d+(?:,\d{3})*(?:\.\d{2})?$")

def parse_receipt(pdf_path, page_num=0, debug=True):
    with pdfplumber.open(pdf_path) as p:
        page = p.pages[page_num]
        words = page.extract_words(use_text_flow=False, keep_blank_chars=False)

        header = find_header_line(words)
        if not header:
            if debug:
                print("NO HEADER")
            return []

        bounds = build_bounds(header, page.width)
        if not bounds:
            if debug:
                print("HEADER MISSING COLUMN ANCHORS")
            return []

        stop_line = find_stop_line(words)

        # start just below header; end just above stop marker (or page end)
        y_start = header["y"] + 2
        y_end   = (stop_line["y"] - 2) if stop_line else page.height

        if debug:
            print("\n--- parse_receipt ---")
            print("file:", pdf_path)
            print(f"table y-range: {y_start:.1f} → {y_end:.1f}")

        cropped = page.crop((0, y_start, page.width, y_end))
        words = cropped.extract_words(use_text_flow=False, keep_blank_chars=False)
        lines = group_words_into_lines(words)

        items = []
        current = None

        for ln in lines:
            buckets = {k: [] for k in bounds}
            for w in sorted(ln["words"], key=lambda w: w["x0"]):
                col = col_for_x(w["x0"], bounds)
                if col:
                    buckets[col].append(w["text"])

            row = {k: " ".join(v).strip() for k, v in buckets.items()}
            if not any(row.values()):
                continue

            text = row["text"]
            m = LINE_ITEM_RE.match(text)

            if m:
                line_no = int(m.group(1))
                sku = m.group(2)
                desc = (m.group(3) or "").strip()

                ordered = row["ordered"]
                shipped = row["shipped"]
                balance = row["balance"]
                price   = row["price"]
                total   = row["total"]

                # Clean balance: keep last token if it picked up noise
                if balance:
                    balance = balance.split()[-1]

                # --- SAFETY: if total is empty but price looks like money, copy it ---
                # (helps when the numeric lands just left of our split on some PDFs)
                if (not total) and price and _moneyish.match(price.split()[-1]):
                    total = price.split()[-1]
                    price = price.split()[-1]

                current = {
                    "line": line_no,
                    "sku": sku,
                    "description": desc,
                    "ordered": ordered,
                    "shipped": shipped,
                    "balance": balance,
                    "price": price,
                    "total": total,
                }
                items.append(current)
            else:
                # continuation line
                if current and text:
                    current["description"] = (current["description"] + " " + text).strip()

        if debug:
            print("items parsed:", len(items))

        return items

if __name__ == "__main__":
    base = os.path.expanduser("~/PycharmProjects/PythonProject_studio_inventory")
    receipts_dir = os.path.join(base, "McMaster_Items", "receipts")

    pdfs = sorted(f for f in os.listdir(receipts_dir) if f.lower().endswith(".pdf"))
    if not pdfs:
        raise FileNotFoundError(f"No PDFs found in: {receipts_dir}")

    pdf_path = os.path.join(receipts_dir, pdfs[1])

    items = parse_receipt(pdf_path, debug=True)

    print("\n--- ITEMS ---")
    for it in items:
        print(it)
