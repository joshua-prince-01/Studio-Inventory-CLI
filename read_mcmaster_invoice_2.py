import pdfplumber as pdf
import os
# dir names:
base_dir = os.path.expanduser("~/PycharmProjects/PythonProject_studio_inventory")

mcmaster_dir = "McMaster_Items"
receipts_dir = "receipts"
packing_dir = "packing_lists"

# build full paths safely
dir_a = os.path.join(base_dir, mcmaster_dir, receipts_dir)
dir_b = os.path.join(base_dir, mcmaster_dir, packing_dir)

# find pdf files
receipts = os.listdir(dir_a)
dir_a_1 = os.path.join(dir_a, receipts[0])

TABLE_SETTINGS = {
    "vertical_strategy": "text",
    "horizontal_strategy": "text",
    "snap_tolerance": 3,
    "join_tolerance": 3,
    "intersection_tolerance": 3,
    "min_words_vertical": 2,
    "min_words_horizontal": 1,
}

def extract_line_items_tables(pdf_path):
    out = []
    with pdf.open(pdf_path) as p:
        for page in p.pages:
            # find where the table header starts
            words = page.extract_words()  # each word has x0,x1,top,bottom,text
            line_words = [w for w in words if w["text"].strip().lower() == "line"]

            if not line_words:
                out.append([])
                continue

            # take the first occurrence of "Line" as the start of the table
            y_top = line_words[0]["top"] - 5  # small padding

            # crop from that y down to bottom of page
            cropped = page.crop((0, y_top, page.width, page.height))

            tables = cropped.extract_tables(table_settings=TABLE_SETTINGS)
            out.append(tables)

    return out

tables = extract_line_items_tables(dir_a_1)
print([len(t) for t in tables])
print(tables[0][0][:10])  # preview rows

