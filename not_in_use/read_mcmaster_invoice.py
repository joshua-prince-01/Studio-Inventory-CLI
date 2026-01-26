# test opening a pdf

import os
import pdfplumber as pdf
import matplotlib.pyplot as plt
import numpy as np

# dir names:
base_dir = os.path.expanduser("/")

mcmaster_dir = "../McMaster_Items"
receipts_dir = "receipts"
packing_dir = "packing_lists"

# build full paths safely
dir_a = os.path.join(base_dir, mcmaster_dir, receipts_dir)
dir_b = os.path.join(base_dir, mcmaster_dir, packing_dir)

# find pdf files
receipts = os.listdir(dir_a)

dir_a_1 = os.path.join(dir_a, receipts[0])

print(dir_a_1)

def pdf_info(pdf_path,table_settings={}):
    with pdf.open(pdf_path) as a_pdf:
        pages = a_pdf.pages
        meta_data = a_pdf.metadata
        width, height, lines, objects, rects, tables, images = [], [], [], [], [], [], []
        for page in pages:
            width.append(page.width)
            height.append(page.height)
            lines.append(page.lines)
            objects.append(page.objects)
            rects.append(page.rects)
            tables.append(page.find_tables(table_settings))
            images.append(page.images)
        return {"pages":pages,
                "meta_data":meta_data,
                "width":width,
                "height":height,
                "lines":lines,
                "objects":objects,
                "rects":rects,
                "tables":tables,
                "images":images}


my_receipt = pdf_info(dir_a_1)

#im = my_receipt["pages"][0].to_image(resolution=150)
#im.debug_tablefinder(table_settings={})
#im.show()
print(my_receipt["tables"])


def return_line_elements(pdf_obj):
    elements = []
    for line in pdf_obj["lines"]:
           for item in line:
               elements.append(item)
    return elements
'''
def pdf_table_finder(pdf_path):
    with pdf.open(pdf_path) as a_pdf:
        pages = a_pdf.pages
        tables_pg = []
        for page in pages:
            tables_pg.append(page.find_tables())
    return tables_pg

my_elements = return_line_elements(my_receipt)
for each in my_elements:
    print(each)


fig, ax = plt.subplots(1,1,figsize=(10,10))
for element in my_elements:
    pts = np.asarray(element["pts"], float)
    dx = pts[:, 0].max() - pts[:, 0].min()
    dy = pts[:, 1].max() - pts[:, 1].min()
    d = np.hypot(dx, dy)
    if d > 230:
        x, y = zip(*pts)
        ax.plot(x,y,"r--")
        print(d)
fig.show()
'''

