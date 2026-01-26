import re
import pdfplumber as pdp
import os
import numpy as np

# dir names:
base_dir = os.path.expanduser("/")

mcmaster_dir = "../McMaster_Items"
receipts_dir = "receipts"
packing_dir = "packing_lists"

# build full paths safely
dir_a = os.path.join(base_dir, mcmaster_dir, receipts_dir)



def choose_file_path(main_dir):
    # build dict to allow user to select file
    file_list = np.array(os.listdir(main_dir))
    x = file_list.shape[0]
    n = np.arange(0,x+1,1)
    file_dict = dict(zip(n.ravel(),file_list[n-1].ravel()))
    # visualize dict of files for user:
    print("List of files to read: ")
    for item in file_dict.keys():
        print(item+1,"-",file_dict[item])

    # find pdf files
    try:
        user_file_num = int(input("Enter file (#) to read: "))
        # Proceed with using user_file_num

        # validate input
        while user_file_num is not None:
            if user_file_num - 1 in np.arange(0, x + 1, 1):
                user_file = file_dict[user_file_num]
                user_file_path = os.path.join(main_dir, user_file)
                return user_file_path
            else:
                raise ValueError("Invalid file number or input ...")
                break
        return None

    except ValueError:
        print("Error: Please enter a valid integer number.")
        # Handle the error (e.g., set a default or exit)


# select receipt
my_receipt_path = choose_file_path(dir_a)



print(my_receipt_path)