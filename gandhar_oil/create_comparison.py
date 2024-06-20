import duckdb
import csv
import shutil
import os
import sys
import json

if not os.path.exists('input/customer-pr.csv') or not os.path.exists('input/customer-sr.csv'):
    print('No files exist to compare. Skipping')
    sys.exit(0)

conn = duckdb.connect('output/output.duckdb')

def get_headers(file_path):
    with open(file_path, mode='r', encoding='utf-8-sig') as handle:
        reader = csv.reader(handle)
        headers = next(reader)
        return headers
    
def get_columns(table_name):
    description = conn.execute(f'SELECT * FROM {table_name} LIMIT 1').description
    columns = [col[0] for col in description]
    return columns

source = get_headers('input/customer-sr.csv')
destination = get_columns('s_sale.clear_sr')


source = ['Company Code', 'Plant', 'Plant Name', 'Business Place', 'Plant GST No.', 'Customer Region', 'Customer Region', 'Customer GST No.', 'Invoice Number', 'Invoice Date', 'Bill. Type', 'Bill. Type', 'Customer Code', 'Customer Name', 'Customer Type', 'HSN', 'Material Code', 'Material Description', 'Quantity', 'UOM', 'Invoice Value', 'Taxable Value', 'Document No.', 'Document Year', 'Posting Date', 'Tax Code', 'Tax Code', 'FI Document Type', 'FI Document Type', 'Original Invoice Number', 'GL Code-JOCG', 'GL Desc-JOCG', 'JOCG-Rate', 'IN:Central GST', 'GL Code-JOIG', 'GL Desc-JOIG', 'JOIG-Rate', 'IN:Integrated GST', 'GL Code-JOSG', 'GL Desc-JOSG', 'JOSG-Rate', 'IN:State GST', 'GL Code-JOUG', 'GL Desc-JOUG', 'JOUG-Rate', 'IN: Uniton UTGST']
destination = ['Document Type Code', 'Document Date *', 'Document Number', 'My GSTIN', 'Quantity', 'Item Unit of Measurement', 'Recipient Billing Name', 'Recipient Billing GSTIN', 'Is this document cancelled?', 'Item Taxable Value *', 'Item Discount Amount', 'CGST Rate', 'CGST Amount', 'SGST Rate', 'SGST Amount', 'IGST Rate', 'IGST Amount', 'Is the item a GOOD (G) or SERVICE (S)', 'HSN or SAC code', 'Item Description', 'Place of Supply', 'Shipping Port Code', 'Shipping Bill Number - Export', 'Shipping Bill Date - Export']


prompt = f"""

Please map the following fields:


source:
{source}

destination: 
{destination}

For each source field, enter the destination field name. If the field is not present in the destination, give an empty string.
Give output in JSON format. Remove single quotes from the keys.

"""



print(prompt)


# Paste this in to chatgpt / openai playground. Set temperature to 0.
# Put the output here and then manually edit it.

output = {
    "Document Type Code": "FI Document Type",
    "Document Date *": "Invoice Date",
    "Document Number": "Document No.",
    "My GSTIN": "Plant GST No.",
    "Quantity": "Quantity",
    "Item Unit of Measurement": "UOM",
    "Recipient Billing Name": "Customer Name",
    "Recipient Billing GSTIN": "Customer GST No.",
    "Is this document cancelled?": "",
    "Item Taxable Value *": "Taxable Value",
    "Item Discount Amount": "",
    "CGST Rate": "JOCG-Rate",
    "CGST Amount": "IN:Central GST",
    "SGST Rate": "JOSG-Rate",
    "SGST Amount": "IN:State GST",
    "IGST Rate": "JOIG-Rate",
    "IGST Amount": "IN:Integrated GST",
    "Is the item a GOOD (G) or SERVICE (S)": "",
    "HSN or SAC code": "HSN",
    "Item Description": "Material Description",
    "Place of Supply": "Customer Region",
    "Shipping Port Code": "",
    "Shipping Bill Number - Export": "",
    "Shipping Bill Date - Export": ""
}


updated_output = {
    "Document Type Code": "",
    "Document Date *": "Invoice Date",
    "Document Number": "Document No.",
    "My GSTIN": "Plant GST No.",
    "Quantity": "Quantity",
    "Item Unit of Measurement": "UOM",
    "Recipient Billing Name": "Customer Name",
    "Recipient Billing GSTIN": "Customer GST No.",
    "Is this document cancelled?": "",
    "Item Taxable Value *": "Taxable Value",
    "Item Discount Amount": "",
    "CGST Rate": "JOCG-Rate",
    "CGST Amount": "IN:Central GST",
    "SGST Rate": "JOSG-Rate",
    "SGST Amount": "IN:State GST",
    "IGST Rate": "JOIG-Rate",
    "IGST Amount": "IN:Integrated GST",
    "Is the item a GOOD (G) or SERVICE (S)": "",
    "HSN or SAC code": "HSN",
    "Item Description": "Material Description",
    "Place of Supply": "Customer Region",
    "Shipping Port Code": "",
    "Shipping Bill Number - Export": "",
    "Shipping Bill Date - Export": ""
}

with open('diff/sr-mapping.json', 'w') as handle:
    json.dump(updated_output, handle, indent=4)

print("Wrote SR Mapping to diff/sr-mapping.json")
