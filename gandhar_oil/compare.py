import duckdb
import shutil
import os
import sys

if not os.path.exists('input/zomato-pr.csv') or not os.path.exists('input/zomato-sales.csv'):
    print('No files exist to compare. Skipping')
    sys.exit(0)

conn = duckdb.connect('output/output.duckdb')

def get_count(sql_query):
    conn.execute(sql_query)
    return conn.fetchone()[0]

def create_table_from_csv(table_name: str, csv_path: str, delim=",", all_varchar=1):
    conn.execute(f"drop table if exists {table_name}")
    conn.execute(f"create table {table_name} as select * from read_csv_auto('{csv_path}', delim='{delim}', all_varchar={all_varchar});")
    print(f"Created table: {table_name}")

def create_common_table(output_table_name: str, left_table: str, right_table: str, common_field_name: str):
    query = f"""
        CREATE OR REPLACE table {output_table_name} as
        (
            (select distinct "{common_field_name}" from {left_table} order by 1)
            INTERSECT
            (select distinct "{common_field_name}" from {right_table} order by 1)
        );
    """

    # print(query)
    conn.execute(query)

    print(f"Created table '{output_table_name}'")
    left_count = get_count(f'select count(distinct "{common_field_name}") from {left_table};')
    right_count = get_count(f'select count(distinct "{common_field_name}") from {right_table};')
    common_count = get_count(f'select count(distinct "{common_field_name}") from {output_table_name};')
    print(f"Stats: \n\t{left_table}: {left_count}\n\t{right_table}\t{right_count}\n\t{output_table_name}\t{common_count}")

def copy_to_csv(output_file, sql_query):
    if os.path.exists(output_file):
        os.remove(output_file)

    conn.execute(f"COPY ({sql_query}) to '{output_file}' (HEADER, DELIMITER ',');")
    print(f"Created {output_file}")

def format(query: str, output_dir: str):
    conn.execute(query)

    names: list[str] = [d[0].strip() for d in conn.description]

    max_len = max(len(name) for name in names)
    pad_columns = max_len + 1

    rows = conn.fetchall()


    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    os.mkdir(output_dir)

    for row in rows:
        d = dict(zip(names, row))

        key = "-".join([str(d[key]) for key in key_columns])

        with open(f"{output_dir}/{key}.txt", "a") as handle:
            handle.write(key)
            handle.write("\n")


            for field in sorted(names):
                if field in key_columns: continue
                if field in skip_columns: continue

                cell = d[field]
                if cell == None:
                    cell = ""

                cell = str(cell)
                cell = cell.strip()

                if field in number_columns:
                    cell = cell.replace(",", "")
                    cell = cell and float(cell) or 0.0
                    cell = abs(cell) # zomato puts negative signs in a weird manner in their SR. We treat those documents as CDN with positive values

                    cell = f"{cell:.0f}"


                if field in zero_pad:
                    cell = cell.rjust(zero_pad[field], "0")

                line = f"\n\t{field.ljust(pad_columns)}: {cell}"
                handle.write(line)

            handle.write("\n\n")


comparison_limit = 10_000

# format(f'select * from original_sr o join (select "SAP Document No." from common_vouchers order by "SAP Document No." limit {comparison_limit}) c on o."SAP Document No." = c."SAP Document No." order by o."SAP Document No."', "diff/sr")
# format(f'select * from transformed_sr t join (select "SAP Document No." from common_vouchers order by "SAP Document No." limit {comparison_limit}) c on t."SAP Document No." = c."SAP Document No." order by t."SAP Document No."', "diff/transformed")

# print(f"Created diff/sr/ diff/transformed for easy diffing (containing upto {comparison_limit} common vouchers)")


############## PR 

create_table_from_csv("original_pr_raw", "input/zomato-pr.csv", all_varchar=0)
conn.execute("""create or replace table original_pr as select * from (
    select
    strftime("Posting Date", '%d.%m.%Y')::varchar  "Voucher Date",
    "Document Number" "Voucher Number",
    "Document type" "Voucher Type",
    "Reference" "Voucher Reference",
    "Offsetting Account" "Vendor Code",
    sum(CGST) "CGST Amount",
    sum(SGST) "SGST Amount",
    sum(IGST) "IGST Amount"
    from original_pr_raw
    group by all);
""")

conn.execute("""
    create or replace table transformed_pr as select * from (
        select
        "Voucher Date",
        cast("Voucher Number" as bigint) "Voucher Number",
        "Voucher Type",
        "Voucher Reference",
        "Vendor Code",
        sum(cast ("CGST Amount" as float)) "CGST Amount",
        sum(cast ("SGST Amount" as float)) "SGST Amount",
        sum(cast ("IGST Amount" as float)) "IGST Amount"
        from s_purchase.lt_clear_pr
        group by all
    );
""")
             
create_common_table("pr_common_vouchers", "original_pr", "transformed_pr", "Voucher Number")

key_columns = ['Voucher Number']
number_columns = [
    "CGST Amount",
    "SGST Amount",
    "IGST Amount",
]

zero_pad = {
    "Vendor Code": 10
}

skip_columns = [
]

format(f'select * from original_pr o join (select "Voucher Number" from pr_common_vouchers order by "Voucher Number" limit {comparison_limit}) c on o."Voucher Number" = c."Voucher Number" order by o."Voucher Number"', "diff/pr")
format(f'select * from transformed_pr t join (select "Voucher Number" from pr_common_vouchers order by "Voucher Number" limit {comparison_limit}) c on t."Voucher Number" = c."Voucher Number" order by t."Voucher Number"', "diff/pr-transformed")




create_table_from_csv('original_sr_clear_format', 'input/zomato-sales.csv')

queries = '''create table original_sr_fixed as select *, lpad("Document Number", 10, '0') fixed_doc_number from original_sr_clear_format
drop table original_sr_clear_format
alter table original_sr_fixed drop column "Document Number"
alter table original_sr_fixed rename column "fixed_doc_number" to "Document Number"
alter table original_sr_fixed rename to original_sr_clear_format
'''

for query in queries.splitlines():
    conn.execute(query)

copy_to_csv('diff/clear_sr.csv', '''select * from s_sale.clear_sr''')
create_table_from_csv('transformed_sr_clear_format', 'diff/clear_sr.csv')
create_common_table('clear_sr_common_vouchers', 'original_sr_clear_format', 'transformed_sr_clear_format', 'Document Number')


key_columns = ['Document Number']
number_columns = [
"Item Taxable Value *",
"CGST Rate",
"CGST Amount",
"SGST Rate",
"SGST Amount",
"IGST Rate",
"IGST Amount",
"Item Discount Amount",
"Quantity",
]

zero_pad = {
    "Document Number": 10,
}

skip_columns = [
    "Status",
    "Document Type Code", # TODO
    "Recipient Billing Name", # TODO
    "Recipient Billing City", # TODO
    "Place of Supply",
    "Shipping Port Code - Export",
    "Shipping Bill Number - Export",
    "Shipping Bill Date - Export",
    "Item Description",
    "Cess Amount",
    "Cess Rate",
    "Is this document cancelled?",
    "Type of Export",
    "Return Filing Month",
    "Total Document Value",
    
]


comparison_limit = 1000

format(f'''select * from original_sr_clear_format o join (select "Document Number" from clear_sr_common_vouchers order by "Document Number" limit {comparison_limit}) c on o."Document Number" = c."Document Number" order by o."Document Number", cast(replace(o."Item Taxable Value *", ',', '') as real)''', "diff/clear-sr")
format(f'''select * from transformed_sr_clear_format t join (select "Document Number" from clear_sr_common_vouchers order by "Document Number" limit {comparison_limit}) c on t."Document Number" = c."Document Number" order by t."Document Number", cast(replace(t."Item Taxable Value *", ',', '') as real)''', "diff/clear-transformed")
print(f"Created diff/clear-sr diff/clear-transformed for easy diffing (containing upto {comparison_limit} common vouchers)")

import csv

def format2(lhs_query, rhs_query, join_column):
    conn.execute(lhs_query)
    names: list[str] = [d[0].strip() for d in conn.description]
    lhs_data = conn.fetchall()

    conn.execute(rhs_query)
    rhs_data = conn.fetchall()

    join_keys = set()
    key_col_index = names.index(join_column)

    for row in lhs_data:
        join_keys.add(row[key_col_index])
    for row in rhs_data:
        join_keys.add(row[key_col_index])

    columns = ["Document Date *"
        "My GSTIN"
        "Quantity"
        "Item Unit of Measurement"
        "Item Taxable Value *"
        "Item Discount Amount"
        "CGST Rate"
        "CGST Amount"
        "SGST Rate"
        "SGST Amount"
        "IGST Rate"
        "IGST Amount"
        "Is the item a GOOD (G) or SERVICE (S"
        "HSN or SAC code"
        "Recipient Billing GSTIN"
    ]

    with open('diff/sr-diff.csv', 'w') as handle:
        writer = csv.writer(handle)

        header = ["Key"] + names + names
        writer.writerow(header)

        for key in join_keys:
            lhs_rows = [row for row in lhs_data if row[key_col_index] == key]
            rhs_rows = [row for row in lhs_data if row[key_col_index] == key]

            for lhs_row, rhs_row in zip(lhs_rows, rhs_rows):
                current_row = [key]
                if not lhs_row: lhs_row = [''] * 15
                if not rhs_row: lhs_row = [''] * 15

                # print(current_row)

                current_row = current_row + list(lhs_row) + list(rhs_row)

                writer.writerow(current_row)

        print("wrote diff/sr-diff.csv")

format2(
    f'select * from original_sr_clear_format o join (select "Document Number" from clear_sr_common_vouchers order by "Document Number" limit {comparison_limit}) c on o."Document Number" = c."Document Number" order by o."Document Number", o."Item Taxable Value *"',
    f'select * from transformed_sr_clear_format t join (select "Document Number" from clear_sr_common_vouchers order by "Document Number" limit {comparison_limit}) c on t."Document Number" = c."Document Number" order by t."Document Number", t."Item Taxable Value *"',
    "Document Number"
)
