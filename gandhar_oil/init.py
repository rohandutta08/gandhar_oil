from __future__ import annotations
from pathlib import Path
import duckdb
import sys
import glob
import csv
import pandas as pd
from utils.s3_utils import S3Utils
from utils.common_utils import get_s3_files_from_task_config

table_schema_file_path = 'table_schema.csv'

db_path_string = "input/input.duckdb"
db_path = Path(db_path_string)

# check if the db file already exists
if db_path.exists():
    print("The db file already exists. Skipping download from S3")
    sys.exit(0)

input_directory = Path("input/")
output_directory = Path("output/")

input_directory.mkdir(parents=True, exist_ok=True)
output_directory.mkdir(parents=True, exist_ok=True)

file_extensions = ('**/*.xlsx', '**/*.xls','**/*.csv')
xlsx_files = []
for file_extension in file_extensions:
    xlsx_files.extend(input_directory.glob(file_extension))
print(xlsx_files)

files_to_process = []
if not xlsx_files:
    # download the xlsx files from S3
    s3 = S3Utils()
    task_config_file_s3_path = sys.argv[1]
    task_config = s3.get_config_from_json(task_config_file_s3_path)

    # task_config = {
    #     "request": {
    #         "fileInfo": {
    #             "INVOICE": [
    #                 {
    #                     "s3FileUrl": "https://storage.clear.in/v1/ap-south-1/ingestionv2-staging/6febbb80-de77-40e0-a495-0534c703feeb/ADVANCE_INGESTION/2024/FEBRUARY/a3ecec32-6b85-4fdf-b9ce-8bbbd9fef93f/OriginalFileName/a3ecec32-6b85-4fdf-b9ce-8bbbd9fef93f__1707128619263.xlsx",
    #                     "userFileName": "Sales Report NDD 01st December 2023.xlsx"
    #                 }
    #             ]
    #         },
    #         "metadata": {},
    #         "activityFlow": "FULL"
    #     }
    # }
    
    files_to_process = get_s3_files_from_task_config(task_config)

    for file in files_to_process:
        table_directory = input_directory / file["table_name"]
        table_directory.mkdir(parents=True, exist_ok=True)
        target_filename = table_directory / file["file_name"]
        s3.download_file_from_s3_v2(file["s3_url"], str(target_filename))
        print(f'Downloaded "{file["table_name"]}" file with filename "{file["file_name"]}" from "{file["s3_url"]}"')

elif len(xlsx_files) > 0:
    print("XLSX files found.")
    for xlsx_file in xlsx_files:
        table_name = xlsx_file.parent.name
        file_name = xlsx_file.name
        files_to_process.append(
            {
                "table_name": table_name,
                "file_name": file_name,
            }
        )
else:
    sys.exit("No XLSX files found or created.")

print("Files to process:", files_to_process)

def read_table_schema(file_path):
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # skip header row
        return [row for row in reader]
    
def generate_create_table_sql(data):
    tables = set()
    table_columns = {}

    for row in data:
        table_name = row[0]
        column_name = row[1]
        data_type = row[2]

        if table_name not in tables:
            tables.add(table_name)
            table_columns[table_name] = []

        table_columns[table_name].append((column_name, data_type))

    sql_statements = []
    for table, columns in table_columns.items():
        columns_sql = ", ".join(f"{column} {data_type}" for column, data_type in columns)
        table_sql = f"CREATE TABLE {table} ({columns_sql});"
        sql_statements.append(table_sql)

    return sql_statements, table_columns

# read data from the CSV file
table_schema = read_table_schema(table_schema_file_path)

# generate SQL statements and table_columns
sql_statements, table_columns = generate_create_table_sql(table_schema)

duckdb_connection = duckdb.connect(db_path_string)
# execute and print the SQL statements
for statement in sql_statements:
    print(f"{statement}\n")
    duckdb_connection.execute(statement)

def read_file(file_path):
    if file_path.suffix == '.xlsx':
        return pd.read_excel(file_path, dtype=str)
    elif file_path.suffix == '.csv':
        return pd.read_csv(file_path, dtype=str)
    else:
        raise ValueError("Unsupported file format")
    
duckdb_connection.execute("SET GLOBAL pandas_analyze_sample=10000")
for file in files_to_process:
    table_name = file["table_name"]
    file_name = file["file_name"]
    file_path = input_directory / table_name / file_name

    print(f"\nInserting into table {table_name} filename {file_name}")
    df = read_file(file_path)

    # filter df to include only common columns
    common_columns = set(df.columns) & set(dict(table_columns[table_name]).keys())
    df = df[list(common_columns)]
    # df['source_filename'] = file_name

    # write df to duckdb table
    duckdb_connection.query(f"INSERT INTO {table_name} BY NAME (SELECT * FROM df)")

duckdb_connection.close()
