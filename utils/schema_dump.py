import duckdb

# Connect to DuckDB database
conn = duckdb.connect('input/input.duckdb')

# Get list of all tables
cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = cursor.fetchall()

# Iterate over all tables
print("TABLE,COLUMN,TYPE")
for table_tuple in tables:
    table_name = table_tuple[0]

    # Get schema for table
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns_info = cursor.fetchall()

    # Iterate over all columns
    for col_info in columns_info:
        col_name = col_info[1]
        col_type = col_info[2]
        print(f"{table_name},{col_name},{col_type}")
