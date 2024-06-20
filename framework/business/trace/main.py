import streamlit as st
from streamlit_ace import st_ace, LANGUAGES
import duckdb
import json
import argparse


MANIFEST_FILE_PATH = "target/manifest.json"
OUTPUT_DB_PATH = "output/output.duckdb"
INPUT_DB_PATH = "input/input.duckdb"


conn = duckdb.connect(OUTPUT_DB_PATH, read_only=True)
try:
    conn.execute(f"attach '{INPUT_DB_PATH}' as raw;")
except:
    pass

st.session_state.parents = []
st.session_state.executed_nodes = []
st.session_state.table_names = {}
st.session_state.document_number = None

def read_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

manifest = read_json(MANIFEST_FILE_PATH)

def fetch_dict():
    rows = conn.fetchall()
    columns = [d[0] for d in conn.description]
    output = []
    for row in rows:
        output.append(dict(zip(columns, row)))
    return output

def set_table_names(manifest):
    for node_name, node_metadata in manifest["nodes"].items():
        st.session_state.table_names[node_name] = f"{node_metadata['schema']}.{node_metadata['name']}"
    for node_name, node_metadata in manifest["sources"].items():
        st.session_state.table_names[node_name] = f"{node_metadata['schema']}.{node_metadata['name']}"

def get_parent_nodes(node_name):
    parent_nodes = manifest["parent_map"].get(node_name, [])
    return parent_nodes

def get_child_nodes(node_name):
    child_nodes = manifest["child_map"].get(node_name, [])
    return child_nodes

def get_anchor(table_name):
    return f"{table_name.replace('.','-').replace('_','-')}"

def execute_and_trace(node_id):
    # only if already not executed
    if node_id in st.session_state.executed_nodes:
        return

    st.session_state.executed_nodes.append(node_id)
    try:
        table_name = st.session_state.table_names[node_id]
    except:
        st.write(f"Table name not found for node ID: {node_id}")

    st.subheader(f"{table_name}", anchor=get_anchor(table_name))

    # execute current node sql

    # TODO: inject filters
    # get table columns
    # if table has columns known for where or on clause then add the filter

    conn.execute(f"DESCRIBE {table_name}")
    columns_dict = fetch_dict()
    column_names = [column["column_name"] for column in columns_dict]
    filterable_columns = ["Document Number", "BELNR", "VBELN", "AWKEY"]
    filters_list = []
    if st.session_state.document_number:
        for filterable_column in filterable_columns:
            if filterable_column in column_names:
                filters_list.append(f"\"{filterable_column}\" = '{st.session_state.document_number}'") 

    filter_text = ""
    if filters_list:
        filter_text = " WHERE " + " OR ".join(filters_list)
        
    sql_query = f"SELECT * FROM {table_name} {filter_text};"

    sql_query_input = st_ace(value=sql_query, language="sql", theme="twilight", show_gutter=False, min_lines=1)
    conn.execute(sql_query_input)
    table_data = fetch_dict()
    st.dataframe(table_data)
    # get parents of current node
    parent_nodes = get_parent_nodes(node_id)
    child_nodes = get_child_nodes(node_id)
    # print the dependency list with links
    parent_node_column, child_node_column = st.columns(2)
    with parent_node_column:
        st.write("#### Parent nodes:")
        for parent_node_id in parent_nodes:
            parent_table_name = st.session_state.table_names[parent_node_id]
            st.write(f"[{parent_table_name}](#{get_anchor(parent_table_name)})")
    with child_node_column:
        st.write("#### Child nodes:")
        for child_node_id in child_nodes:
            child_table_name = st.session_state.table_names[child_node_id]
            st.write(f"[{child_table_name}](#{get_anchor(child_table_name)})")

    st.divider()

    # for each parent, run current function
    for parent_node_id in parent_nodes:
        execute_and_trace(parent_node_id)

def main():

    with st.sidebar:
        document_number = st.text_input("Document Number:", value="", max_chars=None, key=None, type="default", help="Billing document or accounting document number", autocomplete=None, on_change=None, placeholder=None, disabled=False, label_visibility="visible")

        set_table_names(manifest)
        node_options = {node_name: table_name for node_name, table_name in st.session_state.table_names.items()}
        selected_node = st.selectbox('Trace back from:', list(node_options.keys()), format_func=lambda key: node_options[key], help="Select the table from where you want to trace back.")

        st.button("Trace back", type="primary")

    if document_number:
        st.session_state.document_number = str(document_number)
        st.write(f"# Displaying trace for document number {st.session_state.document_number}")
    else:
        st.write("Enter a document number and click on Trace.")
    execute_and_trace(selected_node)

if __name__ == "__main__":
    main()