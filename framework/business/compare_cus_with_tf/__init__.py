import os
from typing import List

import polars as pl
import csv
from framework import TemplateType
from framework.business.compare_cus_with_tf.ai_suggester import AiMatcherClient
from framework.business.compare_cus_with_tf.comparison import DataReconciler
from framework.business.compare_cus_with_tf.config import TemplateRunTime
from framework.db.duckdb import DuckDB
from framework.utils.custlogging import LoggerProvider
from framework.utils.time_annotations import measure_execution_time_sec

logger = LoggerProvider().get_logger(os.path.basename(__file__))


class DataComparator:
    def __init__(self, execute_template_types=None, output_only_diff=False):
        if execute_template_types is None:
            self.execute_template_types = [TemplateType.PURCHASE,TemplateType.SALES]
        else:
            self.execute_template_types = execute_template_types
        self.output_only_diff = output_only_diff

        self.templates = {}

        self._validate_and_populate_config()
        self.duckdb_conn = DuckDB(self.templates[list(self.templates)[0]].output_duck_db_path).get_connection()
        self.aiMatcherClient = AiMatcherClient()

    def _validate_and_populate_config(self):
        current_path = os.getcwd()
        print(current_path)
        for template_type in self.execute_template_types:
            self.templates[template_type] = TemplateRunTime(template_type, current_path)

    def _read_polars_schema_from_csv_headers(self, csv_path, reference_schema, columns_mapping_override):
        headers = self._read_csv_header(csv_path)

        new_schema = {}
        for header in headers:
            temp_header = columns_mapping_override.get(header, header)
            new_schema[header] = reference_schema.get(temp_header, pl.Utf8)

        # Read CSV file with the new schema
        df = pl.read_csv(csv_path, schema=new_schema)

        # Prepare a mapping for bulk renaming if columns_mapping_override is provided
        if columns_mapping_override:
            rename_mapping = {original_name: new_name for original_name, new_name in columns_mapping_override.items() if
                              original_name in df.columns}
            df = df.rename(rename_mapping)

        return df

    def _run_comparison(self, template_type):
        with measure_execution_time_sec(f"run comparison for: {template_type}"):
            template_run_time = self.templates[template_type]
            if template_run_time.valid:
                with measure_execution_time_sec("read csv"):
                    df_output = self.duckdb_conn.execute(
                        f" select * from {template_run_time.template_config.get_output_duckdb_table()}").pl()

                    columns_mapping_override = template_run_time.override_config.columns_mapping
                    df_customer = self._read_polars_schema_from_csv_headers(template_run_time.customer_path,
                                                                            df_output.schema, columns_mapping_override)

                with measure_execution_time_sec("run recon"):
                    data_reconciler = DataReconciler(template_run_time.template_config, df_customer, df_output)
                    differences = data_reconciler.run_recon()
                with measure_execution_time_sec("write csv"):
                    if self.output_only_diff:
                        differences.filter(pl.col("Matching Status") != "Exact").write_csv(
                            template_run_time.output_path)
                    else:
                        differences.write_csv(template_run_time.output_path)

    def execute_comparison(self):
        with measure_execution_time_sec("total run comparison"):
            for template_type in self.templates:
                self._run_comparison(template_type)

    @staticmethod
    def _read_csv_header(self: str) -> List[str]:
        with open(self, mode='r', encoding='utf-8-sig') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Read only the first line
            if len(headers) != len(set(headers)):
                duplicate_headers = set([h for h in headers if headers.count(h) > 1])
                raise ValueError(f"Duplicate columns found in CSV headers: {duplicate_headers}")
            return headers

    def _find_suggestion_for_template_type(self, template_type):
        with measure_execution_time_sec(f"finding suggestion for: {template_type}"):
            template_run_time = self.templates[template_type]
            if template_run_time.valid:
                with measure_execution_time_sec("ai suggestion"):
                    headers = self._read_csv_header(template_run_time.customer_path)
                    destination = template_run_time.template_config.get_all_columns()
                    column_suggester = self.aiMatcherClient.do_column_matching(headers, destination)
                    template_run_time.override_config.set_columns_mapping(column_suggester)
                    template_run_time.override_config.save_to_json(template_run_time.config_path)

    def find_suggestion(self):
        with measure_execution_time_sec("total run comparison"):
            for template_type in self.templates:
                self._find_suggestion_for_template_type(template_type)
