
import json
import os
from enum import Enum

from framework.business.compare_cus_with_tf.config.dto.file_path import FilePath
from framework.utils.custlogging import LoggerProvider

logger = LoggerProvider().get_logger(os.path.basename(__file__))


class AggregationOperator(Enum):
    SET = "set"
    FIRST = "first"
    SUM = "sum"


class DataComparisonConfig:
    def __init__(self, input_file_path: str):
        self.fields_to_compare = []
        self.customer_input_filter = []
        self.columns_mapping = {}
        self.load_from_json(input_file_path)

    def set_columns_mapping(self, columns_mapping):
        self.columns_mapping = columns_mapping

    def load_from_json(self, input_file_path):
        if os.path.exists(input_file_path):
            with open(input_file_path, 'r') as file:
                data = json.load(file)
                self.fields_to_compare = data.get('fields_to_compare', [])
                self.customer_input_filter = data.get('customer_input_filter', [])
                self.columns_mapping = data.get('columns_mapping', {})
        else:
            print(f"File not found: {input_file_path}")

    def save_to_json(self, save_file_path):
        data = {
            'fields_to_compare': self.fields_to_compare,
            'customer_input_filter': self.customer_input_filter,
            'columns_mapping': self.columns_mapping
        }
        os.makedirs(os.path.dirname(save_file_path), exist_ok=True)
        with open(save_file_path, 'w') as file:
            json.dump(data, file, indent=4)

    def __str__(self):
        return f"Fields to Compare: {self.fields_to_compare}\n" \
               f"Customer Input Filter: {self.customer_input_filter}\n" \
               f"Columns Mapping: {self.columns_mapping}"
