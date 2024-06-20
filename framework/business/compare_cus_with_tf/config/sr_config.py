from typing import Dict

from framework.business.compare_cus_with_tf.config.dto import AggregationOperator, FilePath
from framework.business.compare_cus_with_tf.config.dto.i_config_loader import ConfigLoaderInterface


class SalesConfigLoader(ConfigLoaderInterface):
    def get_output_duckdb_table(self) -> str:
        return "s_sale.clear_sr"

    def doc_level_aggregator(self) -> Dict[str, AggregationOperator]:
        return {
            "My GSTIN": AggregationOperator.SET,
            "Item Taxable Amount": AggregationOperator.SUM,
            "Document Number": AggregationOperator.FIRST,
            "Document Date": AggregationOperator.FIRST
        }

    def composite_keys(self):
        return ['Document Number', 'My GSTIN']

    def get_file_path(self):
        return FilePath(
            customer_file="customer-sr.csv",
            elt_output_file="sales_register.csv",
            output_file="sr_comparison.csv",
            config_file="sr-compare-config.json"
        )
