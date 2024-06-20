from typing import Dict

from framework.business.compare_cus_with_tf.config.dto import AggregationOperator, FilePath
from framework.business.compare_cus_with_tf.config.dto.i_config_loader import ConfigLoaderInterface


class PurchaseConfigLoader(ConfigLoaderInterface):
    def get_output_duckdb_table(self) -> str:
        return "s_purchase.lt_clear_pr"

    def doc_level_aggregator(self) -> Dict[str, AggregationOperator]:
        return {
            "Voucher Number": AggregationOperator.FIRST,
            "My GSTIN": AggregationOperator.SET,
            "Supplier GSTIN": AggregationOperator.FIRST,
            "Item Taxable Amount": AggregationOperator.SUM,
            # "Document Number": AggregationOperator.FIRST,
            "Voucher Date": AggregationOperator.FIRST,
            "CGST Amount": AggregationOperator.SUM,
            "SGST Amount": AggregationOperator.SUM,
            "IGST Amount": AggregationOperator.SUM
        }

    def composite_keys(self):
        return ['Voucher Number', 'My GSTIN', 'Supplier GSTIN']

    def get_file_path(self):
        return FilePath(
            customer_file="customer-pr.csv",
            elt_output_file="purchase_register.csv",
            output_file="pr_comparison.csv",
            config_file="pr-compare-config.json"
        )
