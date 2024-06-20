from abc import ABC, abstractmethod
from typing import List, Dict

from framework.business.compare_cus_with_tf.config.dto import FilePath, AggregationOperator


class ConfigLoaderInterface(ABC):
    @abstractmethod
    def composite_keys(self) -> List[str]:
        pass

    @abstractmethod
    def get_file_path(self) -> FilePath:
        pass

    @abstractmethod
    def get_output_duckdb_table(self) -> str:
        pass

    @abstractmethod
    def doc_level_aggregator(self) -> Dict[str, AggregationOperator]:
        pass

    def default_columns_to_compare(self) -> List[str]:
        return list(self.doc_level_aggregator().keys())

    def get_set_columns(self) -> List[str]:
        aggregator = self.doc_level_aggregator()
        return [col for col, op in aggregator.items() if op == AggregationOperator.SET]

    def get_all_columns(self) -> List[str]:
        combined_list = list(self.default_columns_to_compare()) + self.composite_keys()
        unique_columns = list(set(combined_list))
        return unique_columns
