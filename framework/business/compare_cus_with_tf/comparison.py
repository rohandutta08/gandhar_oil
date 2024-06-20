from typing import Tuple

import polars as pl

from framework.business.compare_cus_with_tf.config import ConfigLoaderInterface
from framework.business.compare_cus_with_tf.config.dto import AggregationOperator


class DataReconciler:
    def __init__(self, template_config: ConfigLoaderInterface,
                 df_customer: pl.dataframe,
                 df_output: pl.dataframe):
        self.template_config = template_config
        self.df_customer = df_customer
        self.df_output = df_output
        self._validate()

    def _validate(self):
        required_columns = self.template_config.get_all_columns()

        missing_columns_customer = [col for col in required_columns if col not in self.df_customer.columns]
        if missing_columns_customer:
            raise ValueError(f"Missing columns in customer file: {', '.join(missing_columns_customer)}")

        missing_columns_output = [col for col in required_columns if col not in self.df_output.columns]
        if missing_columns_output:
            raise ValueError(f"Missing columns in ELT output: {', '.join(missing_columns_output)}")

    def _roll_up_to_doc_level(self) -> Tuple[pl.DataFrame, pl.DataFrame]:
        agg_expr = []
        for col, agg_func in self.template_config.doc_level_aggregator().items():
            if agg_func == AggregationOperator.SUM:
                agg_expr.append(pl.col(col).sum().alias(col))
            elif agg_func == AggregationOperator.FIRST:
                agg_expr.append(pl.col(col).first().alias(col))
            elif agg_func == AggregationOperator.SET:
                agg_expr.append(
                    pl.col(col).unique()
                    .apply(lambda x: '|'.join(sorted(filter(None, x), reverse=True)))
                    .alias(col)
                )

        df_output_doc_level = self.df_output.group_by("Composite_Key"). \
            agg(agg_expr).with_columns(pl.col("Composite_Key").alias("Composite_Key_Out"))
        df_customer_doc_level = self.df_customer.group_by("Composite_Key"). \
            agg(agg_expr).with_columns(pl.col("Composite_Key").alias("Composite_Key_Cus"))

        return df_output_doc_level, df_customer_doc_level

    def _derive_composite_key(self):
        composite_key_columns = self.template_config.composite_keys()
        composite_key_expr = pl.concat_str(
            [pl.col(col).fill_null("").cast(pl.Utf8) for col in composite_key_columns]
        )
        self.df_customer = self.df_customer.with_columns(composite_key_expr.alias("Composite_Key"))
        self.df_output = self.df_output.with_columns(composite_key_expr.alias("Composite_Key"))

    def run_recon(self):
        self._derive_composite_key()
        df_output_doc_level, df_customer_doc_level = self._roll_up_to_doc_level()

        return self._find_differences(df_customer_doc_level, df_output_doc_level)

    def _find_differences(self, df_customer_doc_level, df_output_doc_level):
        joined_df = df_output_doc_level.join(
            df_customer_doc_level,
            on="Composite_Key",
            how="outer",
            suffix="_cus"
        )
        columns_to_compare = self.template_config.default_columns_to_compare()
        comparison_cols = [pl.col(col) for col in columns_to_compare] + [pl.col(f"{col}_cus") for col in
                                                                         columns_to_compare]

        def _find_mismatches(row) -> str:
            mismatches = []
            for col in columns_to_compare:
                if row[col] != row[f"{col}_cus"]:
                    mismatches.append(col)
            return "|".join(mismatches)

        differences = joined_df.with_columns(
            pl.struct(comparison_cols).map_elements(_find_mismatches).alias("Mismatched Fields")
        )
        return differences.with_columns(
            pl.when(
                (pl.col("Mismatched Fields").is_null() | (pl.col("Mismatched Fields") == "")) &
                pl.col("Composite_Key_Out").is_not_null() &
                pl.col("Composite_Key_Cus").is_not_null()
            )
            .then(pl.lit("Exact"))
            .when(pl.col("Composite_Key_Out").is_null())
            .then(pl.lit("Missing in CT Generated"))
            .when(pl.col("Composite_Key_Cus").is_null())
            .then(pl.lit("Missing in Customer"))
            .when(pl.col("Mismatched Fields").is_not_null() & (pl.col("Mismatched Fields") != ""))
            .then(pl.lit("Mismatch"))
            .otherwise(pl.lit("Unknown"))  # Just in case there's a scenario not covered
            .alias("Matching Status")
        )
