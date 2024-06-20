import pandas as pd
import numpy as np
from typing import List, Optional, Any, Tuple


def get_array_columns(df: pd.DataFrame) -> Tuple[List[Any], List[Any]]:
    array_columns = []
    non_array_columns = []
    for col in df.columns:
        if isinstance(df[col].values[0], (list, np.ndarray)):
            array_columns.append(col)
        else:
            non_array_columns.append(col)
    return array_columns, non_array_columns


def check_different_column_headers(df1: pd.DataFrame, df2: pd.DataFrame) -> bool:
    headers1 = set(df1.columns)
    headers2 = set(df2.columns)
    diff_columns = headers1.symmetric_difference(headers2)
    if len(diff_columns) > 0:
        print(diff_columns)
        return True
    return False


def iterable_to_list(x):
    if isinstance(x, np.ndarray):
        return x.tolist()
    return x


def lists_are_equal(list1, list2):
    if not list1 and not list2:  # both lists are empty
        return True
    return sorted(list1) == sorted(list2)


def compare_array_columns(df1: pd.DataFrame, df2: pd.DataFrame, array_columns: List[str]) -> bool:
    if not set(array_columns).issubset(df1.columns) or not set(array_columns).issubset(df2.columns):
        print("Error: One or more columns in array_columns are not present in the DataFrames.")
        return False

    all_match = True
    for col in array_columns:
        try:
            # Convert numpy arrays to lists
            df1_col = df1[col].apply(iterable_to_list)
            df2_col = df2[col].apply(iterable_to_list)

            # Explicitly compare the lists element-wise for each row
            matching_rows = df1_col.combine(df2_col, lists_are_equal)

            # Get the indices of non-matching rows
            non_matching_indices = matching_rows[~matching_rows].index.tolist()

            if non_matching_indices:
                all_match = False
                print(f"Rows not matching for column {col}: {non_matching_indices}")
                print("DataFrame 1 values:")
                print(df1.loc[non_matching_indices, col])
                print("DataFrame 2 values:")
                print(df2.loc[non_matching_indices, col])
                print("-" * 50)
        except Exception as e:
            print(f"Error comparing column {col}: {e}")
            return False

    return all_match


def compare_two_dataframes_select(df1: pd.DataFrame, df2: pd.DataFrame,
                                  white_list_columns: Optional[List[str]]) -> bool:
    if df1.empty and df2.empty:
        print("empty")
        return True
    elif df1.empty or df2.empty:
        return False

    if white_list_columns:
        df1 = df1[white_list_columns]
        df2 = df2[white_list_columns]

    if df1.shape != df2.shape:
        print("shape different", df1.shape, df2.shape)
        return False
    else:
        print("shape of 2 df", df1.shape, df2.shape)

    if check_different_column_headers(df1, df2):
        print("columns different")
        return False

    array_col, non_array_columns = get_array_columns(df1)

    comparison_result = df1[non_array_columns].compare(df2[non_array_columns])
    if comparison_result.empty:
        if len(array_col) > 0:
            return compare_array_columns(df1, df2, array_col)
        else:
            return True
    else:
        print("Comparison Diff:\n")
        print(comparison_result)
        return False


def compare_two_dataframes(df1: pd.DataFrame, df2: pd.DataFrame, ignore_column_list: Optional[List[str]]) -> bool:
    if df1.empty and df2.empty:
        print("empty")
        return True
    elif df1.empty or df2.empty:
        return False

    if ignore_column_list:
        df1 = df1.drop(columns=ignore_column_list, errors='ignore')
        df2 = df2.drop(columns=ignore_column_list, errors='ignore')

    if df1.shape != df2.shape:
        print("shape different", df1.shape, df2.shape)
        if df1.shape[0] == df2.shape[0]:
            print(set(df1.columns).difference(set(df2.columns)))
            print(set(df2.columns).difference(set(df1.columns)))
        return False
    else:
        print("shape of 2 df", df1.shape, df2.shape)

    if check_different_column_headers(df1, df2):
        print("columns different")
        return False

    array_col, non_array_columns = get_array_columns(df1)

    comparison_result = df1[non_array_columns].compare(df2[non_array_columns])
    if comparison_result.empty:
        if len(array_col) > 0:
            return compare_array_columns(df1, df2, array_col)
        else:
            return True
    else:
        print(f"Comparison Diff: {comparison_result.shape}")
        print(comparison_result)
        return False


def get_array_columns(df: pd.DataFrame) -> tuple[list[Any], list[Any]]:
    array_columns = []
    non_array_columns = []
    for col in df.columns:
        if isinstance(df[col].values[0], (list, np.ndarray)):
            array_columns.append(col)
        else:
            non_array_columns.append(col)
    return array_columns, non_array_columns


def compare_array_columns(df1: pd.DataFrame, df2: pd.DataFrame, array_columns: List[str]) -> bool:
    for col in array_columns:
        for i in range(len(df1)):
            if not np.array_equal(df1[col].iloc[i], df2[col].iloc[i]):
                print(f"Difference in array column '{col}' at row {i}")
                return False
    return True


def compare_two_dataframes_with_key(df1: pd.DataFrame, df2: pd.DataFrame,
                                    ignore_column_list: Optional[List[str]] = None,
                                    join_key: Optional[str] = None) -> bool:
    if df1.empty and df2.empty:
        print("Both dataframes are empty")
        return True
    elif df1.empty or df2.empty:
        print("One of the dataframes is empty")
        return False

    if ignore_column_list:
        df1 = df1.drop(columns=ignore_column_list, errors='ignore')
        df2 = df2.drop(columns=ignore_column_list, errors='ignore')

    if df1.shape != df2.shape:
        print("Dataframes have different shapes:", df1.shape, df2.shape)
        if df1.shape[0] == df2.shape[0]:
            print("Columns missing in df1:", set(df2.columns).difference(set(df1.columns)))
            print("Columns missing in df2:", set(df1.columns).difference(set(df2.columns)))
        return False

    if join_key:
        df1 = df1.set_index(join_key)
        df2 = df2.set_index(join_key)
        df1, df2 = df1.align(df2, join='inner', axis=0)

    array_columns, non_array_columns = get_array_columns(df1)

    comparison_result = df1[non_array_columns].compare(df2[non_array_columns])
    if comparison_result.empty:
        if compare_array_columns(df1, df2, array_columns):
            print("Dataframes are identical")
            return True
        else:
            return False
    else:
        print("Dataframes differ in non-array columns:")
        print(comparison_result)
        return False
