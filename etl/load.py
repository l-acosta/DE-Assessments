import pandas as pd
import os


def load(df: pd.DataFrame, path_to_write: str) -> bool:
    """
    Write a Pandas DataFrame to a CSV file at the specified path.

    Args:
        df (pd.DataFrame): The DataFrame to write to a file.
        path_to_write (str): The path to the file to write.

    Returns:
        bool: True if the DataFrame was successfully written to the file.

    Raises:
        ValueError: If the path is incorrect or does not exist, the DataFrame is empty,
            or required columns are missing from the DataFrame.
    """
    # Catch wrong paths
    if not (os.path.isdir("/".join(path_to_write.split("/")[:-1]))):
        raise ValueError(f"The data path '{path_to_write}' does not exist")

    # Exception if df empty
    if df.empty:
        raise ValueError("The dataframe is empty")

    # Exception if columns are wrong
    exactly_expected_columns = [
        "date",
        "Region",
        "Country",
        "diff_AvgTemperature_vs_last_year",
        "AvgTemperature",
    ]
    if set(exactly_expected_columns) != set(df.columns):
        # Find the names of the missing columns
        missing_columns = ", ".join(
            list(set(exactly_expected_columns) - set(df.columns))
        )
        raise ValueError(f"{missing_columns} are missing in the dataframe")

    df.to_csv(path_to_write, index=False)
    return True
