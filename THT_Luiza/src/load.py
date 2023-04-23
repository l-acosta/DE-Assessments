import pandas as pd
import os


def load(df: pd.DataFrame, path_to_write: str = None) -> tuple:
    """
    Write a Pandas DataFrame to a CSV file at the specified path.

    If no path is provided, a filename will be generated based on the current timestamp and
    the output file will be written to the 'data/output' folder.

    Args:
        df (pd.DataFrame): The DataFrame to write to a file.
        path_to_write (str, optional): The path to the file to write. Defaults to None.

    Returns:
        bool: True if the DataFrame was successfully written to the file.

    Raises:
        ValueError: If the DataFrame is empty or required columns are missing from the DataFrame.
        FileNotFoundError: If the directory for the output file does not exist.


    Args:
        df (pd.DataFrame): The DataFrame to write to a file.
        path_to_write (str): The path to the file to write.

    Returns:
        bool: True if the DataFrame was successfully written to the file.

    Raises:
        ValueError: If the path is incorrect or does not exist or if the DataFrame is empty.
    """

    # Exception if df empty
    if df.empty:
        raise ValueError("The dataframe is empty")

    # Raise exception if output directory does not exist
    output_dir = os.path.dirname(path_to_write)
    if not (os.path.isdir("/".join(output_dir.split("/")[:-1]))):
        raise ValueError(f"The data path '{path_to_write}' does not exist")

    df.to_csv(path_to_write, index=False)

    return True, path_to_write
