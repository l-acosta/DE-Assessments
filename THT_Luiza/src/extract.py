import pandas as pd
import os


def extract(path_polling: str, path_conn_status: str, path_orders: str) -> tuple:
    """
    Reads CSV files from the specified paths returns them as a tuple with 3 pandas DataFrame.

    Args:
        path_polling (str): The path to the polling CSV file
        path_conn_status (str): The path to the connectivity status CSV file
        path_orders (str): The path to the orders CSV file

    Returns:
        tuple: A tuple of the 3 dataframes containing the following columns:
        - df_polling:
            - creation_time
            - device_id
            - error_code
            - status_code
        - df_conn_status
            - creation_time
            - status
            - device_id
        - df_orders
            - device_id
            - order_creation_time
            - order_id

    Raises:
        ValueError: If the specified path does not exist.
    """

    for p in [path_polling, path_conn_status, path_orders]:
        output_dir = os.path.dirname(p)
        if not os.path.isdir(output_dir):
            raise ValueError(f"The data path '{p}' does not exist")

    # Read the CSV files into a DataFrames
    df_polling = pd.read_csv(path_polling, header=[0], sep=",")
    df_conn_status = pd.read_csv(path_conn_status, header=[0], sep=",")
    df_orders = pd.read_csv(path_orders, header=[0], sep=",")

    return df_polling, df_conn_status, df_orders
