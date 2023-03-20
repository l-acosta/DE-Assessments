import pandas as pd
import os


def extract(path: str = "local/sample_city_temperature.csv") -> pd.DataFrame:
    """
    Reads a CSV file from the specified path and returns a pandas DataFrame
    containing the necessary columns.

    Args:
        path (str): The path to the CSV file.

    Returns:
        A pandas DataFrame containing the following columns:
        - Region
        - Country
        - City
        - Month
        - Day
        - Year
        - AvgTemperature

    Raises:
        ValueError: If the specified path does not exist, or if the DataFrame
        at the specified path is empty or missing one or more of the necessary columns.
    """
    if not (os.path.exists(path)):
        raise ValueError(f"The data path '{path}' does not exist")

    # Read the CSV file into a DataFrame
    df = pd.read_csv(path)

    if len(df) == 0:
        raise ValueError(f"Dataframe at '{path}' is empty")

    necessary_columns = [
        "Region",
        "Country",
        "City",
        "Month",
        "Day",
        "Year",
        "AvgTemperature",
    ]
    if not (set(necessary_columns).issubset(set(df.columns))):
        # Find the names of the missing columns
        missing_columns = ", ".join(list(set(necessary_columns) - set(df.columns)))
        raise ValueError(f"{missing_columns} are missing in the dataframe at '{path}'")

    # Return only the necessary columns
    return df[necessary_columns]
