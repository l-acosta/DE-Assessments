import pandas as pd
from datetime import date


def clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans the input DataFrame by dropping any rows with missing values or duplicates, and removing any rows where
    the 'AvgTemperature' column is equal to -99.0.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
    """
    df = df.dropna().drop_duplicates()
    df = df[df["AvgTemperature"] != -99.0]
    return df


def compute_avg_temp_month(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the average temperature for each month, region, and country in the input DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with columns for region, country, month, year, and average temperature.
    """
    group_by_col = ["Region", "Country", "Month", "Year"]
    num_col = ["AvgTemperature"]
    return (
        df.groupby(group_by_col)
        .mean(numeric_only=True)
        .reset_index()[group_by_col + num_col]
    )


def compute_diff_vs_last_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes the difference between the average temperature in the current year and the previous year for each region,
    country, and month in the input DataFrame.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with columns for region, country, month, year, and the difference in average temperature
        between the current year and the previous year.
    """
    join_col = ["Region", "Country", "Month", "Year"]
    diff_col = "diff_AvgTemperature_vs_last_year"

    # Create a copy of the DataFrame and add 1 to the 'Year' column to create a DataFrame for the previous year.
    df_last_year = df.copy()
    df_last_year["Year"] = df_last_year["Year"] + 1

    # Merge the current year DataFrame with the previous year DataFrame on the join columns, and compute the difference
    # in average temperature between the two years.
    df_vs_last_year = df.merge(
        df_last_year, how="inner", on=join_col, suffixes=("", "_prev")
    )
    df_vs_last_year[diff_col] = (
        df_vs_last_year["AvgTemperature"] - df_vs_last_year["AvgTemperature_prev"]
    )
    return df_vs_last_year[join_col + ["AvgTemperature"] + [diff_col]]


def transformation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies a series of transformations to the input DataFrame to compute the difference in average temperature between
    the current year and the previous year for each region, country, and month.

    Args:
        df (pd.DataFrame): Input DataFrame.

    Returns:
        pd.DataFrame: DataFrame with columns for date, region, country, and the difference in average temperature
        between the current year and the previous year.
    """
    df = clean_df(df)
    df = compute_avg_temp_month(df)
    df = compute_diff_vs_last_year(df)

    # Compute a 'date' column based on the 'Year' and 'Month' columns.
    df["date"] = df.apply(
        lambda row: date(year=row["Year"], month=row["Month"], day=1), axis=1
    )
    return df[
        [
            "date",
            "Region",
            "Country",
            "AvgTemperature",
            "diff_AvgTemperature_vs_last_year",
        ]
    ]
