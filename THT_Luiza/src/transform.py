import pandas as pd
import datetime as dt
import numpy as np


def find_near_events(orders: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """
    Finds the nearest preceding and following event for each order in the orders dataframe
    based on the creation time of events associated with each device ID.
    The output dataframe contains the order ID, device ID, order creation time,
    preceding event time, and following event time.

    Args:
        orders (pd.DataFrame): A Pandas DataFrame containing orders data. It must have the following columns:
            - order_id
            - device_id
            - order_creation_time
        events (pd.DataFrame): A Pandas DataFrame containing information about the polling events associated with each device ID.
        It must have the following columns:
            - device_id
            - creation_time

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the following columns:
            - order_id
            - device_id
            - order_creation_time
            - preceding_event
            - following_event
    """
    # Merge dataframes to find preceding polling event
    df_merged_preceding = pd.merge_asof(
        orders.sort_values("order_creation_time"),
        events.sort_values("creation_time"),
        left_on="order_creation_time",
        right_on="creation_time",
        by="device_id",
        direction="backward",
    )
    df_merged_preceding = df_merged_preceding[
        ["order_id", "device_id", "order_creation_time", "creation_time"]
    ].rename(columns={"creation_time": "preceding_event"})

    # Second merge to find following polling event
    df_merged_following = pd.merge_asof(
        df_merged_preceding.sort_values("order_creation_time"),
        events.sort_values("creation_time"),
        left_on="order_creation_time",
        right_on="creation_time",
        by="device_id",
        direction="forward",
    )
    result = df_merged_following[
        [
            "order_id",
            "device_id",
            "order_creation_time",
            "preceding_event",
            "creation_time",
        ]
    ].rename(columns={"creation_time": "following_event"})

    return result


def find_connectivity_status(
    orders: pd.DataFrame, conn_status: pd.DataFrame
) -> pd.DataFrame:
    """
    Finds the nearest previous connectivity status of each device associated with an order in the orders dataframe
    based on the creation time of connectivity status records associated with each device ID.

    Args:
        orders (pd.DataFrame): A Pandas DataFrame containing information about the orders. It must have the following columns:
            - order_id
            - device_id
            - order_creation_time
        conn_status (pd.DataFrame): A Pandas DataFrame containing information about the connectivity status of devices.
        It must have the following columns:
            - device_id
            - creation_time
            - status

    Returns:
        pd.DataFrame: A Pandas DataFrame containing the following columns:
            - order_id
            - device_id
            - order_creation_time
            - preceding_event
            - following_event
            - status_time
            - previous_status
    """
    # Merge dataframes to find preceding polling event
    df_connect_status = pd.merge_asof(
        orders.sort_values("order_creation_time"),
        conn_status.sort_values("creation_time"),
        left_on="order_creation_time",
        right_on="creation_time",
        by="device_id",
        direction="backward",
    )
    df_connect_status = df_connect_status[
        [
            "order_id",
            "device_id",
            "order_creation_time",
            "preceding_event",
            "following_event",
            "status",
            "creation_time",
        ]
    ].rename(columns={"creation_time": "status_time", "status": "previous_status"})
    return df_connect_status


def count_polling_events(df: pd.DataFrame, time_ref: str) -> pd.DataFrame:
    """
    Computes number of pooling events related to orders in the input DataFrame
    across the already filtered period of time. The output is a DataFrame with
    the counts of:
      - The total count of all polling events
      - The count of each type of polling status_code
      - The count of each type of polling error_code
      - The count of responses with no error_codes.

    Args:
        df (pd.DataFrame): A DataFrame containing the orders and their related polling events.
        time_ref (str): A suffix to be added to the output column names indicating the time period.

    Returns:
        pd.DataFrame: A DataFrame containing the counts for each of the 3 periods of time, with the following columns:
          - order_id: The ID of the order
          - count_total_events_{time_ref}: The total number of polling events for the order within the time period.
          - count_status_code_0_{time_ref}: The number of polling events with a status_code of 0 within the time period.
          - count_status_code_200_{time_ref}: The number of polling events with a status_code of 200 within the time period.
          - count_status_code_401_{time_ref}: The number of polling events with a status_code of 401 within the time period.
          - count_error_econnaborted_{time_ref}: The number of polling events with an error_code of 'ECONNABORTED' within the time period.
          - count_error_generic_error_{time_ref}: The number of polling events with an error_code of 'GENERIC_ERROR' within the time period.
          - count_no_error_code_{time_ref}: The number of polling events with no error_code within the time period.
    """

    events_count = (
        df.groupby("order_id")
        .agg(
            count_total_events=("status_code", "count"),
            count_status_code_0=("status_code", lambda x: np.sum(x == 0)),
            count_status_code_200=("status_code", lambda x: np.sum(x == 200)),
            count_status_code_401=("status_code", lambda x: np.sum(x == 401)),
            count_error_econnaborted=(
                "error_code",
                lambda x: np.sum(x == "ECONNABORTED"),
            ),
            count_error_generic_error=(
                "error_code",
                lambda x: np.sum(x == "GENERIC_ERROR"),
            ),
            count_no_error_code=("error_code", lambda x: np.sum(x.isnull())),
        )
        .reset_index()
    )

    new_column_names = [
        col + time_ref if col != "order_id" else col for col in events_count.columns
    ]
    events_count.columns = new_column_names

    return events_count


def transformation(
    df_polling: pd.DataFrame,
    df_connectivity_status: pd.DataFrame,
    df_orders: pd.DataFrame,
) -> pd.DataFrame:
    """
    Applies a series of transformations to the input DataFrames in order to
    produce a report about the connectivity environment of a device in the
    period of time surrounding when an order is dispatched to it.

    Args:
      df_polling (pd.DataFrame): Input DataFrame containing client HTTP endpoint
          polling event data for a set of devices running a web application
      df_connectivity_status (pd.DataFrame): Input DataFrame containing internet
          connectivity status logs for the above set of devices, generated when
          a device goes offline whilst running the application
      df_orders (pd.DataFrame): Input DataFrame containing data for orders that
          have been dispatched to devices running the above web application

    Returns:
      pd.DataFrame :  DataFrame with columns for order information, preceding and
      following polling events, connectivity status and polling events counts.
    """

    # Convert creation_time to datetime
    df_polling["creation_time"] = pd.to_datetime(
        df_polling["creation_time"], errors="coerce"
    )
    df_connectivity_status["creation_time"] = pd.to_datetime(
        df_connectivity_status["creation_time"], errors="coerce"
    )
    df_orders["order_creation_time"] = pd.to_datetime(
        df_orders["order_creation_time"], errors="coerce"
    )

    # Sort dataframes by time
    df_polling = df_polling.sort_index().sort_index(axis=1)
    df_connectivity_status = df_connectivity_status.sort_index(axis=1)
    df_orders = df_orders.sort_index().sort_index(axis=1)

    # Create base of orders for the output df
    df_base = df_orders[["order_id", "device_id", "order_creation_time"]]

    # Drop orders that don't have a corresponding device_id
    df_base = df_base[df_base["device_id"].notna()]

    # Gathering report information
    df_stg_phase1 = find_near_events(orders=df_base, events=df_polling)
    df_stg_phase2 = find_connectivity_status(
        orders=df_stg_phase1, conn_status=df_connectivity_status
    )

    # Merge original orders base with pooling events to perform the counts
    df_merged = pd.merge(df_base, df_polling, on="device_id")

    # Define time periods and compute the counts
    time_period = [-3, 3, -60]
    df_counts = df_base[["order_id"]]

    for t in time_period:
        if t > 0:
            ref = f"_after_{abs(t)}min"
            merged_df_filtered = df_merged[
                (df_merged["creation_time"] >= df_merged["order_creation_time"])
                & (
                    df_merged["creation_time"]
                    <= df_merged["order_creation_time"] + dt.timedelta(minutes=t)
                )
            ]
        else:
            ref = f"_before_{abs(t)}min"
            merged_df_filtered = df_merged[
                (
                    df_merged["creation_time"]
                    >= df_merged["order_creation_time"] + dt.timedelta(minutes=t)
                )
                & (df_merged["creation_time"] <= df_merged["order_creation_time"])
            ]

        count_events = count_polling_events(df=merged_df_filtered, time_ref=ref)

        df_counts = pd.merge(df_counts, count_events, on="order_id")

    # Merge final information
    df_output = pd.merge(df_stg_phase2, df_counts, on="order_id")

    return df_output
