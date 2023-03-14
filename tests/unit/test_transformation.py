import sys

sys.path.insert(0, ".")
from etl.transformation import (
    clean_df,
    compute_avg_temp_month,
    compute_diff_vs_last_year,
)
import pandas as pd
from pandas.testing import assert_frame_equal


def test_transformation_cleaning():
    """
    Test the cleaning function to remove duplicates and NaN values.
    """
    # data
    dirty_data = pd.DataFrame(
        [
            # duplicate rows
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Day": 1,
                "Year": 2010,
                "AvgTemperature": 59.1,
            },
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Day": 1,
                "Year": 2010,
                "AvgTemperature": 59.1,
            },
            # row with nan
            {
                "Region": "Africa",
                "Country": None,
                "Month": 1,
                "Day": 2,
                "Year": 2010,
                "AvgTemperature": 55.6,
            },
            # row with temperature wrong
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Day": 2,
                "Year": 2010,
                "AvgTemperature": -99.0,
            },
        ]
    )
    # expected
    expected = pd.DataFrame(
        [
            # duplicate rows
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Day": 1,
                "Year": 2010,
                "AvgTemperature": 59.1,
            }
        ]
    )
    # compute
    computed = clean_df(dirty_data)
    # tests
    assert_frame_equal(computed, expected)


def test_compute_avg_temperature_by_month_country_continent():
    """
    Test the computation of average temperature by month, country, and continent.
    """
    # data
    data = pd.DataFrame(
        [
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Day": 1,
                "Year": 2010,
                "AvgTemperature": 1.0,
            },
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Day": 2,
                "Year": 2010,
                "AvgTemperature": 3.0,
            },
        ]
    )
    # expected
    expected = pd.DataFrame(
        [
            # duplicate rows
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Year": 2010,
                "AvgTemperature": 2.0,
            },
        ]
    )
    # computed
    computed = compute_avg_temp_month(data)
    # tests
    assert_frame_equal(computed, expected)


def test_compute_diff_vs_last_year():
    """
    Test the `compute_diff_vs_last_year` function.

    The function computes the difference in average temperature between two consecutive years
    for each month, country, and region combination.

    The test creates a DataFrame with two rows, one for 2010 and one for 2011, for Algeria
    in the Africa region. The computed result should show a difference of 2.0 for January 2011
    compared to January 2010.

    """
    # data
    data = pd.DataFrame(
        [
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Year": 2010,
                "AvgTemperature": 1.0,
            },
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Year": 2011,
                "AvgTemperature": 3.0,
            },
        ]
    )
    # expected
    expected = pd.DataFrame(
        [
            # duplicate rows
            {
                "Region": "Africa",
                "Country": "Algeria",
                "Month": 1,
                "Year": 2011,
                "AvgTemperature": 3.0,
                "diff_AvgTemperature_vs_last_year": 2.0,
            },
        ]
    )
    # computed
    computed = compute_diff_vs_last_year(data)
    # tests
    assert_frame_equal(computed, expected)
