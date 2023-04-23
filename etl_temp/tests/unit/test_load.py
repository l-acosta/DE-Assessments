import sys
import os
import pandas as pd
import pytest

sys.path.insert(0, ".")
from datetime import date
from etl.load import load


def test_if_exception_if_path_not_exist():
    """Test that an exception is raised when a file path is incorrect or does not exist."""
    # data
    path_to_write = "wrong_path"
    df = pd.DataFrame(
        [
            {
                "Region": "Africa",
                "Country": "Algeria",
                "AvgTemperature": 1.0,
                "date": date(2020, 5, 17),
                "diff_AvgTemperature_vs_last_year": 2.0,
            },
        ]
    )
    # test
    with pytest.raises(ValueError) as e:
        load(df=df, path_to_write=path_to_write)

    assert str(e.value) == f"The data path '{path_to_write}' does not exist"


def test_if_exception_if_column_are_wrong():
    """Test that an exception is raised when required columns are missing from the dataframe."""
    # data
    path_to_write = os.path.dirname(os.path.abspath(__file__))
    df = pd.DataFrame(
        [
            {
                "Wrong": "Africa",
                "Country": "Algeria",
                "AvgTemperature": 1.0,
                "date": date(2020, 5, 17),
                "diff_AvgTemperature_vs_last_year": 2.0,
            },
        ]
    )
    # test
    with pytest.raises(ValueError) as e:
        load(df=df, path_to_write=path_to_write)

    assert str(e.value) == "Region are missing in the dataframe"


def test_if_exception_if_df_empty():
    """Test that an exception is raised when the dataframe is empty."""
    # data
    path_to_write = os.path.dirname(os.path.abspath(__file__))
    df = pd.DataFrame(
        columns=[
            "date",
            "Region",
            "Country",
            "diff_AvgTemperature_vs_last_year",
            "AvgTemperature",
        ]
    )
    # test
    with pytest.raises(ValueError) as e:
        load(df=df, path_to_write=path_to_write)

    assert str(e.value) == "The dataframe is empty"


def test_if_df_is_written():
    """Test that the dataframe is written to a CSV file and the file can be read."""
    # data
    path_to_write = "tests/resources/output.csv"
    df = pd.DataFrame(
        [
            {
                "Region": "Africa",
                "Country": "Algeria",
                "AvgTemperature": 1.0,
                "date": date(2020, 5, 17),
                "diff_AvgTemperature_vs_last_year": 2.0,
            },
        ]
    )
    # call the function
    load(path_to_write=path_to_write, df=df)
    # tests
    try:
        pd.read_csv(path_to_write)
        os.remove(path_to_write)
        assert True
    except FileNotFoundError:
        assert False
