import sys
import os
import pandas as pd
import pytest

sys.path.insert(0, ".")
from src.load import load


def test_if_exception_if_path_not_exist():
    """Test that an exception is raised when a file path is incorrect or does not exist."""
    # data
    path_to_write = "wrong_path"
    df = pd.DataFrame(
        [
            {
                "order_id": 123,
                "count_total_events_": 123,
            },
        ]
    )
    # test
    with pytest.raises(ValueError) as e:
        load(df=df, path_to_write=path_to_write)

    assert str(e.value) == f"The data path '{path_to_write}' does not exist"


def test_if_exception_if_df_empty():
    """Test that an exception is raised when the dataframe is empty."""
    # data
    path_to_write = os.path.dirname(os.path.abspath(__file__))
    df = pd.DataFrame(
        columns=[
            "order_id",
            "count_total_events_",
        ]
    )
    # test
    with pytest.raises(ValueError) as e:
        load(df=df, path_to_write=path_to_write)

    assert str(e.value) == "The dataframe is empty"
