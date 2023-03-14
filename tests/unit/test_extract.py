import sys

sys.path.insert(0, ".")

import pytest
from etl.extract import extract


def test_extract_path_not_exist():
    """
    Test that an error is raised when the specified data path does not exist.
    """
    # Define the data path
    path = "wrong_path"

    # Test that an error is raised when attempting to extract data from the path
    with pytest.raises(ValueError) as e:
        extract(path=path)

    assert str(e.value) == f"The data path '{path}' does not exist"

    assert str(e.value) == f"The data path '{path}' does not exist"


def test_extract_empty_dataframe():
    """
    Test that an error is raised when attempting to extract an empty DataFrame.
    """
    # Define the data path
    path = "tests/resources/empty_city_temperature.csv"

    # Test that an error is raised when attempting to extract data from the path
    with pytest.raises(ValueError) as e:
        extract(path=path)

    assert str(e.value) == f"Dataframe at '{path}' is empty"

    assert str(e.value) == f"Dataframe at '{path}' is empty"


def test_miss_column_dataframe():
    """
    Test that an error is raised when attempting to extract a DataFrame missing
    one or more of the necessary columns.
    """
    # Define the data path
    path = "tests/resources/miss_col_city_temperature.csv"

    # Test that an error is raised when attempting to extract data from the path
    with pytest.raises(ValueError) as e:
        extract(path=path)

    assert str(e.value) == f"AvgTemperature are missing in the dataframe at '{path}'"
