import sys

sys.path.insert(0, ".")

import pytest
from src.extract import extract


def test_extract_path_not_exist():
    """
    Test that an error is raised when the specified data path does not exist.
    """
    # Define the data path
    path = "wrong_path"

    # Test that an error is raised when attempting to extract data from the path
    with pytest.raises(ValueError) as e:
        extract(path_polling=path, path_conn_status=path, path_orders=path)

    assert str(e.value) == f"The data path '{path}' does not exist"
