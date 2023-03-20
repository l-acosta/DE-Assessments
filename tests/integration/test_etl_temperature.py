import sys

sys.path.insert(0, ".")
import os
import pandas as pd
from etl_temperature import main
from pandas.testing import assert_frame_equal


def test_etl_temperature(capsys):
    # data
    input_data_path = "tests/resources/sample_city_temperature.csv"
    output_path = "tests/resources/test_sample_city_temperature.csv"
    expected_path = "tests/resources/expected_output_sample_city_temperature.csv"
    # running main, getting computed
    sys.argv = ["main.py", input_data_path, output_path]
    main()
    df_computed = pd.read_csv(output_path)
    os.remove(output_path)
    # get expectec
    df_expected = pd.read_csv(expected_path)
    # test
    assert_frame_equal(df_computed, df_expected)
