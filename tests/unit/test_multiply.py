from src.multiply import multiply


def test_multiply():
    # data
    a, b = 2, 3
    # compute expected
    expected = 6
    # compute real
    real = multiply(a, b)
    # compute real vs. expected
    assert expected == real
