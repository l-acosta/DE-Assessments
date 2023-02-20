from src.addition import addition


def test_addition():
    # data
    a, b = 2, 3
    # compute expected
    expected = 5
    # compute real
    real = addition(a, b)
    # compute real vs. expected
    assert expected == real
