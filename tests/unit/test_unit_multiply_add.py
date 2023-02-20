import sys

sys.path.insert(0, ".")
from src.multiply import multiply
from src.addition import addition


def test_multiply():
    # data
    a, b = 2, 3
    # compute expected
    expected = 6
    # compute real
    real = multiply(a, b)
    # compute real vs. expected
    assert expected == real


def test_addition():
    # data
    a, b = 2, 3
    # compute expected
    expected = 5
    # compute real
    real = addition(a, b)
    # compute real vs. expected
    assert expected == real
