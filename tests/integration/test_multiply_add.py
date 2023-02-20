from multiply_add import multiply_add


def test_multiply_add():
    # data
    a, b, c = 2, 3, 4
    # compute expected
    expected = 10
    # compute real
    real = multiply_add(a, b, c)
    # compute real vs. expected
    assert expected == real
