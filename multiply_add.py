from src.multiply import multiply
from src.addition import addition


def multiply_add(a, b, c):
    # multiply a and b
    product = multiply(a, b)
    # add product to c
    return addition(product, c)
