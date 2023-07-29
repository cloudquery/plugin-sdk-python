import pytest
from cloudquery.sdk.scalar import Float


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (1, Float(True, float(1), 32)),
        ("1", Float(True, float(1), 32)),
    ],
)
def test_float32_set(input_value, expected_scalar):
    b = Float(bitwidth=32)
    b.set(input_value)
    assert b == expected_scalar
