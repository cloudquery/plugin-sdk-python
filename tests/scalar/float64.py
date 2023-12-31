import pytest
from cloudquery.sdk.scalar import Float


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (1, Float(True, float(1), 64)),
        ("1", Float(True, float(1), 64)),
    ],
)
def test_float64_set(input_value, expected_scalar):
    b = Float(bitwidth=64)
    b.set(input_value)
    assert b == expected_scalar
