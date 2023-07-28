import pytest
from cloudquery.sdk.scalar import Int


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (1, Int(True, 1, 64)),
        ("1", Int(True, 1, 64)),
    ],
)
def test_int_set(input_value, expected_scalar):
    b = Int(bitwidth=64)
    b.set(input_value)
    assert b == expected_scalar
