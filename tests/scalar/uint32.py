import pytest
from cloudquery.sdk.scalar import Uint


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (1, Uint(True, 1, 32)),
        ("1", Uint(True, 1, 32)),
    ],
)
def test_uint_set(input_value, expected_scalar):
    b = Uint(bitwidth=32)
    b.set(input_value)
    assert b == expected_scalar
