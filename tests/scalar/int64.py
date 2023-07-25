import pytest
from cloudquery.sdk.scalar import Int64

@pytest.mark.parametrize("input_value,expected_scalar", [
    (1, Int64(True, float(1))),
    ("1", Int64(True, float(1))),
])
def test_binary_set(input_value, expected_scalar):
    b = Int64()
    b.set(input_value)
    assert b == expected_scalar
