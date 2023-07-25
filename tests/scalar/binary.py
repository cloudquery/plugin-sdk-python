import pytest
from cloudquery.sdk.scalar import Binary


@pytest.mark.parametrize("input_value,expected_scalar", [
    (b'123', Binary(True, b'123')),
    (b'', Binary(True, b'')),
    (None, Binary()),
    (bytes([1,2,3]), Binary(True, b'\x01\x02\x03')),
])
def test_binary_set(input_value, expected_scalar):
    b = Binary()
    b.set(input_value)
    assert b == expected_scalar
