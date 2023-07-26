import pytest
import uuid
from cloudquery.sdk.scalar import UUID

@pytest.mark.parametrize("input_value,expected_scalar", [
    ("550e8400-e29b-41d4-a716-446655440000", UUID(True, uuid.UUID("550e8400-e29b-41d4-a716-446655440000"))),
])
def test_binary_set(input_value, expected_scalar):
    b = UUID()
    b.set(input_value)
    assert b == expected_scalar
