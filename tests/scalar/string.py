import pytest
import uuid
from cloudquery.sdk.scalar import String


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (
            "foo",
            String(True, "foo"),
        ),
    ],
)
def test_string_set(input_value, expected_scalar):
    b = String()
    b.set(input_value)
    assert b == expected_scalar
