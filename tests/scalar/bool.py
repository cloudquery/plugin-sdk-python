import pytest
from cloudquery.sdk.scalar import Bool


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (True, Bool(True, True)),
        (False, Bool(True, False)),
        ("true", Bool(True, True)),
        ("false", Bool(True, False)),
    ],
)
def test_bool_set(input_value, expected_scalar):
    b = Bool()
    b.set(input_value)
    assert b == expected_scalar
