import pytest
from datetime import datetime, time
from cloudquery.sdk.scalar import Timestamp
import pandas as pd


@pytest.mark.parametrize(
    "input_value,expected_scalar",
    [
        (
            datetime.strptime("2006-01-02T15:04:05", "%Y-%m-%dT%H:%M:%S"),
            Timestamp(
                True,
                pd.to_datetime("2006-01-02 15:04:05"),
            ),
        ),
        (
            "2006-01-02T15:04:05Z",
            Timestamp(
                True,
                pd.to_datetime("2006-01-02 15:04:05Z"),
            ),
        ),
        (
            "2006-01-02T15:04:05Z07:00",
            Timestamp(
                True,
                pd.to_datetime("2006-01-02T15:04:05Z07:00"),
            ),
        ),
        (
            "2006-01-02 15:04:05.999999999 -0700",
            #            "2006-01-02 15:04:05.999999999 -0700 MST",
            Timestamp(
                True,
                pd.to_datetime("2006-01-02 15:04:05.999999999 -0700"),
            ),
        ),
        (
            "2006-01-02 15:04:05.999999999",
            Timestamp(
                True,
                pd.to_datetime("2006-01-02 15:04:05.999999999"),
            ),
        ),
        (
            "2006-01-02 15:04:05.999999999Z",
            Timestamp(
                True,
                pd.to_datetime("2006-01-02 15:04:05.999999999Z"),
            ),
        ),
    ],
)
def test_timestamp_set(input_value, expected_scalar):
    b = Timestamp()
    b.set(input_value)
    assert b == expected_scalar, f"{b} and {expected_scalar} are not equal"


@pytest.mark.parametrize(
    "input_value",
    [
        "2026-10-21T10:00:00Z",
        datetime(2026, 10, 21, 10),
        pd.Timestamp("2026-10-21T10:00:00Z"),
        time(10, 30),
    ],
)
def test_timestamp_set_none_invalidates_and_can_be_reused(input_value):
    scalar = Timestamp()
    assert not scalar.is_valid
    scalar.set(input_value)
    assert scalar.is_valid

    scalar.set(None)
    assert not scalar.is_valid
    assert str(scalar) == "null"

    scalar.set(None)
    assert not scalar.is_valid

    scalar.set(input_value)
    assert scalar.is_valid
    assert scalar == Timestamp(True, input_value)
