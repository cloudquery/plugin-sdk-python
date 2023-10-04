from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError, NULL_VALUE
from datetime import datetime, time
from typing import Any
import pandas as pd


class Timestamp(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, Timestamp):
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value: Any):
        if value is None:
            return

        if isinstance(value, Timestamp):
            self._valid = value.is_valid
            self._value = value.value
            return

        if isinstance(value, pd.Timestamp):
            self._value = value
        elif isinstance(value, datetime):
            self._value = pd.to_datetime(value)
        elif isinstance(value, str):
            self._value = pd.to_datetime(value)
        elif isinstance(value, time):
            self._value = pd.to_datetime(datetime.combine(datetime.today(), value))
        else:
            raise ScalarInvalidTypeError(
                f"Invalid type {type(value)} for Timestamp scalar"
            )

        self._valid = True
