from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError, NULL_VALUE
from datetime import datetime, time
from typing import Any


class Date32(Scalar):
    def __init__(self, valid: bool = False, value: bool = False) -> None:
        self._valid = valid
        self._value = value

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Date32:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    def __str__(self) -> str:
        return str(self._value) if self._valid else NULL_VALUE

    @property
    def value(self):
        return self._value

    def set(self, value: Any):
        if value is None:
            return

        if type(value) == datetime:
            self._value = value
        elif type(value) == str:
            self._value = datetime.strptime(value, "%Y-%m-%d")
        elif type(value) == time:
            self._value = datetime.combine(datetime.today(), value)
        else:
            raise ScalarInvalidTypeError("Invalid type for Bool scalar")

        self._valid = True
