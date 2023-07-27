from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError
from .scalar import NULL_VALUE


class Binary(Scalar):
    def __init__(self, valid: bool = False, value: bytes = None):
        self._valid = valid
        self._value = value

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Binary:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    def __str__(self) -> str:
        return str(self._value) if self._valid else NULL_VALUE

    @property
    def value(self):
        return self._value

    def set(self, scalar):
        if scalar is None:
            return

        if type(scalar) == bytes:
            self._valid = True
            self._value = scalar
        elif type(scalar) == str:
            self._valid = True
            self._value = scalar.encode()
        else:
            raise ScalarInvalidTypeError("Invalid type for Binary scalar")
