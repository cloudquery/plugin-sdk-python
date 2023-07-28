from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError
from .scalar import NULL_VALUE


class Binary(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Binary:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, scalar):
        if scalar is None:
            self._valid = False
            return

        if type(scalar) == bytes:
            self._valid = True
            self._value = scalar
        elif type(scalar) == str:
            self._valid = True
            self._value = scalar.encode()
        else:
            raise ScalarInvalidTypeError("Invalid type for Binary scalar")
