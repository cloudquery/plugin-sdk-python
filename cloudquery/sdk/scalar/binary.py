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

    def set(self, value: any):
        if value is None:
            return

        if isinstance(value, Binary):
            self._valid = value.is_valid
            self._value = value.value
            return

        if type(value) == bytes:
            self._valid = True
            self._value = value
        elif type(value) == str:
            self._valid = True
            self._value = value.encode()
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for Binary scalar".format(type(value))
            )
