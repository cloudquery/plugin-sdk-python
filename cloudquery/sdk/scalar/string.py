from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError
from .scalar import NULL_VALUE


class String(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == String:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value: any):
        if value is None:
            return

        if isinstance(value, String):
            self._valid = value._valid
            self._value = value.value
            return

        if type(value) == str:
            self._valid = True
            self._value = value
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for String scalar".format(type(value))
            )
