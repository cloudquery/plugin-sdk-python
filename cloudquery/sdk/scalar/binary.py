from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError
from .scalar import NULL_VALUE


class Binary(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, Binary):
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

        if isinstance(value, bytes):
            self._valid = True
            self._value = value
        elif isinstance(value, str):
            self._valid = True
            self._value = value.encode()
        else:
            raise ScalarInvalidTypeError(
                f"Invalid type {type(value)} for Binary scalar"
            )
