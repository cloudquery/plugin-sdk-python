from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class String(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, String):
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

        if isinstance(value, str):
            self._valid = True
            self._value = value
        else:
            raise ScalarInvalidTypeError(
                f"Invalid type {type(value)} for String scalar"
            )
