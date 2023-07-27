from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Int64(Scalar):
    def __init__(self, valid: bool = False, value: float = None):
        self._valid = valid
        self._value = value

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Int64:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value):
        if value is None:
            return

        if type(value) == int:
            self._value = value
        elif type(value) == float:
            self._value = int(value)
        elif type(value) == str:
            try:
                self._value = int(value)
            except ValueError as e:
                raise ScalarInvalidTypeError("Invalid type for Int64 scalar") from e
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for Int64 scalar".format(type(value))
            )
        self._valid = True
