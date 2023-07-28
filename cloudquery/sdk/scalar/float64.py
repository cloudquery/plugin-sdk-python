from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Float64(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Float64:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value: any):
        if value is None:
            self._valid = False
            return

        if type(value) == int:
            self._value = float(value)
        elif type(value) == float:
            self._value = value
        elif type(value) == str:
            try:
                self._value = float(value)
            except ValueError:
                raise ScalarInvalidTypeError("Invalid type for Float64 scalar")
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for Float64 scalar".format(type(value))
            )
        self._valid = True
