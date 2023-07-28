from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Float(Scalar):
    def __init__(self, valid: bool = False, value: any = None, bitwidth: int = 64):
        super().__init__(valid, value)
        self._bitwidth = bitwidth

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Float and self._bitwidth == scalar.bitwidth:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def bitwidth(self):
        return self._bitwidth

    @property
    def value(self):
        return self._value

    def set(self, value: any):
        if value is None:
            self._valid = False
            return

        if isinstance(value, Float) and value.bitwidth == self._bitwidth:
            self._valid = value.is_valid
            self._value = value.value
            return

        if type(value) == int:
            self._value = float(value)
        elif type(value) == float:
            self._value = value
        elif type(value) == str:
            try:
                self._value = float(value)
            except ValueError:
                raise ScalarInvalidTypeError(
                    "Invalid value for Float{} scalar".format(self._bitwidth)
                )
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for Float{} scalar".format(type(value), self._bitwidth)
            )
        self._valid = True
