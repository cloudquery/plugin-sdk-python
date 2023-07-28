from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Int(Scalar):
    def __init__(self, valid: bool = False, value: any = None, bitwidth: int = 64):
        super().__init__(valid, value)
        self._bitwidth = bitwidth

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Int and self._bitwidth == scalar.bitwidth:
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

        if type(value) == int:
            self._value = value
        elif type(value) == float:
            self._value = int(value)
        elif type(value) == str:
            try:
                self._value = int(value)
            except ValueError as e:
                raise ScalarInvalidTypeError(
                    "Invalid type for Int{} scalar".format(self._bitwidth)
                ) from e
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for Int{} scalar".format(type(value), self._bitwidth)
            )
        self._valid = True
