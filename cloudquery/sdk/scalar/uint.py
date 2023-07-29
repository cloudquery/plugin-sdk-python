from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Uint(Scalar):
    def __init__(self, valid: bool = False, value: any = None, bitwidth: int = 64):
        self._bitwidth = bitwidth
        self._max = 2**bitwidth
        super().__init__(valid, value)

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == Uint:
            return (
                self._bitwidth == scalar.bitwidth
                and self._value == scalar._value
                and self._valid == scalar._valid
            )
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

        if isinstance(value, Uint) and value.bitwidth == self._bitwidth:
            self._valid = value.is_valid
            self._value = value.value
            return

        if type(value) == int:
            val = value
        elif type(value) == float:
            val = int(value)
        elif type(value) == str:
            try:
                val = int(value)
            except ValueError as e:
                raise ScalarInvalidTypeError(
                    "Invalid value for Int{} scalar".format(self._bitwidth)
                ) from e
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for Int{} scalar".format(type(value), self._bitwidth)
            )

        if val < 0 or val >= self._max:
            raise ScalarInvalidTypeError(
                "Invalid Uint{} scalar".format(self._bitwidth, val)
            )
        self._value = val
        self._valid = True
