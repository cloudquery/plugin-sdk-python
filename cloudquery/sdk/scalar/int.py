from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Int(Scalar):
    def __init__(self, valid: bool = False, value: any = None, bitwidth: int = 64):
        self._bitwidth = bitwidth
        self._min = -(2 ** (bitwidth - 1))
        self._max = 2 ** (bitwidth - 1)
        super().__init__(valid, value)

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, Int) and self._bitwidth == scalar.bitwidth:
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

        if isinstance(value, Int) and value.bitwidth == self._bitwidth:
            self._valid = value.is_valid
            self._value = value.value
            return

        if isinstance(value, int):
            val = value
        elif isinstance(value, float):
            val = int(value)
        elif isinstance(value, str):
            try:
                val = int(value)
            except ValueError as e:
                raise ScalarInvalidTypeError(
                    f"Invalid type for Int{self._bitwidth} scalar"
                ) from e
        else:
            raise ScalarInvalidTypeError(
                f"Invalid type {type(value)} for Int{self._bitwidth} scalar"
            )
        if val < self._min or val >= self._max:
            raise ScalarInvalidTypeError(
                f"Invalid Int{self._bitwidth} scalar with value {val}"
            )
        self._value = val
        self._valid = True
