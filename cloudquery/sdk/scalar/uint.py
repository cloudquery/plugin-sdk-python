from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class Uint(Scalar):
    def __init__(self, valid: bool = False, value: any = None, bitwidth: int = 64):
        self._bitwidth = bitwidth
        self._max = 2**bitwidth
        super().__init__(valid, value)

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, Uint):
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

        if isinstance(value, int):
            val = value
        elif isinstance(value, float):
            val = int(value)
        elif isinstance(value, str):
            try:
                val = int(value)
            except ValueError as e:
                raise ScalarInvalidTypeError(
                    f"Invalid value for Int{self._bitwidth} scalar"
                ) from e
        else:
            raise ScalarInvalidTypeError(
                f"Invalid type {type(value)} for Int{self._bitwidth} scalar"
            )

        if val < 0 or val >= self._max:
            raise ScalarInvalidTypeError(f"Invalid Uint{self._bitwidth} scalar {val}")
        self._value = val
        self._valid = True
