NULL_VALUE = "null"


class ScalarInvalidTypeError(Exception):
    pass


class Scalar:
    def __init__(self, valid: bool = False):
        self._valid = valid
        self._value = None

    @property
    def is_valid(self) -> bool:
        return self._valid

    @property
    def value(self):
        raise NotImplementedError("Scalar value not implemented")

    def __str__(self) -> str:
        return str(self._value) if self._valid else NULL_VALUE
