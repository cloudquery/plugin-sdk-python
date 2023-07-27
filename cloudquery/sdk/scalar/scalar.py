NULL_VALUE = "null"


class ScalarInvalidTypeError(Exception):
    pass


class Scalar:
    @property
    def is_valid(self) -> bool:
        return self._valid

    @property
    def value(self):
        raise NotImplementedError("Scalar value not implemented")
