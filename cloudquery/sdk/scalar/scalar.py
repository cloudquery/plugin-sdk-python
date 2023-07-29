from abc import abstractmethod

NULL_VALUE = "null"


class ScalarInvalidTypeError(Exception):
    pass


class Scalar:
    def __init__(self, valid: bool = False, value: any = None):
        self._valid = valid
        self._value = None
        self.set(value)

    @property
    def is_valid(self) -> bool:
        return self._valid

    def __str__(self) -> str:
        return str(self._value) if self._valid else NULL_VALUE

    @property
    @abstractmethod
    def value(self):
        pass

    @abstractmethod
    def set(self, value: any):
        pass
