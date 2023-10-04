from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError
from .scalar import NULL_VALUE
from .vector import Vector
from typing import Any, Type


class List(Scalar):
    def __init__(self, scalar_type: Type[Scalar]):
        super().__init__(False, None)
        self._value = Vector(scalar_type)
        self._type = scalar_type

    def __eq__(self, other: "List") -> bool:
        if (not isinstance(other, self.__class__)) or self._valid != other._valid:
            return False
        return self._value == other._value

    @property
    def type(self):
        return self._type

    @property
    def value(self):
        return self._value

    def set(self, val: Any):
        if val is None:
            self._valid = False
            self._value = Vector()
            return

        if isinstance(val, (Scalar, self._type)):
            if not val.is_valid:
                self._valid = False
                self._value = Vector()
                return
            return self.set([val.value])

        if isinstance(val, (list, tuple)):
            self._value = Vector()
            for item in val:
                scalar = self._type()
                scalar.set(item)
                self._value.append(scalar)
                self._valid = True
            return

        raise ScalarInvalidTypeError(f"Invalid type {type(val)} for List")

    def __str__(self) -> str:
        if not self._valid:
            return NULL_VALUE
        return f"[{', '.join(str(v) for v in self._value)}]"

    def __len__(self):
        return len(self.value.data)
