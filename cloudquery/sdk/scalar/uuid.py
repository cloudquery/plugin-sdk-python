import uuid
from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class UUID(Scalar):
    def __init__(self, valid: bool = False, value: uuid.UUID = None):
        self._valid = valid
        self._value = value

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if type(scalar) == UUID:
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value):
        if value is None:
            return

        if type(value) == uuid.UUID:
            self._value = value
        elif type(value) == str:
            try:
                self._value = uuid.UUID(value)
            except ValueError as e:
                raise ScalarInvalidTypeError("Invalid type for UUID scalar") from e
        else:
            raise ScalarInvalidTypeError(
                "Invalid type {} for UUID scalar".format(type(value))
            )
        self._valid = True
