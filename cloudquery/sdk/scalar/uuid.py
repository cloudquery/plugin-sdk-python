import uuid
from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class UUID(Scalar):
    def __init__(self, valid: bool = False, value: uuid.UUID = None):
        super().__init__(valid, value)

    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, UUID):
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value: any):
        if value is None:
            self._valid = False
            return

        if isinstance(value, UUID):
            self._valid = value.is_valid
            self._value = value.value
            return

        if isinstance(value, uuid.UUID):
            self._value = value
        elif isinstance(value, str):
            try:
                self._value = uuid.UUID(value)
            except ValueError as e:
                raise ScalarInvalidTypeError("Invalid type for UUID scalar") from e
        else:
            raise ScalarInvalidTypeError(f"Invalid type {type(value)} for UUID scalar")
        self._valid = True
