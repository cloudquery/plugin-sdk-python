import json
from cloudquery.sdk.scalar import Scalar, ScalarInvalidTypeError


class JSON(Scalar):
    def __eq__(self, scalar: Scalar) -> bool:
        if scalar is None:
            return False
        if isinstance(scalar, JSON):
            return self._value == scalar._value and self._valid == scalar._valid
        return False

    @property
    def value(self):
        return self._value

    def set(self, value: any):
        if value is None:
            return

        if isinstance(value, (str, bytes)):
            # test if it is a valid json
            json.loads(value)
            self._value = value
        else:
            self._value = json.dumps(value)
        self._valid = True
