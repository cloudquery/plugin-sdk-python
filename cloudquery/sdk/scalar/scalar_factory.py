import pyarrow as pa
from .scalar import ScalarInvalidTypeError
from .int64 import Int64


class ScalarFactory:
    def __init__(self):
        pass

    def new_scalar(self, dt):
        dt_id = dt.id
        if dt_id == pa.types.lib.Type_INT64:
            return Int64()
        else:
            raise ScalarInvalidTypeError("Invalid type {} for scalar".format(dt))
