from functools import partial

import pyarrow as pa

from cloudquery.sdk.types import UUIDType, JSONType
from .binary import Binary
from .bool import Bool
from .date32 import Date32
from .date64 import Date64
from .float import Float
from .int import Int
from .json import JSON
from .list import List
from .scalar import ScalarInvalidTypeError
from .string import String
from .timestamp import Timestamp
from .uint import Uint
from .uuid import UUID


class ScalarFactory:
    def __init__(self):
        pass

    def new_scalar(self, dt: pa.DataType):
        dt_id = dt.id
        type_id__map = {
            pa.types.lib.Type_INT64: partial(Int, bitwidth=64),
            pa.types.lib.Type_INT32: partial(Int, bitwidth=32),
            pa.types.lib.Type_INT16: partial(Int, bitwidth=16),
            pa.types.lib.Type_INT8: partial(Int, bitwidth=8),
            pa.types.lib.Type_UINT64: partial(Uint, bitwidth=64),
            pa.types.lib.Type_UINT32: partial(Uint, bitwidth=32),
            pa.types.lib.Type_UINT16: partial(Uint, bitwidth=16),
            pa.types.lib.Type_UINT8: partial(Uint, bitwidth=8),
            pa.types.lib.Type_BINARY: Binary,
            pa.types.lib.Type_LARGE_BINARY: Binary,
            pa.types.lib.Type_FIXED_SIZE_BINARY: Binary,
            pa.types.lib.Type_BOOL: Bool,
            pa.types.lib.Type_DATE64: Date64,
            pa.types.lib.Type_DATE32: Date32,
            pa.types.lib.Type_DOUBLE: partial(Float, bitwidth=64),
            pa.types.lib.Type_FLOAT: partial(Float, bitwidth=32),
            pa.types.lib.Type_HALF_FLOAT: partial(Float, bitwidth=16),
            pa.types.lib.Type_LIST: List,
            pa.types.lib.Type_LARGE_LIST: List,
            pa.types.lib.Type_FIXED_SIZE_LIST: List,
            pa.types.lib.Type_STRING: String,
            pa.types.lib.Type_LARGE_STRING: String,
            pa.types.lib.Type_TIMESTAMP: Timestamp,
        }
        # Built-in Types
        if dt_id in type_id__map:
            scalar_type = type_id__map[dt_id]
            if scalar_type == List:
                item = self.new_scalar(dt.field(0).type)
                return scalar_type(type(item))
            return scalar_type()
        # Extension Types - Can't do the same trick as above as they don't have `id`s and they are not hashable. :(
        if dt == UUIDType():
            return UUID()
        if dt == JSONType():
            return JSON()
        raise ScalarInvalidTypeError(f"Invalid type {dt} for scalar")
