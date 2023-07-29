import pyarrow as pa
from .scalar import ScalarInvalidTypeError
from .binary import Binary
from .bool import Bool
from .date32 import Date32
from .date64 import Date64
from .float import Float
from .int import Int
from .list import List
from .string import String
from .timestamp import Timestamp
from .uint import Uint
from .uuid import UUID
from cloudquery.sdk.types import UUIDType, JSONType


class ScalarFactory:
    def __init__(self):
        pass

    def new_scalar(self, dt: pa.DataType):
        dt_id = dt.id
        if dt_id == pa.types.lib.Type_INT64:
            return Int(bitwidth=64)
        elif dt_id == pa.types.lib.Type_INT32:
            return Int(bitwidth=32)
        elif dt_id == pa.types.lib.Type_INT16:
            return Int(bitwidth=16)
        elif dt_id == pa.types.lib.Type_INT8:
            return Int(bitwidth=8)
        elif dt_id == pa.types.lib.Type_UINT64:
            return Uint(bitwidth=64)
        elif dt_id == pa.types.lib.Type_UINT32:
            return Uint(bitwidth=32)
        elif dt_id == pa.types.lib.Type_UINT16:
            return Uint(bitwidth=16)
        elif dt_id == pa.types.lib.Type_UINT8:
            return Uint(bitwidth=8)
        elif (
            dt_id == pa.types.lib.Type_BINARY
            or dt_id == pa.types.lib.Type_LARGE_BINARY
            or dt_id == pa.types.lib.Type_FIXED_SIZE_BINARY
        ):
            return Binary()
        elif dt_id == pa.types.lib.Type_BOOL:
            return Bool()
        elif dt_id == pa.types.lib.Type_DATE64:
            return Date64()
        elif dt_id == pa.types.lib.Type_DATE32:
            return Date32()
        # elif dt_id == pa.types.lib.Type_DECIMAL256:
        #     return ()
        # elif dt_id == pa.types.lib.Type_DECIMAL128:
        #     return ()
        # elif dt_id == pa.types.lib.Type_DICTIONARY:
        #     return ()
        # elif dt_id == pa.types.lib.Type_DURATION:
        #     return ()
        elif dt_id == pa.types.lib.Type_DOUBLE:
            return Float(bitwidth=64)
        elif dt_id == pa.types.lib.Type_FLOAT:
            return Float(bitwidth=32)
        elif dt_id == pa.types.lib.Type_HALF_FLOAT:
            return Float(bitwidth=16)
        # elif dt_id == pa.types.lib.Type_INTERVAL_MONTH_DAY_NANO:
        #     return ()
        elif (
            dt_id == pa.types.lib.Type_LIST
            or dt_id == pa.types.lib.Type_LARGE_LIST
            or dt_id == pa.types.lib.Type_FIXED_SIZE_LIST
        ):
            item = ScalarFactory.new_scalar(dt.field(0).type)
            return List(type(item))
        # elif dt_id == pa.types.lib.Type_MAP:
        #     return ()
        elif (
            dt_id == pa.types.lib.Type_STRING or dt_id == pa.types.lib.Type_LARGE_STRING
        ):
            return String()
        # elif dt_id == pa.types.lib.Type_STRUCT:
        #     return ()
        # elif dt_id == pa.types.lib.Type_TIME32:
        #     return ()
        # elif dt_id == pa.types.lib.Type_TIME64:
        #     return ()
        elif dt_id == pa.types.lib.Type_TIMESTAMP:
            return Timestamp()
        elif dt == UUIDType:
            return UUID()
        elif dt == JSONType:
            return String()
        else:
            raise ScalarInvalidTypeError("Invalid type {} for scalar".format(dt))
