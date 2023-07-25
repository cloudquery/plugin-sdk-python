import pyarrow as pa
from .int64 import Int64

class ScalarInvalidTypeError(Exception):
  pass

class Scalar:
    @property
    def is_valid(self) -> bool:
      return self._valid


class ScalarFactory:
  def __init__(self):
    self._type_map = {
        pa.int64: lambda dt: Int64(),
    }
  
  def new_scalar(self, dt):
    if dt in self._type_map:
      return self._type_map[dt]()
    else:
      raise ScalarInvalidTypeError("Invalid type for scalar")
