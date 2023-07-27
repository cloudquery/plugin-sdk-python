import pyarrow as pa
import pyarrow
import sys


class UuidType(pa.PyExtensionType):
    def __init__(self):
        pa.PyExtensionType.__init__(self, pa.binary(16))

    def __reduce__(self):
        return UuidType, ()

    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return b"uuid-serialized"

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        return UuidType()
