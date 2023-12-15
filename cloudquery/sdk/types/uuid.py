import pyarrow as pa


class UUIDType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(
            self, extension_name="uuid", storage_type=pa.binary(16)
        )

    def __reduce__(self):
        return UUIDType, ()

    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return b"uuid-serialized"

    def __str__(self):
        return "uuid"

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        return UUIDType()
