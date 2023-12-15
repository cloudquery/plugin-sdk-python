import pyarrow as pa


class JSONType(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, extension_name="json", storage_type=pa.binary())

    def __reduce__(self):
        return JSONType, ()

    def __arrow_ext_serialize__(self):
        # since we don't have a parameterized type, we don't need extra
        # metadata to be deserialized
        return b"json-serialized"

    def __str__(self):
        return "json"

    @classmethod
    def __arrow_ext_deserialize__(self, storage_type, serialized):
        # return an instance of this subclass given the serialized
        # metadata.
        return JSONType()
