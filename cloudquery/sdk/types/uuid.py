import pyarrow as pa

class UuidType(pa.PyExtensionType):

    def __init__(self):
        pa.PyExtensionType.__init__(self, pa.binary(16))

    def __reduce__(self):
        return UuidType, ()