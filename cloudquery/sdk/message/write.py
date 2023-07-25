import pyarrow as pa


class WriteMessage:
    pass


class InsertMessage(WriteMessage):
    def __init__(self, record: pa.RecordBatch):
        self.record = record


class MigrateMessage(WriteMessage):
    def __init__(self, table: pa.Schema):
        self.table = table
