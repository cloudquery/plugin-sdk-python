import pyarrow as pa


class SyncMessage:
    pass


class SyncInsertMessage:
    def __init__(self, record: pa.RecordBatch):
        self.record = record


class SyncMigrateTableMessage:
    def __init__(self, schema: pa.Schema):
        self.schema = schema
