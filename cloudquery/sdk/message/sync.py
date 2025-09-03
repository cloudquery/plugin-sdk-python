import pyarrow as pa


class SyncMessage:
    pass


class SyncInsertMessage(SyncMessage):
    def __init__(self, record: pa.RecordBatch):
        self.record = record


class SyncMigrateTableMessage(SyncMessage):
    def __init__(self, table: pa.Schema):
        self.table = table


class SyncErrorMessage(SyncMessage):
    def __init__(self, table_name: str, error: str):
        self.table_name = table_name
        self.error = error
