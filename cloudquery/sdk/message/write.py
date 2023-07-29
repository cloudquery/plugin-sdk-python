import pyarrow as pa
from cloudquery.sdk.schema import Table


class WriteMessage:
    pass


class WriteInsertMessage(WriteMessage):
    def __init__(self, record: pa.RecordBatch):
        self.record = record


class WriteMigrateTableMessage(WriteMessage):
    def __init__(self, table: Table):
        self.table = table


class WriteDeleteStale(WriteMessage):
    def __init__(self, table_name: str, source_name: str, sync_time):
        self.table_name = table_name
        self.source_name = source_name
        self.sync_time = sync_time
