from cloudquery.sdk.schema import Table
import pyarrow as pa


class ReadMessage:
    pass


class ReadInsertMessage(ReadMessage):
    def __init__(self, record: pa.RecordBatch):
        self.record = record


class ReadRequest(ReadMessage):
    def __init__(self, table: Table):
        self.table = table
