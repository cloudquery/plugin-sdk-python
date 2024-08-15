import pyarrow as pa


class ReadMessage:
    def __init__(self, record: pa.RecordBatch):
        self.record = record
