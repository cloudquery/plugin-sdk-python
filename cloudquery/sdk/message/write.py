
import pyarrow as pa

class WriteMessage:
    pass

class InsertMessage(WriteMessage):
    def __init__(self, record: pa.RecordBatch):
        self._record = record

class MigrateMessage(WriteMessage):
    def __init__(self, record: pa.RecordBatch):
      self._record = record