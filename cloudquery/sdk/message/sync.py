
import pyarrow as pa

class SyncMessage:
    pass

class SyncInsertMessage:
    def __init__(self, record: pa.RecordBatch):
        self._record = record

class SyncMigrateMessage:
    def __init__(self, record: pa.RecordBatch):
      self._record = record