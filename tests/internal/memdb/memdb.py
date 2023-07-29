from cloudquery.sdk.internal import memdb
from cloudquery.sdk.plugin import SyncOptions


def test_memdb():
    p = memdb.MemDB()
    p.init(None)
    msgs = []
    for msg in p.sync(SyncOptions(tables=["*"])):
        msgs.append(msg)
    assert len(msgs) == 0
