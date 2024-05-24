from cloudquery.sdk.internal import memdb
from cloudquery.sdk.plugin import SyncOptions


def get_spec(spec=None):
    return b"{}" if spec is None or spec == b"" else spec


def test_memdb():
    p = memdb.MemDB()
    p.init(get_spec())
    msgs = []
    for msg in p.sync(SyncOptions(tables=["*"])):
        msgs.append(msg)
    assert len(msgs) == 0
