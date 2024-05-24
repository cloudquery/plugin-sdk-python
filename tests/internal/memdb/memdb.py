from cloudquery.sdk.internal import memdb
from cloudquery.sdk.internal.servers.plugin_v3 import plugin
from cloudquery.sdk.plugin import SyncOptions


def test_memdb():
    p = memdb.MemDB()
    p.init(plugin.sanitize_spec(b"null"))
    msgs = []
    for msg in p.sync(SyncOptions(tables=["*"])):
        msgs.append(msg)
    assert len(msgs) == 0
