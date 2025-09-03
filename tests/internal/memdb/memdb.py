from cloudquery.sdk.internal import memdb
from cloudquery.sdk.internal.servers.plugin_v3 import plugin
from cloudquery.sdk.plugin import SyncOptions
from cloudquery.sdk.message import SyncMigrateTableMessage, SyncInsertMessage
import structlog


def test_memdb():
    p = memdb.MemDB()
    p.set_logger(structlog.get_logger())
    p.init(plugin.sanitize_spec(b"null"))
    msgs = []
    for msg in p.sync(SyncOptions(tables=["*"],skip_tables=[])):
        msgs.append(msg)
    assert len(msgs) == 18

    assert isinstance(msgs[0], SyncMigrateTableMessage)
    assert isinstance(msgs[1], SyncMigrateTableMessage)
    assert isinstance(msgs[2], SyncMigrateTableMessage)

    # other messages should be inserts
    for msg in msgs[3:]:
        assert isinstance(msg, SyncInsertMessage)


