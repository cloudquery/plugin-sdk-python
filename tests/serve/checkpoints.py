import pyarrow as pa
import pytest
from cloudquery.plugin_v3 import arrow, plugin_pb2

from cloudquery.sdk.internal.servers.plugin_v3.plugin import PluginServicer
from cloudquery.sdk.message import SyncCheckpointMessage, SyncInsertMessage
from cloudquery.sdk.plugin.plugin import Plugin, SyncOptions
from cloudquery.sdk.stateclient.stateclient import Table as StateTable

state_table = StateTable("cq_state_test")


class CheckpointingPlugin(Plugin):
    def __init__(self):
        super().__init__("checkpointing", "development")
        self.options = None

    def sync(self, options: SyncOptions):
        self.options = options
        yield SyncInsertMessage(pa.record_batch([[1]], names=["id"]))
        yield SyncCheckpointMessage(state_table.record([("cursor", "1")]))


def sync_request(supports_checkpoints: bool):
    return plugin_pb2.Sync.Request(
        tables=["*"],
        backend=plugin_pb2.Sync.BackendOptions(
            table_name="cq_state_test", connection="localhost:7777"
        ),
        supports_checkpoints=supports_checkpoints,
    )


def test_sync_sends_checkpoints_and_passes_the_cli_support():
    plugin = CheckpointingPlugin()
    responses = list(PluginServicer(plugin).Sync(sync_request(True), None))

    assert plugin.options.backend_options.supports_checkpoints
    assert plugin.options.backend_options.table_name == "cq_state_test"
    assert plugin.options.backend_options.connection == "localhost:7777"
    assert [r.WhichOneof("message") for r in responses] == ["insert", "checkpoint"]
    record = arrow.new_record_from_bytes(responses[1].checkpoint.record)
    assert record.to_pylist() == [{"key": "cursor", "value": "1"}]


def test_sync_refuses_checkpoints_the_cli_would_drop():
    plugin = CheckpointingPlugin()
    with pytest.raises(RuntimeError):
        list(PluginServicer(plugin).Sync(sync_request(False), None))
    assert not plugin.options.backend_options.supports_checkpoints
