import contextvars
import random
import threading
import time
from concurrent import futures
from typing import Any, Dict, Generator, List, Optional, Tuple

import pyarrow as pa
import pytest

from cloudquery.sdk.message import (
    SyncCheckpointMessage,
    SyncInsertMessage,
    SyncMessage,
)
from cloudquery.sdk.plugin.plugin import BackendOptions
from cloudquery.sdk.scheduler import Scheduler, TableResolver
from cloudquery.sdk.schema import Column
from cloudquery.sdk.schema.arrow import METADATA_TABLE_NAME
from cloudquery.sdk.schema.table import Table
from cloudquery.sdk.stateclient.stateclient import (
    ConnectedStateClient,
    StateClient,
    StateClientBuilder,
)

STATE_TABLE = "cq_state_test"


class MemoryStateClient(StateClient):
    """A state client whose backend is a dict."""

    def __init__(self, checkpoints=None, fail_writes=False):
        self.backend: Dict[str, str] = {}
        self.writes: List[List[Tuple[str, str]]] = []
        self.fail_writes = fail_writes
        super().__init__(
            backend_options=BackendOptions(
                connection="unused", table_name=STATE_TABLE, supports_checkpoints=True
            ),
            checkpoints=checkpoints,
        )

    def migrate_state_table(self):
        pass

    def read_all_state(self):
        self.mem.update(self.backend)

    def write_all_state(self, changed_keys):
        rows = list(changed_keys)
        if self.fail_writes:
            raise ConnectionError("backend unavailable")
        self.writes.append(rows)
        self.backend.update(rows)


class TestClient:
    def id(self):
        return "test_client"


def checkpoint_rows(msg: SyncCheckpointMessage) -> Dict[str, str]:
    rows = msg.record.to_pylist()
    return {row["key"]: row["value"] for row in rows}


def sleep_a_little():
    time.sleep(random.uniform(0, 0.005))


class ChildResolver(TableResolver):
    """Yields children slowly, so they finish after the parent moved on."""

    def __init__(self, children: int, state: Optional[StateClient] = None):
        super().__init__(
            Table(
                "child",
                [Column("parent_id", pa.int64()), Column("child_id", pa.int64())],
            )
        )
        self._children = children
        self._state = state

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        parent_id = parent_resource.item["id"]
        for child_id in range(self._children):
            sleep_a_little()
            yield {"parent_id": parent_id, "child_id": child_id}
        if self._state is not None:
            self._state.set_key(f"child-{parent_id}", str(self._children - 1))
            self._state.flush()


class ParentResolver(TableResolver):
    """Yields parents and checkpoints its cursor after each one, as an incremental table would."""

    def __init__(self, state: StateClient, parents: int, children: int, child_state):
        super().__init__(
            Table("parent", [Column("id", pa.int64())]),
            child_resolvers=[ChildResolver(children, child_state)],
        )
        self._state = state
        self._parents = parents

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for parent_id in range(self._parents):
            yield {"id": parent_id}
            self._state.set_key("parent-cursor", str(parent_id))
            self._state.flush()


def run_sync(
    parents=20, children=5, child_checkpoints=False, concurrency=8
) -> Tuple[List[SyncMessage], MemoryStateClient]:
    scheduler = Scheduler(concurrency)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)
    scheduler.set_post_sync_hook(state.flush)
    resolver = ParentResolver(
        state, parents, children, state if child_checkpoints else None
    )
    try:
        messages = list(scheduler.sync(TestClient(), [resolver]))
    finally:
        scheduler.shutdown()
    return messages, state


def test_parent_checkpoints_follow_the_relations_of_the_resources_they_cover():
    parents, children = 20, 5
    messages, state = run_sync(parents, children)

    assert state.writes == [], "state must not be written to the backend directly"
    seen_children = set()
    cursors = []
    for msg in messages:
        if isinstance(msg, SyncInsertMessage) and msg.record.schema.metadata[
            METADATA_TABLE_NAME
        ] == (b"child"):
            row = msg.record.to_pylist()[0]
            seen_children.add((row["parent_id"], row["child_id"]))
        elif isinstance(msg, SyncCheckpointMessage):
            cursor = int(checkpoint_rows(msg)["parent-cursor"])
            cursors.append(cursor)
            for parent_id in range(cursor + 1):
                for child_id in range(children):
                    assert (parent_id, child_id) in seen_children, (
                        f"checkpoint of cursor {cursor} sent before child "
                        f"{child_id} of parent {parent_id}"
                    )
    assert cursors, "no checkpoints were sent"
    assert cursors == sorted(cursors), "a key's checkpoints must keep their order"
    assert cursors[-1] == parents - 1


def test_child_checkpoints_follow_their_own_resources():
    parents, children = 10, 4
    messages, _ = run_sync(parents, children, child_checkpoints=True)

    seen_children = set()
    child_keys = set()
    for msg in messages:
        if isinstance(msg, SyncInsertMessage) and msg.record.schema.metadata[
            METADATA_TABLE_NAME
        ] == (b"child"):
            row = msg.record.to_pylist()[0]
            seen_children.add((row["parent_id"], row["child_id"]))
        elif isinstance(msg, SyncCheckpointMessage):
            for key in checkpoint_rows(msg):
                if key.startswith("child-"):
                    parent_id = int(key.split("-")[1])
                    child_keys.add(key)
                    for child_id in range(children):
                        assert (parent_id, child_id) in seen_children
    assert len(child_keys) == parents


def test_checkpoint_records_are_state_table_rows():
    messages, _ = run_sync(parents=2, children=1)
    checkpoint = next(m for m in messages if isinstance(m, SyncCheckpointMessage))
    schema = checkpoint.record.schema
    assert schema.metadata[METADATA_TABLE_NAME] == STATE_TABLE.encode()
    assert schema.field("key").type == pa.string()
    assert schema.field("value").type == pa.string()


def test_post_sync_hook_flush_is_sent_after_every_resource():
    scheduler = Scheduler(2)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)

    class Resolver(TableResolver):
        def __init__(self):
            super().__init__(Table("items", [Column("id", pa.int64())]))

        def resolve(self, client, parent_resource):
            yield {"id": 1}
            yield {"id": 2}

    def hook():
        # outside table resolvers, so it waits for nothing: the hook runs after every resource
        state.set_key("sync-finished", "yes")
        state.flush()

    scheduler.set_post_sync_hook(hook)
    messages = list(scheduler.sync(TestClient(), [Resolver()]))
    scheduler.shutdown()

    assert isinstance(messages[-1], SyncCheckpointMessage)
    assert checkpoint_rows(messages[-1]) == {"sync-finished": "yes"}
    assert sum(isinstance(m, SyncInsertMessage) for m in messages) == 2


def test_flush_writes_to_the_backend_without_checkpoints():
    state = MemoryStateClient(checkpoints=None)
    state.set_key("a", "1")
    state.set_key("b", "2")
    state.flush()
    state.flush()  # nothing changed since

    assert state.writes == [[("a", "1"), ("b", "2")]]
    state.set_key("a", "3")
    state.flush()
    assert state.writes[-1] == [("a", "3")]


def test_failed_flush_keeps_the_changes():
    state = MemoryStateClient(checkpoints=None, fail_writes=True)
    state.set_key("a", "1")
    with pytest.raises(ConnectionError):
        state.flush()

    state.fail_writes = False
    state.flush()
    assert state.backend == {"a": "1"}


def test_checkpoint_flush_outside_a_sync_fails_and_keeps_the_changes():
    scheduler = Scheduler(1)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)
    state.set_key("a", "1")
    with pytest.raises(RuntimeError):
        state.flush()
    assert "a" in state.changes
    assert state.writes == []
    scheduler.shutdown()


def test_concurrent_flushes_send_the_latest_value_last():
    scheduler = Scheduler(16)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)
    counter = iter(range(10**6))
    lock = threading.Lock()

    class Resolver(TableResolver):
        def __init__(self, name):
            super().__init__(Table(name, [Column("id", pa.int64())]))

        def resolve(self, client, parent_resource):
            for i in range(50):
                yield {"id": i}
                with lock:
                    value = next(counter)
                    state.set_key("shared", f"{value:08d}")
                state.flush()

    messages = list(
        scheduler.sync(TestClient(), [Resolver(f"items_{i}") for i in range(8)])
    )
    scheduler.shutdown()

    values = [
        checkpoint_rows(m)["shared"]
        for m in messages
        if isinstance(m, SyncCheckpointMessage)
    ]
    assert values == sorted(values)
    assert values[-1] == f"{8 * 50 - 1:08d}"


def test_builder_uses_checkpoints_only_when_the_cli_supports_them(monkeypatch):
    for method in ("migrate_state_table", "read_all_state", "write_all_state"):
        monkeypatch.setattr(ConnectedStateClient, method, lambda self, *a: None)
    scheduler = Scheduler(1)

    supported = StateClientBuilder.build(
        backend_options=BackendOptions(
            connection="localhost:1", table_name="t", supports_checkpoints=True
        ),
        checkpoints=scheduler.checkpoints,
    )
    unsupported = StateClientBuilder.build(
        backend_options=BackendOptions(connection="localhost:1", table_name="t"),
        checkpoints=scheduler.checkpoints,
    )
    not_requested = StateClientBuilder.build(
        backend_options=BackendOptions(
            connection="localhost:1", table_name="t", supports_checkpoints=True
        ),
    )
    scheduler.shutdown()

    assert supported.checkpoints is scheduler.checkpoints
    assert unsupported.checkpoints is None
    assert not_requested.checkpoints is None


class SingleItemResolver(TableResolver):
    def __init__(self, name: str, multiplex_delay: float = 0):
        super().__init__(Table(name, [Column("id", pa.int64())]))
        self._multiplex_delay = multiplex_delay

    def multiplex(self, client):
        time.sleep(self._multiplex_delay)
        return [client]

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        yield {"id": 1}


def test_sync_waits_for_resolvers_submitted_after_earlier_ones_finished():
    scheduler = Scheduler(1)
    resolvers = [
        SingleItemResolver("fast"),
        SingleItemResolver("slow_to_multiplex", multiplex_delay=0.2),
    ]
    try:
        messages = list(scheduler.sync(TestClient(), resolvers))
    finally:
        scheduler.shutdown()
    inserts = [m for m in messages if isinstance(m, SyncInsertMessage)]
    assert len(inserts) == len(resolvers)


def test_scope_follows_copied_context_into_other_threads():
    scheduler = Scheduler(1)
    scopes = []

    class Resolver(TableResolver):
        def __init__(self):
            super().__init__(Table("items", [Column("id", pa.int64())]))

        def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
            with futures.ThreadPoolExecutor(1) as pool:
                ctx = contextvars.copy_context()
                scopes.append(
                    pool.submit(ctx.run, scheduler.checkpoints.current_scope).result()
                )
            yield {"id": 1}

    try:
        list(scheduler.sync(TestClient(), [Resolver()]))
    finally:
        scheduler.shutdown()
    assert scopes[0] is not None


class CursorResolver(TableResolver):
    def __init__(self, state: StateClient, tag: str):
        super().__init__(Table("items", [Column("id", pa.int64())]))
        self._state = state
        self._tag = tag

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for item_id in range(1, 30):
            time.sleep(0.02)
            yield {"id": item_id}
            self._state.set_key("cursor", f"{self._tag}-{item_id}")
            self._state.flush()


def test_resolvers_of_an_ended_sync_do_not_checkpoint_into_the_next_one():
    scheduler = Scheduler(4)
    try:
        ended = scheduler.sync(
            TestClient(),
            [
                CursorResolver(
                    MemoryStateClient(checkpoints=scheduler.checkpoints), "ended"
                )
            ],
        )
        for _ in range(5):
            next(ended)
        ended.close()

        state = MemoryStateClient(checkpoints=scheduler.checkpoints)
        scheduler.set_post_sync_hook(state.flush)
        messages = list(scheduler.sync(TestClient(), [CursorResolver(state, "next")]))
    finally:
        scheduler.shutdown()
    cursors = [
        checkpoint_rows(m)["cursor"]
        for m in messages
        if isinstance(m, SyncCheckpointMessage)
    ]
    assert cursors
    assert all(cursor.startswith("next-") for cursor in cursors)
