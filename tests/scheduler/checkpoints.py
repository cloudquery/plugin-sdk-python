import contextvars
import queue
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
from cloudquery.sdk.stateclient import SyncNotRunningError
from cloudquery.sdk.stateclient.stateclient import (
    ConnectedStateClient,
    StateClient,
    StateClientBuilder,
)

STATE_TABLE = "cq_state_test"


class MemoryStateClient(StateClient):
    """A state client whose backend is a dict."""

    def __init__(self, checkpoints=None, fail_writes=False, backend=None):
        self.backend: Dict[str, str] = dict(backend or {})
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


def test_setting_an_unchanged_value_is_not_a_change():
    state = MemoryStateClient(checkpoints=None)
    state.backend["a"] = "1"
    state.read_all_state()
    state.set_key("a", "1")
    state.flush()
    assert state.writes == []

    state.set_key("a", "2")
    state.set_key("a", "2")
    state.flush()
    assert state.writes == [[("a", "2")]]


def test_setting_an_unchanged_value_adds_the_caller_to_the_pending_change():
    scheduler = Scheduler(1)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)
    state.set_key("a", "1")
    scope = scheduler.checkpoints.new_scope(None)
    with scheduler.checkpoints.entered(scope):
        state.set_key("a", "1")
    scheduler.shutdown()
    assert state.changes["a"] == {None, scope}


def test_unchanged_values_order_later_checkpoints_after_the_callers_resources():
    scheduler = Scheduler(1)
    checkpoints = scheduler.checkpoints
    res = queue.Queue()
    checkpoints.start(res)
    state = MemoryStateClient(checkpoints=checkpoints)
    first, unfinished, last = (checkpoints.new_scope(None) for _ in range(3))
    child = checkpoints.new_scope(unfinished)

    def flush_in(scope, value):
        with checkpoints.entered(scope):
            state.set_key("cursor", value)
            state.flush()

    def sent():
        return [checkpoint_rows(res.get_nowait())["cursor"] for _ in range(res.qsize())]

    try:
        flush_in(first, "1")
        assert sent() == ["1"]
        # the same value, but it also covers the resources of a resolver whose relations still run
        flush_in(unfinished, "1")
        flush_in(last, "2")
        assert sent() == [], "a later value must not overtake the relations it follows"
        checkpoints.resolver_finished(child)
        assert sent() == ["1", "2"]
    finally:
        checkpoints.stop()
        scheduler.shutdown()


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
    with pytest.raises(SyncNotRunningError):
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


def test_resolvers_of_an_ended_sync_cannot_change_the_state_of_the_next_one():
    plugin_attributes: Dict[str, StateClient] = {}
    next_started, leftover_done = threading.Event(), threading.Event()
    leftover_errors: List[Exception] = []

    class LeftoverResolver(TableResolver):
        def __init__(self):
            super().__init__(Table("leftover", [Column("id", pa.int64())]))

        def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
            yield {"id": 1}
            try:
                assert next_started.wait(5), "the next sync did not start"
                plugin_attributes["state"].set_key("leftover_cursor", "ended")
            except Exception as e:
                leftover_errors.append(e)
            finally:
                leftover_done.set()

    class NextResolver(CursorResolver):
        def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
            next_started.set()
            assert leftover_done.wait(5), "the leftover resolver did not finish"
            yield from super().resolve(client, parent_resource)

    scheduler = Scheduler(4)
    try:
        ended = scheduler.sync(TestClient(), [LeftoverResolver()])
        while not isinstance(next(ended), SyncInsertMessage):
            pass
        ended.close()

        plugin_attributes["state"] = MemoryStateClient(
            checkpoints=scheduler.checkpoints
        )
        scheduler.set_post_sync_hook(plugin_attributes["state"].flush)
        messages = list(
            scheduler.sync(
                TestClient(), [NextResolver(plugin_attributes["state"], "next")]
            )
        )
    finally:
        scheduler.shutdown()
    assert leftover_errors == []
    assert plugin_attributes["state"].get_key("leftover_cursor") is None
    checkpoints = [
        checkpoint_rows(m) for m in messages if isinstance(m, SyncCheckpointMessage)
    ]
    assert [c["cursor"] for c in checkpoints] == [f"next-{i}" for i in range(1, 30)]


class SetKeyThenWait(TableResolver):
    def __init__(self, state: StateClient, release: threading.Event):
        super().__init__(Table("set_then_wait", [Column("id", pa.int64())]))
        self._state = state
        self._release = release

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        self._state.set_key("cursor", "ended")
        yield {"id": 1}
        self._release.wait(5)


def test_a_state_client_reused_across_syncs_drops_unflushed_changes_of_an_ended_sync():
    release = threading.Event()
    scheduler = Scheduler(4)
    try:
        state = MemoryStateClient(
            checkpoints=scheduler.checkpoints, backend={"cursor": "flushed"}
        )
        ended = scheduler.sync(TestClient(), [SetKeyThenWait(state, release)])
        while not isinstance(next(ended), SyncInsertMessage):
            pass
        ended.close()
        release.set()

        scheduler.set_post_sync_hook(state.flush)
        cursors_seen: List[Optional[str]] = []

        class ReadCursor(TableResolver):
            def __init__(self):
                super().__init__(Table("read_cursor", [Column("id", pa.int64())]))

            def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
                cursors_seen.append(state.get_key("cursor"))
                yield {"id": 1}
                state.set_key("other", "next")

        messages = list(scheduler.sync(TestClient(), [ReadCursor()]))
        list(scheduler.sync(TestClient(), [ReadCursor()]))
    finally:
        release.set()
        scheduler.shutdown()
    assert cursors_seen == ["flushed", "flushed"]
    checkpoints = [
        checkpoint_rows(m) for m in messages if isinstance(m, SyncCheckpointMessage)
    ]
    assert checkpoints == [{"other": "next"}]


class HookCursorResolver(TableResolver):
    """Checkpoints its cursor from a resource hook, before the resource is on the queue."""

    def __init__(self, state: StateClient, hook: str, parents: int):
        super().__init__(
            Table("parent", [Column("id", pa.int64())]),
            child_resolvers=[ChildResolver(2)],
        )
        self._state = state
        self._hook = hook
        self._parents = parents

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for parent_id in range(self._parents):
            yield {"id": parent_id}

    def pre_resource_resolve(self, client, resource):
        if self._hook == "pre":
            self._checkpoint(resource)

    def post_resource_resolve(self, client, resource):
        if self._hook == "post":
            self._checkpoint(resource)

    def _checkpoint(self, resource):
        self._state.set_key("hook-cursor", str(resource.item["id"]))
        self._state.flush()


@pytest.mark.parametrize("hook", ["pre", "post"])
def test_checkpoints_flushed_by_resource_hooks_follow_the_resource_and_its_relations(
    hook,
):
    parents = 10
    scheduler = Scheduler(4)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)
    try:
        messages = list(
            scheduler.sync(TestClient(), [HookCursorResolver(state, hook, parents)])
        )
    finally:
        scheduler.shutdown()

    seen = set()
    cursors = []
    for msg in messages:
        if isinstance(msg, SyncInsertMessage):
            table = msg.record.schema.metadata[METADATA_TABLE_NAME]
            row = msg.record.to_pylist()[0]
            if table == b"parent":
                seen.add((row["id"],))
            else:
                seen.add((row["parent_id"], row["child_id"]))
        elif isinstance(msg, SyncCheckpointMessage):
            cursor = int(checkpoint_rows(msg)["hook-cursor"])
            cursors.append(cursor)
            for parent_id in range(cursor + 1):
                assert (parent_id,) in seen, f"cursor {cursor} sent before its parent"
                for child_id in range(2):
                    assert (parent_id, child_id) in seen, (
                        f"cursor {cursor} sent before child {child_id} of parent "
                        f"{parent_id}"
                    )
    assert cursors and cursors[-1] == parents - 1


class GrandchildResolver(TableResolver):
    """Yields slowly, so it still runs after the child resolver that spawned it returned."""

    def __init__(self):
        super().__init__(
            Table(
                "grandchild",
                [
                    Column("parent_id", pa.int64()),
                    Column("child_id", pa.int64()),
                    Column("grandchild_id", pa.int64()),
                ],
            )
        )

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for grandchild_id in range(3):
            time.sleep(random.uniform(0.005, 0.02))
            yield {**parent_resource.item, "grandchild_id": grandchild_id}


class FastChildResolver(TableResolver):
    def __init__(self):
        super().__init__(
            Table(
                "fast_child",
                [Column("parent_id", pa.int64()), Column("child_id", pa.int64())],
            ),
            child_resolvers=[GrandchildResolver()],
        )

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for child_id in range(2):
            yield {"parent_id": parent_resource.item["id"], "child_id": child_id}


class GrandparentResolver(TableResolver):
    def __init__(self, state: StateClient, parents: int, flush_each: bool):
        super().__init__(
            Table("grandparent", [Column("id", pa.int64())]),
            child_resolvers=[FastChildResolver()],
        )
        self._state = state
        self._parents = parents
        self._flush_each = flush_each

    def resolve(self, client, parent_resource) -> Generator[Any, None, None]:
        for parent_id in range(self._parents):
            yield {"id": parent_id}
            if self._flush_each:
                self._state.set_key("grandparent-cursor", str(parent_id))
                self._state.flush()
        if not self._flush_each:
            # the child resolvers return meanwhile, while their grandchildren still run
            time.sleep(0.01)
            self._state.set_key("grandparent-cursor", str(self._parents - 1))
            self._state.flush()


@pytest.mark.parametrize("flush_each", [True, False])
def test_parent_checkpoints_follow_the_relations_of_their_relations(flush_each):
    parents = 10
    scheduler = Scheduler(8)
    state = MemoryStateClient(checkpoints=scheduler.checkpoints)
    try:
        messages = list(
            scheduler.sync(
                TestClient(), [GrandparentResolver(state, parents, flush_each)]
            )
        )
    finally:
        scheduler.shutdown()

    seen = set()
    cursors = []
    for msg in messages:
        if (
            isinstance(msg, SyncInsertMessage)
            and msg.record.schema.metadata[METADATA_TABLE_NAME] == b"grandchild"
        ):
            row = msg.record.to_pylist()[0]
            seen.add((row["parent_id"], row["child_id"], row["grandchild_id"]))
        elif isinstance(msg, SyncCheckpointMessage):
            cursor = int(checkpoint_rows(msg)["grandparent-cursor"])
            cursors.append(cursor)
            for parent_id in range(cursor + 1):
                for child_id in range(2):
                    for grandchild_id in range(3):
                        assert (parent_id, child_id, grandchild_id) in seen, (
                            f"cursor {cursor} sent before grandchild "
                            f"{(parent_id, child_id, grandchild_id)}"
                        )
    assert cursors and cursors[-1] == parents - 1
