"""
Orders state checkpoints after the resources they summarize.

When the CLI supports checkpoints, it persists the state rows a source sends as
``Sync.MessageCheckpoint`` once every destination has committed the messages sent
before them. A checkpoint must therefore go on the sync stream only after every
resource its state covers: the resources produced, before the flush, by the table
resolvers that changed its keys, and the relations those resources spawned.

The scheduler sends every message through a single queue, so a checkpoint put on
the queue once those resources are on it is ordered after them on the stream.
"""

from __future__ import annotations

import contextvars
import queue
import threading
from contextlib import contextmanager
from typing import Dict, Iterable, Iterator, List, Optional, Set, Tuple

from cloudquery.sdk.message import SyncCheckpointMessage


class Scope:
    """
    A table resolver call and the relations its resources spawn.

    A scope is finished once its resolver returned and every child scope it
    spawned is finished.
    """

    def __init__(self, parent: Optional[Scope], sync: Optional[object]):
        self.parent = parent
        # the sync the scope belongs to, so that resolvers still running after it ended cannot checkpoint into the next one
        self.sync = sync
        # child scopes that are not finished yet
        self.open_children: Set[Scope] = set()
        self.resolving = True
        # checkpoints waiting for this scope to finish
        self.waiters: List[_Pending] = []

    def finished(self) -> bool:
        return not self.resolving and not self.open_children


class _Pending:
    """A checkpoint waiting for scopes to finish and for earlier checkpoints of its keys to be sent."""

    def __init__(self, msg: SyncCheckpointMessage, keys: List[str], sync: object):
        self.msg = msg
        self.keys = keys
        self.sync = sync
        self.blockers = 0
        self.dependents: List[_Pending] = []


class Checkpoints:
    """
    Sends state checkpoints on the scheduler's queue after the resources they cover.

    The scheduler owns an instance and passes it to the state client through
    :meth:`cloudquery.sdk.stateclient.StateClientBuilder.build`.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._scope: contextvars.ContextVar[Optional[Scope]] = contextvars.ContextVar(
            "cloudquery_checkpoint_scope", default=None
        )
        self._res: Optional[queue.Queue] = None
        # newest unsent checkpoint of each key
        self._unsent: Dict[str, _Pending] = {}
        self._sync: Optional[object] = None

    def start(self, res: queue.Queue) -> None:
        """Sends later checkpoints on res. Called by the scheduler when a sync starts."""
        with self._lock:
            self._res = res
            self._sync = object()
            self._unsent = {}

    def stop(self) -> None:
        """Drops unsent checkpoints and rejects later ones. Called by the scheduler when a sync ends."""
        with self._lock:
            self._res = None
            self._sync = None
            self._unsent = {}

    def new_scope(self, parent: Optional[Scope]) -> Scope:
        """Returns a scope for a table resolver call, before the call is submitted."""
        scope = Scope(parent, parent.sync if parent is not None else self._sync)
        if parent is not None:
            with self._lock:
                parent.open_children.add(scope)
        return scope

    @contextmanager
    def entered(self, scope: Scope) -> Iterator[None]:
        """
        Makes scope the current one in this context, for the state changes its resolver makes.
        To change state from other threads, run them in a copy of the context, e.g. with
        ``contextvars.copy_context().run``; asyncio tasks copy it already.
        """
        token = self._scope.set(scope)
        try:
            yield
        finally:
            self._scope.reset(token)

    def current_scope(self) -> Optional[Scope]:
        """Returns the scope of the table resolver running in this context, or None outside table resolvers."""
        return self._scope.get()

    def resolver_finished(self, scope: Scope) -> None:
        """
        Marks the resolver of scope as returned, once every resource it produced is on the queue.
        """
        with self._lock:
            scope.resolving = False
            ready: List[_Pending] = []
            while scope is not None and scope.finished():
                for pending in scope.waiters:
                    pending.blockers -= 1
                    if pending.blockers == 0:
                        ready.append(pending)
                scope.waiters = []
                if scope.parent is not None:
                    scope.parent.open_children.discard(scope)
                scope = scope.parent
            self._send(ready)

    def schedule(
        self,
        checkpoints: List[
            Tuple[Iterable[Optional[Scope]], List[str], SyncCheckpointMessage]
        ],
    ) -> None:
        """
        Sends each (scopes, keys, msg) once every resource the scopes produced so far is on the
        queue, including the relations spawned from them, and after every earlier unsent
        checkpoint of keys. A None scope stands for changes made outside table resolvers, which
        wait for nothing. Schedules all of them or, if it raises, none of them.

        Raises:
            RuntimeError: if no sync is running, as the checkpoints could not be sent.
        """
        with self._lock:
            if self._res is None:
                raise RuntimeError(
                    "state checkpoints can only be flushed while the scheduler syncs, "
                    "e.g. from a table resolver or the post-sync hook"
                )
            for scopes, _, _ in checkpoints:
                if any(s is not None and s.sync is not self._sync for s in scopes):
                    raise RuntimeError(
                        "state checkpoints of a table resolver can only be flushed during its own sync"
                    )
            for scopes, keys, msg in checkpoints:
                self._schedule(scopes, keys, msg)

    def _schedule(
        self,
        scopes: Iterable[Optional[Scope]],
        keys: List[str],
        msg: SyncCheckpointMessage,
    ) -> None:
        pending = _Pending(msg, keys, self._sync)
        for scope in {s for s in scopes if s is not None}:
            # the scope's own resources are on the queue already: resolvers put each
            # resource on it before they resume, so only its relations can be in flight
            for child in scope.open_children:
                child.waiters.append(pending)
                pending.blockers += 1
        earlier = set()
        for key in keys:
            previous = self._unsent.get(key)
            if previous is not None and previous not in earlier:
                earlier.add(previous)
                previous.dependents.append(pending)
                pending.blockers += 1
            self._unsent[key] = pending
        if pending.blockers == 0:
            self._send([pending])

    def _send(self, ready: List[_Pending]) -> None:
        """Puts ready checkpoints on the queue, then the ones that only waited for them. Called with the lock held."""
        while ready:
            pending = ready.pop()
            if pending.sync is self._sync:
                self._res.put(pending.msg)
            for key in pending.keys:
                if self._unsent.get(key) is pending:
                    del self._unsent[key]
            for dependent in pending.dependents:
                dependent.blockers -= 1
                if dependent.blockers == 0:
                    ready.append(dependent)
            pending.dependents = []
