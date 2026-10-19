"""Builds state keys, so that plugins choose how finely they track progress without inventing key formats."""

from typing import Optional, Union

_SEPARATOR = "|"
_ASSIGN = "="
_ESCAPE = "\\"


def _escape(part: str) -> str:
    return (
        part.replace(_ESCAPE, _ESCAPE * 2)
        .replace(_SEPARATOR, _ESCAPE + _SEPARATOR)
        .replace(_ASSIGN, _ESCAPE + _ASSIGN)
    )


def state_key(
    table: str,
    *,
    client_id: Optional[str] = None,
    **partition: Union[str, int],
) -> str:
    """
    Returns the state key of a table's progress, optionally for one client and one partition of the table.

    A table with one cursor uses ``state_key("items")``. A table that tracks progress per parent, account or
    other partition passes the fields that identify it, e.g. ``state_key("responses", form_id=form["id"])``.
    Each such key is its own state row: with checkpoints, a partition's state waits only for the resources of
    the table resolver that set it, and the CLI persists the keys changed since its last commit together.

    Keys are readable and deterministic: partition fields are sorted, and separators in names and values are
    escaped, so different tables, clients or partitions never share a key.

    Example:
        >>> state_key("responses", client_id="acct-1", form_id=123)
        'responses|client_id=acct-1|form_id=123'
    """
    parts = [_escape(table)]
    if client_id is not None:
        parts.append(f"client_id{_ASSIGN}{_escape(client_id)}")
    for name in sorted(partition):
        parts.append(f"{_escape(name)}{_ASSIGN}{_escape(str(partition[name]))}")
    return _SEPARATOR.join(parts)
