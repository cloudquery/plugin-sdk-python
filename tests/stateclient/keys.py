from cloudquery.sdk.stateclient import state_key


def test_table_key():
    assert state_key("items") == "items"


def test_partition_fields_are_sorted():
    assert state_key("t", b="2", a=1) == state_key("t", a=1, b="2") == "t|a=1|b=2"


def test_client_id_comes_before_partition_fields():
    assert state_key("t", client_id="acct", a="x") == "t|client_id=acct|a=x"


def test_separators_are_escaped():
    assert state_key("t", a="x|b=y") == "t|a=x\\|b\\=y"
    assert state_key("t", a="x|b=y") != state_key("t", a="x", b="y")
    assert state_key("t", a="x\\") != state_key("t", a="x\\\\")


def test_tables_clients_and_partitions_never_share_a_key():
    keys = [
        state_key("t"),
        state_key("t|a=1"),
        state_key("t", a=1),
        state_key("t", client_id="a=1"),
        state_key("t", client_id="c", a=1),
        state_key("t", client_id="c|a=1"),
        state_key("t", a="1|b=2"),
        state_key("t", a=1, b=2),
    ]
    assert len(set(keys)) == len(keys)


def test_client_id_that_is_not_a_string():
    assert state_key("t", client_id=123) == "t|client_id=123"
