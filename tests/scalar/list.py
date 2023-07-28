from cloudquery.sdk.scalar import String, Bool, List


def test_list():
    s = type(String())
    l = List(s)
    assert l == List(s)

    l.set([String(True, "a string"), String(True, "another string"), String(False)])
    assert len(l) == 3


def test_list_eq():
    s = type(String())
    l1 = List(s)
    l1.set([String(True, "a string"), String(True, "another string"), String(False)])

    l2 = List(s)
    l2.set([String(True, "a string"), String(True, "another string"), String(False)])

    assert l1 == l2


def test_list_ineq():
    s = type(String())
    l1 = List(s)
    l1.set([String(True, "a string")])

    l2 = List(s)
    l2.set([String(True, "another string")])

    assert l1 != l2


def test_list_eq_invalid():
    s = type(String())
    l1 = List(s)
    l1.set([String(False)])

    l2 = List(s)
    l2.set([String(False)])

    assert l1 == l2
