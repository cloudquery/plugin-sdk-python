from cloudquery.sdk.scalar import String, Bool, Vector


def test_vector_append():
    s = type(String())
    v = Vector(s)
    assert v == Vector(s)

    v.append(String(True, "a string"))
    v.append(String(True, "another string"))
    v.append(String(False))
    assert len(v) == 3


def test_vector_invalid_type_append():
    s = type(String())
    v = Vector(s)
    b = Bool(True, True)
    try:
        v.append(b)
        assert False
    except:
        assert True


def test_vector_eq():
    s = type(String())
    v1 = Vector(s)
    v1.append(String(True, "a string"))

    v2 = Vector(s)
    v2.append(String(True, "a string"))

    assert v1 == v2


def test_vector_ineq():
    s = type(String())
    v1 = Vector(s)
    v1.append(String(True, "a string"))

    v2 = Vector(s)
    v2.append(String(True, "another string"))

    assert v1 != v2


def test_vector_ineq_order():
    s = type(String())
    v1 = Vector(s)
    v1.append(String(True, "a"))
    v1.append(String(True, "b"))

    v2 = Vector(s)
    v2.append(String(True, "b"))
    v2.append(String(True, "a"))

    assert v1 != v2
