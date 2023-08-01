from cloudquery.sdk.types import JSONType


def test_json_type():
    j = JSONType()
    # test equality
    assert j == JSONType()
