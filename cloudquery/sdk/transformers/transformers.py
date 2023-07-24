from typing import List, Dict

import pyarrow as pa

from cloudquery.sdk.schema import Table


def transform_list_of_dict(data: List[Dict], table: Table):
    """
    Transform json data to arrow record using the given table schema
    """
    schema = table.to_arrow_schemas()
    return pa.RecordBatch.from_pylist(data, schema)
