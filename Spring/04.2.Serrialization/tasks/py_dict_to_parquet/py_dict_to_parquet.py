import pyarrow as pa
import pyarrow.parquet as pq
from collections import OrderedDict


ValueType = int | list[int] | str | dict[str, str]

def _infer_type(value) -> pa.DataType:
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, list):
        return pa.list_(pa.int64())
    if isinstance(value, str):
        return pa.string()
    if isinstance(value, dict):
        return pa.map_(pa.string(), pa.string())
    raise TypeError(f"Unsupported value type: {type(value).__name__!r}")

def save_rows_to_parquet(rows: list[dict[str, ValueType]], output_filepath: str) -> None:
    """
    Save rows to parquet file.

    :param rows: list of rows containing data.
    :param output_filepath: local filepath for the resulting parquet file.
    :return: None.
    """
    columns_type: OrderedDict[str, pa.DataType] = OrderedDict()
    columns_null: dict[str, bool] = {}

    for row in rows:
        for k,v in row.items():
            tmp = _infer_type(v)
            if k in columns_type:
                if columns_type[k] != tmp:
                    raise TypeError(f"Field {k} has different types")
            else:
                columns_type[k] = tmp

    for k in columns_type.keys():
        columns_null[k] = any([k not in s for s in
                              [set(row.keys()) for row in rows]])

    schema = pa.schema([pa.field(k, v, nullable=columns_null[k])
                        for k,v in columns_type.items()])

    columns: dict[str, list] = {k:[] for k in columns_type}

    for row in rows:
        for k in columns_type:
            v = row.get(k)
            if v is not None and isinstance(v, dict):
                v = list(v.items())
            columns[k].append(v)

    arrays = [
        pa.array(columns[k], type=schema.field(k).type)
        for k in columns_type
    ]
    table = pa.table(
        {name: arr for name, arr in zip(columns_type, arrays)},
        schema=schema,
    )
    pq.write_table(table, output_filepath)
