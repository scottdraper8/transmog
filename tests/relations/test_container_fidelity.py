"""Direct container serialization retains structure and explicit null semantics."""

import json
import math

import pytest

from tests.helpers import decode_file
from transmog.writers import create_streaming_writer, create_writer


@pytest.mark.claim("C-WRITER-CONTAINER")
@pytest.mark.parametrize("output_format", ("csv", "avro"))
@pytest.mark.parametrize("streaming", (False, True))
def test_nonfinite_container_elements_use_json_null(tmp_path, output_format, streaming):
    """P: non-finite elements become null recursively, without invalid JSON tokens."""
    records = [{"values": [1.5, math.nan, [math.inf, "日本語"], {"x": -math.inf}]}]
    path = tmp_path / f"items.{output_format}"
    if streaming:
        writer = create_streaming_writer(
            output_format, destination=str(tmp_path), entity_name="items"
        )
        writer.write_main_records(records)
        assert writer.close() == [path]
    else:
        create_writer(output_format).write(records, str(path))
    rows = decode_file(path, output_format)
    assert len(rows) == 1
    assert rows[0]["values"] == '[1.5, null, [null, "日本語"], {"x": null}]'
    assert json.loads(rows[0]["values"]) == [1.5, None, [None, "日本語"], {"x": None}]


@pytest.mark.claim("C-WRITER-ARROW-OPTIONS")
@pytest.mark.parametrize("streaming", (False, True))
def test_parquet_options_are_honored(tmp_path, streaming):
    """P: constructor and per-write options reach real Parquet metadata."""
    import pyarrow.parquet as pq

    records = [{"value": 1}, {"value": 2}]
    path = tmp_path / "items.parquet"
    if streaming:
        writer = create_streaming_writer(
            "parquet",
            destination=str(tmp_path),
            entity_name="items",
            compression="gzip",
            use_dictionary=False,
        )
        writer.write_main_records(records)
        assert writer.close() == [path]
    else:
        create_writer("parquet", compression="snappy", use_dictionary=False).write(
            records, str(path), compression="gzip"
        )
    table = pq.read_table(path)
    assert table.to_pylist() == records
    column = pq.read_metadata(path).row_group(0).column(0)
    assert column.compression == "GZIP"
    assert "RLE_DICTIONARY" not in column.encodings
