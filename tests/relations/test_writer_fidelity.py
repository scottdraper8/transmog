"""Value-preservation sensors for native types and evolving part schemas."""

from __future__ import annotations

import math
import warnings

import pytest

import transmog as tm
from tests.helpers import decode_file, hash_config, payload
from transmog.writers import create_streaming_writer, create_writer

FORMATS = ("csv", "parquet", "orc", "avro")


@pytest.mark.claim("C-WRITER-NUMERIC")
@pytest.mark.parametrize("output_format", ("parquet", "orc", "avro"))
@pytest.mark.parametrize("batch_size", (1, 2, 5000))
@pytest.mark.parametrize("consolidate", (False, True))
@pytest.mark.parametrize("coerce_schema", (False, True))
@pytest.mark.parametrize(
    "data",
    [
        [{"value": 1}, {"value": 2.5}],
        [{"values": [1, 2.5]}, {"values": [3.75]}],
        [{"values": [None]}, {"values": [2]}],
        [{"values": [[1, 2], [3, 4]]}, {"values": [[5]]}],
        [{"value": None}, {"value": 2.5, "extra": "new"}],
    ],
    ids=("scalar-promotion", "list-promotion", "null-first", "nested-list", "sparse"),
)
def test_save_and_stream_preserve_values(
    tmp_path, output_format, batch_size, consolidate, coerce_schema, data
):
    """P: numeric values, lists, and sparse fields survive both writer paths."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        config = hash_config(batch_size=batch_size, include_nulls=True)
        result = tm.flatten(data, name="items", config=config)
        save_path = tmp_path / f"save.{output_format}"
        result.save(save_path, output_format=output_format)
        paths = tm.flatten_stream(
            data,
            tmp_path / "stream",
            name="items",
            config=config,
            output_format=output_format,
            consolidate=consolidate,
            coerce_schema=coerce_schema,
        )
    saved = [
        payload(row) for row in decode_file(save_path, output_format, logical=True)
    ]
    streamed = [
        payload(row)
        for path in sorted(paths)
        for row in decode_file(path, output_format, logical=True)
    ]
    # Uncoerced parts need not contain columns discovered in later batches.
    fields = set().union(*(row.keys() for row in saved))
    streamed = [{key: row.get(key) for key in fields} for row in streamed]
    assert streamed == saved
    for source, decoded in zip(data, saved, strict=True):
        for key, value in source.items():
            if isinstance(value, list) and output_format == "avro":
                import json

                assert json.loads(decoded[key]) == value
            else:
                assert decoded[key] == value


@pytest.mark.claim("C-WRITER-NATIVE-UNION")
@pytest.mark.parametrize("batch_size", (1, 5000))
@pytest.mark.parametrize("coerce_schema", (False, True))
def test_avro_union_preserves_native_type(tmp_path, batch_size, coerce_schema):
    """P: an Avro union retains booleans, numeric strings, bytes, and numbers."""
    values = [42, "123", True, 3.5, b"bytes", None]
    records = [{"value": value} for value in values]
    writer = create_writer("avro")
    saved = tmp_path / "save.avro"
    writer.write(records, str(saved))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        stream = create_streaming_writer(
            "avro",
            destination=str(tmp_path / "stream"),
            entity_name="items",
            batch_size=batch_size,
            coerce_schema=coerce_schema,
        )
        for record in records:
            stream.write_main_records([record])
        paths = stream.close()
    for path in [saved, *paths]:
        rows = decode_file(path, "avro")
        assert [row["value"] for row in rows] == values
        assert [type(row["value"]) for row in rows] == [type(value) for value in values]


@pytest.mark.claim("C-WRITER-NULL")
@pytest.mark.parametrize("output_format", FORMATS)
@pytest.mark.parametrize("streaming", (False, True))
def test_direct_writer_special_float_semantics(tmp_path, output_format, streaming):
    """P: CSV/Avro normalize; Arrow preserves IEEE values without preprocessing."""
    records = [{"value": value} for value in (42.5, math.nan, math.inf, -math.inf)]
    if streaming:
        writer = create_streaming_writer(
            output_format,
            destination=str(tmp_path),
            entity_name="items",
            batch_size=2,
        )
        writer.write_main_records(records)
        paths = writer.close()
        assert paths == [tmp_path / f"items.{output_format}"]
        path = paths[0]
    else:
        path = tmp_path / f"items.{output_format}"
        create_writer(output_format).write(records, str(path))
    rows = decode_file(path, output_format)
    values = [row["value"] for row in rows]
    if output_format in ("parquet", "orc"):
        assert values[0] == 42.5
        assert math.isnan(values[1])
        assert values[2:] == [math.inf, -math.inf]
    else:
        expected = (
            ["42.5", "", "", ""] if output_format == "csv" else [42.5, None, None, None]
        )
        assert values == expected
