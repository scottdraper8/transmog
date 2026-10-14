"""Automatic typed-file representations require no caller knowledge of the schema."""

import math
import warnings

import pytest

from tests.helpers import decode_file, read_arrow_table
from transmog.writers import create_streaming_writer, create_writer


@pytest.mark.claim("C-WRITER-AUTOMATIC-SCHEMA")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize("batch_size", (1, 2, 5000))
@pytest.mark.parametrize("consolidate", (False, True))
@pytest.mark.parametrize("coerce_schema", (False, True))
def test_heterogeneous_columns_are_encoded_without_caller_schema(
    tmp_path, output_format, batch_size, consolidate, coerce_schema
):
    """P: numbers, booleans, strings, containers, and null survive automatic inference."""
    values = [None, "1", 1, True, [1, "x", None], {"a": [False, "日本語"]}]
    records = [
        {"value": value, "plain": "null", "seq": index}
        for index, value in enumerate(values)
    ]
    saved = tmp_path / f"saved.{output_format}"
    create_writer(output_format).write(records, str(saved))
    writer = create_streaming_writer(
        output_format,
        destination=str(tmp_path / "stream"),
        entity_name="items",
        batch_size=batch_size,
        consolidate=consolidate,
        coerce_schema=coerce_schema,
    )
    for record in records:
        writer.write_main_records([record])
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Schema deviations", category=UserWarning
        )
        paths = writer.close()
    assert len(paths) == (1 if consolidate else math.ceil(len(records) / batch_size))
    saved_rows = decode_file(saved, output_format, logical=True)
    streamed_rows = [
        row for path in paths for row in decode_file(path, output_format, logical=True)
    ]
    assert saved_rows == streamed_rows == records
    for rows in (saved_rows, streamed_rows):
        assert [type(row["value"]) for row in rows] == [type(value) for value in values]
    for path in [saved, *paths]:
        table = read_arrow_table(path, output_format)
        assert not (table.schema.field("plain").metadata or {}).get(
            b"transmog.encoding"
        )
        assert table["plain"].to_pylist() == ["null"] * len(table)
        if path == saved or consolidate or coerce_schema or batch_size >= len(records):
            assert (table.schema.field("value").metadata or {}).get(
                b"transmog.encoding"
            ) == b"json"
    # A numeric-looking source string must remain distinct from a numeric value.
    assert read_arrow_table(saved, output_format)["value"].to_pylist() == [
        None,
        '"1"',
        "1",
        "true",
        '[1, "x", null]',
        '{"a": [false, "日本語"]}',
    ]


@pytest.mark.claim("C-WRITER-PRECISION")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize("integer", (2**53 + 1, 2**80))
@pytest.mark.parametrize("batch_size", (1, 5000))
@pytest.mark.parametrize(
    "consolidate,coerce_schema", ((True, False), (False, True), (False, False))
)
def test_unrepresentable_numeric_promotions_use_exact_json(
    tmp_path, output_format, integer, batch_size, consolidate, coerce_schema
):
    """P: numeric promotion never rounds a large integer to a nearby float."""
    records = [{"value": integer}, {"value": 0.5}]
    saved = tmp_path / f"saved.{output_format}"
    create_writer(output_format).write(records, str(saved))
    writer = create_streaming_writer(
        output_format,
        destination=str(tmp_path / "stream"),
        entity_name="items",
        batch_size=batch_size,
        consolidate=consolidate,
        coerce_schema=coerce_schema,
    )
    for record in records:
        writer.write_main_records([record])
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Schema deviations", category=UserWarning
        )
        paths = writer.close()
    for rows in (
        decode_file(saved, output_format, logical=True),
        [
            row
            for path in paths
            for row in decode_file(path, output_format, logical=True)
        ],
    ):
        assert rows == records
        assert type(rows[0]["value"]) is int
        assert type(rows[1]["value"]) is float
    if consolidate or coerce_schema:
        for path in paths:
            field = read_arrow_table(path, output_format).schema.field("value")
            assert (field.metadata or {}).get(b"transmog.encoding") == b"json"


@pytest.mark.claim("C-WRITER-NULL-INFERENCE")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize(
    "initial,value",
    (
        (None, [1, 2]),
        (None, {"nested": [3]}),
        (None, 2.5),
        ([None], [[2]]),
        ([[None]], [[2]]),
    ),
)
def test_null_storage_placeholders_do_not_force_text(
    tmp_path, output_format, initial, value
):
    """P: an unresolved null storage type does not override later native inference."""
    writer = create_streaming_writer(
        output_format, destination=str(tmp_path), entity_name="items", batch_size=1
    )
    writer.write_main_records([{"value": initial}])
    writer.write_main_records([{"value": value}])
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Schema deviations", category=UserWarning
        )
        paths = writer.close()
    assert len(paths) == 1
    table = read_arrow_table(paths[0], output_format)
    assert table.to_pylist() == [{"value": initial}, {"value": value}]
    assert not (table.schema.field("value").metadata or {}).get(b"transmog.encoding")


@pytest.mark.claim("C-WRITER-MIXED-COLUMN")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize("streaming", (False, True))
def test_heterogeneous_column_requires_no_caller_schema(
    tmp_path, output_format, streaming
):
    """P: heterogeneous JSON values need no caller schema or manual stringification."""
    records = [{"value": "text"}, {"value": 7}, {"value": [1, 2]}]
    path = tmp_path / f"items.{output_format}"
    if streaming:
        writer = create_streaming_writer(
            output_format, destination=str(tmp_path), entity_name="items"
        )
        writer.write_main_records(records)
        assert writer.close() == [path]
    else:
        create_writer(output_format).write(records, str(path))
    assert decode_file(path, output_format, logical=True) == records
    assert decode_file(path, output_format) == [
        {"value": '"text"'},
        {"value": "7"},
        {"value": "[1, 2]"},
    ]
    field = read_arrow_table(path, output_format).schema.field("value")
    assert (field.metadata or {}).get(b"transmog.encoding") == b"json"
