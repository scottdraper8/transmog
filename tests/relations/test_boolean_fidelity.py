"""Arrow's successful inference must not convert JSON booleans to numbers."""

import json
import warnings

import pytest

from tests.helpers import decode_file, read_arrow_table
from transmog.writers import create_streaming_writer, create_writer


@pytest.mark.claim("C-WRITER-BOOLEAN-KIND")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize("batch_size", (1, 5000))
@pytest.mark.parametrize(
    "records",
    (
        [{"value": 0.0}, {"value": False}],
        [{"value": [0.0, False]}],
        [{"value": {"nested": [0.0, False]}}],
    ),
)
def test_boolean_after_float_preserves_json_kind(
    tmp_path, output_format, batch_size, records
):
    """P: false is represented as JSON false, never the numerically equal 0.0."""
    saved = tmp_path / f"saved.{output_format}"
    create_writer(output_format).write(records, str(saved))
    writer = create_streaming_writer(
        output_format,
        destination=str(tmp_path / "stream"),
        entity_name="items",
        batch_size=batch_size,
    )
    for record in records:
        writer.write_main_records([record])
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Schema deviations", category=UserWarning
        )
        paths = writer.close()
    for path in (saved, *paths):
        table = read_arrow_table(path, output_format)
        assert (table.schema.field("value").metadata or {}).get(
            b"transmog.encoding"
        ) == b"json"
        assert table["value"].to_pylist() == [
            json.dumps(record["value"], ensure_ascii=False) for record in records
        ]
        assert decode_file(path, output_format, logical=True) == records
