"""Nested JSON object key presence survives native inference and part merging."""

import warnings

import pytest

from tests.helpers import decode_file
from transmog.writers import create_streaming_writer, create_writer


@pytest.mark.claim("C-WRITER-OBJECT-STRUCTURE")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize("batch_size", (1, 5000))
@pytest.mark.parametrize(
    "values",
    (
        [{}, {}],
        [[{}], [[{}]]],
        [{"a": {}}, {"a": {}}],
        [{"a": 1}, {"b": 2}],
        [[{"a": 1}, {"b": 2}]],
        [{"a": None}, {"a": [1]}],
    ),
)
def test_empty_and_sparse_objects_preserve_exact_structure(
    tmp_path, output_format, batch_size, values
):
    """P: empty objects are writable and missing nested keys are not invented."""
    records = [{"value": value} for value in values]
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
    assert decode_file(saved, output_format, logical=True) == records
    assert [
        row for path in paths for row in decode_file(path, output_format, logical=True)
    ] == records
