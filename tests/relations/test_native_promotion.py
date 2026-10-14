"""Compatible numeric schemas remain queryable native types without caller configuration."""

import warnings

import pyarrow as pa
import pytest

from tests.helpers import read_arrow_table
from transmog.writers import create_streaming_writer, create_writer


@pytest.mark.claim("C-WRITER-NATIVE-PROMOTION")
@pytest.mark.parametrize("output_format", ("parquet", "orc"))
@pytest.mark.parametrize("batch_size", (1, 5000))
def test_compatible_numbers_and_lists_remain_native(
    tmp_path, output_format, batch_size
):
    """P: representable numeric promotions use native floats, not unnecessary JSON."""
    records = [{"value": 1, "values": [1]}, {"value": 2.5, "values": [2.5]}]
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
    assert len(paths) == 1
    for path in (saved, *paths):
        table = read_arrow_table(path, output_format)
        assert table.to_pylist() == records
        assert table.schema.field("value").type == pa.float64()
        assert table.schema.field("values").type == pa.list_(pa.float64())
        assert all(
            not (field.metadata or {}).get(b"transmog.encoding")
            for field in table.schema
        )
