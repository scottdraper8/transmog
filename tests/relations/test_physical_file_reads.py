"""Reading a writer's own part files must not infer data from output directory names."""

import warnings

import pytest

from tests.helpers import decode_file
from transmog.writers import create_streaming_writer


@pytest.mark.claim("C-WRITER-PHYSICAL-FILE")
@pytest.mark.parametrize("consolidate,coerce_schema", ((True, False), (False, True)))
def test_parquet_directory_names_do_not_supply_partition_columns(
    tmp_path, consolidate, coerce_schema
):
    """P: consolidation/coercion reads actual rows, not inferred Hive partitions."""
    writer = create_streaming_writer(
        "parquet",
        destination=str(tmp_path / "year=2025"),
        entity_name="items",
        batch_size=1,
        consolidate=consolidate,
        coerce_schema=coerce_schema,
    )
    writer.write_main_records([{"year": 2024}])
    writer.write_main_records([{"year": 2023, "extra": "new"}])
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", message="Schema deviations", category=UserWarning
        )
        paths = writer.close()
    assert [row for path in paths for row in decode_file(path, "parquet")] == [
        {"year": 2024, "extra": None},
        {"year": 2023, "extra": "new"},
    ]
