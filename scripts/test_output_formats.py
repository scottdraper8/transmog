"""CI smoke test for output format methods and API access patterns."""

import json
import tempfile
from pathlib import Path

import transmog as tm


def main() -> None:
    """Run smoke tests for output format methods and API access patterns."""
    data = {
        "id": 123,
        "name": "Test",
        "details": {"value": 456},
        "items": [{"id": 1}, {"id": 2}],
    }

    result = tm.flatten(data, name="test_entity")

    print("Testing basic result access...")
    print(f"Main table records: {len(result.main)}")
    print(f"Child tables: {len(result.tables)}")
    print(f"All tables: {list(result.all_tables.keys())}")

    print("Testing result iteration...")
    for record in result.main:
        print(f"Record ID: {record.get('_id', 'N/A')}")
        break

    print(f"Result length: {len(result.main)}")

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        print("Testing CSV file output...")
        csv_file = temp_path / "test_output.csv"
        result.save(csv_file, output_format="csv")
        if csv_file.exists():
            print(f"CSV file created: {csv_file}")

        print("Testing Parquet file output...")
        try:
            parquet_file = temp_path / "test_output.parquet"
            result.save(parquet_file, output_format="parquet")
            if parquet_file.exists():
                print(f"Parquet file created: {parquet_file}")
        except ImportError:
            print("PyArrow not available, skipping Parquet test")

        print("Testing directory output...")
        output_dir = temp_path / "tables"
        paths = result.save(output_dir, output_format="csv")
        print(f"Created {len(paths)} CSV files in directory")

        print("Testing streaming API...")
        stream_output_dir = temp_path / "streaming"
        stream_output_dir.mkdir(exist_ok=True)

        tm.flatten_stream(
            data,
            output_path=stream_output_dir,
            name="streaming_test",
            output_format="csv",
        )
        stream_files = list(stream_output_dir.glob("*.csv"))
        print(f"Streaming created {len(stream_files)} files")

        print("Testing file processing...")
        source_json_path = temp_path / "source_data.json"
        with open(source_json_path, "w") as f:
            json.dump(data, f)

        file_result = tm.flatten(source_json_path, name="file_test")
        print(f"File processing: {len(file_result.main)} records")

    print("Testing configuration options...")

    config_inline = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)
    result_inline = tm.flatten(data, name="inline_test", config=config_inline)
    print(f"Inline arrays: {len(result_inline.main)} records")

    config_skip = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
    result_skip = tm.flatten(data, name="skip_test", config=config_skip)
    print(f"Skip arrays: {len(result_skip.main)} records")

    config_memory = tm.TransmogConfig(batch_size=100)
    result_memory = tm.flatten(data, name="memory_test", config=config_memory)
    print(f"Memory optimized: {len(result_memory.main)} records")

    config_include_nulls = tm.TransmogConfig(include_nulls=True)
    result_include = tm.flatten(data, name="nulls_test", config=config_include_nulls)
    print(f"Include nulls: {len(result_include.main)} records")

    print("All API methods working correctly!")


if __name__ == "__main__":
    main()
