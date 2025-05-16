"""Example demonstrating native output format capabilities in Transmog.

This example shows how to use the output format methods:
- to_dict()
- to_json_objects()
- to_pyarrow_tables()
- to_parquet_bytes()
- to_csv_bytes()
- to_json_bytes()
"""

import io
import os
import sys
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor
from transmog.process.result import ConversionMode


def main():
    """Run the example demonstrating native output formats."""
    # Sample nested data
    data = {
        "organization": {
            "id": "org123",
            "name": "Example Org",
            "founded": 2020,
            "active": True,
            "contacts": [
                {
                    "type": "primary",
                    "name": "John Doe",
                    "email": "john@example.com",
                    "phone": "555-1234",
                },
                {
                    "type": "billing",
                    "name": "Jane Smith",
                    "email": "jane@example.com",
                    "phone": "555-5678",
                },
            ],
            "locations": [
                {
                    "id": "loc1",
                    "city": "New York",
                    "employees": 150,
                    "departments": [
                        {"name": "Sales", "manager": "Bob Wilson"},
                        {"name": "Engineering", "manager": "Alice Johnson"},
                    ],
                },
                {
                    "id": "loc2",
                    "city": "Boston",
                    "employees": 75,
                    "departments": [
                        {"name": "Marketing", "manager": "Carol Brown"},
                    ],
                },
            ],
        }
    }

    # Process the data
    processor = Processor()
    result = processor.process(data, entity_name="organization")

    print("\n=== 1. Native Dictionary Output ===")
    print("Get data as Python dictionaries:")
    dict_output = result.to_dict()
    print(f"Tables: {list(dict_output.keys())}")
    print("Main table sample:")
    pprint(dict_output["main"][0], depth=1)  # Show just the first record, limited depth

    # Show first record of each child table
    for table_name, table_data in dict_output.items():
        if table_name != "main" and table_data:
            print(f"\n{table_name} table sample (1 of {len(table_data)}):")
            pprint(table_data[0], depth=1)
            break  # Just show one child table as an example

    print("\n=== 2. JSON-Serializable Objects ===")
    print("Get data as JSON-serializable objects:")
    json_objects = result.to_json_objects()
    # Test JSON serialization
    import json

    json_str = json.dumps(json_objects["main"], indent=2)
    print(f"Successfully serialized {len(json_str)} bytes of JSON data")

    # Show the difference between to_dict and to_json_objects
    print("\nDifference between to_dict() and to_json_objects():")
    if dict_output["main"] and "__extract_datetime" in dict_output["main"][0]:
        dt_value_dict = dict_output["main"][0]["__extract_datetime"]
        dt_value_json = json_objects["main"][0]["__extract_datetime"]
        print(f"to_dict() datetime type: {type(dt_value_dict).__name__}")
        print(f"to_json_objects() datetime type: {type(dt_value_json).__name__}")

    print("\n=== 3. PyArrow Tables ===")
    print("Get data as PyArrow Tables:")
    try:
        tables = result.to_pyarrow_tables()
        main_table = tables["main"]
        print(
            f"Main table has {main_table.num_rows} rows and "
            f"{len(main_table.column_names)} columns"
        )
        print(f"Column names: {main_table.column_names[:5]}...")  # First few columns

        # Show schema info
        print("\nSchema information:")
        print(main_table.schema)

        # Demonstrate working with PyArrow table
        print("\nWorking with PyArrow:")
        # Convert to pandas if available
        try:
            # Import pandas only when needed
            df = main_table.to_pandas()
            print(f"Converted to pandas DataFrame with shape: {df.shape}")
        except ImportError:
            print("Pandas not available")

    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    print("\n=== 4. In-Memory Bytes Output ===")
    # Demonstrate bytes output for each format

    # 4.1 JSON bytes
    print("\nJSON bytes output:")
    json_bytes = result.to_json_bytes(indent=None)  # No indentation for smaller output
    print(f"Main table: {len(json_bytes['main'])} bytes")
    # Show a small snippet of the JSON
    print(f"Sample: {json_bytes['main'][:60].decode('utf-8')}...")

    # 4.2 CSV bytes
    print("\nCSV bytes output:")
    csv_bytes = result.to_csv_bytes()
    print(f"Main table: {len(csv_bytes['main'])} bytes")
    # Show a small snippet of the CSV
    print(f"Sample: {csv_bytes['main'][:60].decode('utf-8')}...")

    # 4.3 Parquet bytes
    print("\nParquet bytes output:")
    try:
        parquet_bytes = result.to_parquet_bytes()
        print(f"Main table: {len(parquet_bytes['main'])} bytes")

        # Demonstrate reading the bytes back into a PyArrow table
        import pyarrow.parquet as pq

        buffer = io.BytesIO(parquet_bytes["main"])
        table = pq.read_table(buffer)
        print(f"Successfully read back {table.num_rows} rows from Parquet bytes")

    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    print("\n=== 5. Memory Efficiency Options ===")
    # Demonstrate the different conversion modes

    # Default mode (EAGER)
    print("Default conversion mode (EAGER):")
    eager_result = result.with_conversion_mode(ConversionMode.EAGER)
    # First conversion creates and caches the result
    _ = eager_result.to_json_bytes()
    # Second conversion reuses the cached result
    start_time = __import__("time").time()
    _ = eager_result.to_json_bytes()
    end_time = __import__("time").time()
    print(f"  Second conversion time (cached): {(end_time - start_time) * 1000:.3f} ms")

    # LAZY mode
    print("\nLAZY conversion mode:")
    lazy_result = result.with_conversion_mode(ConversionMode.LAZY)
    # First conversion
    _ = lazy_result.to_json_bytes()
    # Second conversion computes again
    start_time = __import__("time").time()
    _ = lazy_result.to_json_bytes()
    end_time = __import__("time").time()
    print(
        f"  Second conversion time (recomputed): "
        f"{(end_time - start_time) * 1000:.3f} ms"
    )

    # MEMORY_EFFICIENT mode
    print("\nMEMORY_EFFICIENT conversion mode:")
    efficient_result = result.with_conversion_mode(ConversionMode.MEMORY_EFFICIENT)
    # Conversion and clearing
    _ = efficient_result.to_json_bytes()
    print("  After conversion, intermediate data is cleared to save memory")

    print("\n=== 6. Writing to files ===")
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Write to different formats
    parquet_files = {}
    try:
        parquet_files = result.write_all_parquet(os.path.join(output_dir, "parquet"))
        print(f"Wrote {len(parquet_files)} Parquet files")
    except ImportError:
        print("PyArrow not available. Unable to write Parquet files.")

    csv_files = result.write_all_csv(os.path.join(output_dir, "csv"))
    json_files = result.write_all_json(os.path.join(output_dir, "json"))

    print(f"Wrote {len(csv_files)} CSV files")
    print(f"Wrote {len(json_files)} JSON files")
    print(f"Output directory: {output_dir}")

    # 6.1 Using the generic writer interface
    print("\nGeneric writer interface:")
    result.write(
        format_name="json", base_path=os.path.join(output_dir, "generic"), indent=2
    )
    print(f"Written to {os.path.join(output_dir, 'generic')}")

    # 6.2 Streaming to output
    print("\nStreaming to output:")
    try:
        # Stream directly to a file object
        with open(os.path.join(output_dir, "streamed_main.json"), "wb") as f:
            result.stream_to_output(
                format_name="json", output_destination=f, table_name="main", indent=2
            )
        print(f"Streamed to {os.path.join(output_dir, 'streamed_main.json')}")
    except Exception as e:
        print(f"Error streaming: {e}")

    # 7. Table operations
    print("\n=== 7. Table Operations ===")
    # Count records
    record_counts = result.count_records()
    print("Record counts by table:")
    for table, count in record_counts.items():
        print(f"  {table}: {count} records")


if __name__ == "__main__":
    main()
