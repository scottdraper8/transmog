"""
Example demonstrating native output format capabilities in Transmog.

This example shows how to use the new output format methods:
- to_dict()
- to_json_objects()
- to_pyarrow_tables()
- to_parquet_bytes()
- to_csv_bytes()
- to_json_bytes()
"""

import os
import sys
import io
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from src.transmog import Processor


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
    processor = Processor(visit_arrays=True)
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

    print("\n=== 3. PyArrow Tables ===")
    print("Get data as PyArrow Tables:")
    try:
        tables = result.to_pyarrow_tables()
        main_table = tables["main"]
        print(
            f"Main table has {main_table.num_rows} rows and {len(main_table.column_names)} columns"
        )
        print(f"Column names: {main_table.column_names[:5]}...")  # First few columns

        # Show schema info
        print("\nSchema information:")
        print(main_table.schema)

    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    print("\n=== 4. In-Memory Bytes Output ===")
    # Demonstrate bytes output for each format

    # 4.1 JSON bytes
    print("\nJSON bytes output:")
    json_bytes = result.to_json_bytes(indent=None)  # No indentation for smaller output
    print(f"Main table: {len(json_bytes['main'])} bytes")
    # Show a small snippet of the JSON
    print(f"Sample: {json_bytes['main'][:60]}...")

    # 4.2 CSV bytes
    print("\nCSV bytes output:")
    csv_bytes = result.to_csv_bytes()
    print(f"Main table: {len(csv_bytes['main'])} bytes")
    # Show a small snippet of the CSV
    print(f"Sample: {csv_bytes['main'][:60]}...")

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

    print("\n=== 5. Integration with other libraries ===")
    try:
        # Here you could demonstrate working directly with PyArrow
        print("This section demonstrates working directly with PyArrow tables:")

        tables = result.to_pyarrow_tables()
        main_table = tables["main"]

        # Demonstrate some PyArrow operations
        print(
            f"PyArrow table info: {main_table.num_rows} rows, {main_table.num_columns} columns"
        )

        # Show how to work with a specific column
        if "organization_name" in main_table.column_names:
            col = main_table.column("organization_name")
            print(
                f"Sample values from 'organization_name': {col.slice(0, min(3, len(col))).to_pylist()}"
            )

    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    print("\n=== 6. Writing to files (still supported) ===")
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Write to different formats
    parquet_files = result.write_all_parquet(os.path.join(output_dir, "parquet"))
    csv_files = result.write_all_csv(os.path.join(output_dir, "csv"))
    json_files = result.write_all_json(os.path.join(output_dir, "json"))

    print(f"Wrote {len(parquet_files)} Parquet files")
    print(f"Wrote {len(csv_files)} CSV files")
    print(f"Wrote {len(json_files)} JSON files")
    print(f"Output directory: {output_dir}")


if __name__ == "__main__":
    main()
