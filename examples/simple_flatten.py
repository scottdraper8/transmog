"""Simple example demonstrating Transmog functionality.

This example shows how to flatten a nested JSON structure,
extract child arrays, and save them to various output formats.
"""

import os
import sys
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor


def main():
    """Run the example."""
    # Sample nested JSON data
    data = {
        "id": 123,
        "name": "Example Company",
        "address": {
            "street": "123 Main St",
            "city": "Anytown",
            "state": "CA",
            "zip": "12345",
        },
        "contacts": [
            {
                "type": "primary",
                "name": "John Doe",
                "phone": "555-1234",
                "details": {"department": "Sales", "position": "Manager"},
            },
            {
                "type": "secondary",
                "name": "Jane Smith",
                "phone": "555-5678",
                "details": {"department": "Support", "position": "Director"},
            },
        ],
        "locations": [
            {
                "name": "Headquarters",
                "address": {
                    "street": "456 Corp Ave",
                    "city": "Metropolis",
                    "state": "NY",
                },
                "departments": [
                    {"name": "Executive", "staff_count": 10},
                    {"name": "Finance", "staff_count": 15},
                ],
            },
            {
                "name": "Branch Office",
                "address": {
                    "street": "789 Branch Rd",
                    "city": "Smallville",
                    "state": "KS",
                },
                "departments": [
                    {"name": "Sales", "staff_count": 20},
                    {"name": "Support", "staff_count": 25},
                ],
            },
        ],
    }

    # Create output directory
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Basic usage with default configuration
    print("\n=== Example 1: Basic Usage ===")
    processor = Processor()
    result = processor.process(data=data, entity_name="company")

    # Print main table
    print("\nMain Table:")
    pprint(result.get_main_table())

    # Print all child tables
    print("\nChild Tables:")
    for table_name in result.get_table_names():
        formatted_name = result.get_formatted_table_name(table_name)
        table_data = result.get_child_table(table_name)
        print(f"\n-- {formatted_name} ({table_name}) --")
        if table_data:
            pprint(table_data[0])  # Print just the first record
            print(f"...and {len(table_data) - 1} more records")

    # Example 2: Memory-optimized processing
    print("\n=== Example 2: Memory-Optimized Processing ===")
    processor = Processor.memory_optimized()
    result = processor.process(data=data, entity_name="company")
    print(
        f"Processed with memory optimization - created {
            len(result.get_table_names()) + 1
        } tables"
    )

    # Example 3: Performance-optimized processing
    print("\n=== Example 3: Performance-Optimized Processing ===")
    processor = Processor.performance_optimized()
    result = processor.process(data=data, entity_name="company")
    print(
        f"Processed with performance optimization - created {
            len(result.get_table_names()) + 1
        } tables"
    )

    # Example 4: Custom configuration
    print("\n=== Example 4: Custom Configuration ===")
    processor = (
        Processor()
        .with_naming(separator=".", abbreviate_table_names=False)
        .with_processing(cast_to_string=False, skip_null=False)
        .with_metadata(id_field="record_id", parent_field="parent_id")
    )
    result = processor.process(data=data, entity_name="company")
    print("Processed with custom configuration")

    # Show some output from custom configuration
    print("\nCustom Configuration Output:")
    if result.get_main_table():
        record = result.get_main_table()[0]
        # Print a few fields to demonstrate changes
        for field in ["record_id", "id", "name", "address.city"]:
            if field in record:
                print(f"{field}: {record[field]}")

    # Example 5: Deterministic IDs
    print("\n=== Example 5: Deterministic IDs ===")
    processor = Processor.with_deterministic_ids(
        {
            "": "id",  # Root level uses "id" field
            "company_contacts": "name",  # Contacts use "name" field
            "company_locations": "name",  # Locations use "name" field
            "company_locations_departments": "name",  # Departments use "name" field
        }
    )
    result = processor.process(data=data, entity_name="company")
    print("Processed with deterministic IDs")

    # Demonstrate multiple output formats
    print("\n=== Example 6: Multiple Output Formats ===")

    # Get as Python dictionaries
    dict_output = result.to_dict()
    print(f"Dictionary output has {len(dict_output)} tables")

    # Get as JSON-serializable objects
    json_objects = result.to_json_objects()
    print(f"JSON objects output has {len(json_objects)} tables")

    # Get as bytes for direct writing
    json_bytes = result.to_json_bytes(indent=2)
    csv_bytes = result.to_csv_bytes()

    print(f"JSON bytes size: {sum(len(v) for v in json_bytes.values())} bytes")
    print(f"CSV bytes size: {sum(len(v) for v in csv_bytes.values())} bytes")

    try:
        # Try to get PyArrow tables if available
        pa_tables = result.to_pyarrow_tables()
        print(f"PyArrow tables created successfully: {len(pa_tables)} tables")

        # Try to get Parquet bytes if available
        parquet_bytes = result.to_parquet_bytes(compression="snappy")
        print(
            f"Parquet bytes size: {sum(len(v) for v in parquet_bytes.values())} bytes"
        )
    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    # Write to multiple formats
    print("\n=== Writing to Files ===")

    # Write to JSON
    json_outputs = result.write_all_json(base_path=os.path.join(output_dir, "json"))
    print(f"Wrote {len(json_outputs)} JSON files")

    # Write to CSV
    csv_outputs = result.write_all_csv(base_path=os.path.join(output_dir, "csv"))
    print(f"Wrote {len(csv_outputs)} CSV files")

    # Try to write to Parquet
    try:
        parquet_outputs = result.write_all_parquet(
            base_path=os.path.join(output_dir, "parquet"), compression="snappy"
        )
        print(f"Wrote {len(parquet_outputs)} Parquet files")
    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    print(f"\nOutput files written to: {output_dir}")


if __name__ == "__main__":
    main()
