"""Example Name: Flattening Basics.

Demonstrates: Core functionality for flattening nested JSON structures

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/essentials/basic-concepts.html
- https://transmog.readthedocs.io/en/latest/user/processing/data-transformation.html

Learning Objectives:
- How to flatten a nested JSON structure
- How to extract arrays into separate tables
- How to access main and child tables
- How to output data in different formats
"""

import os
from pprint import pprint

# Import from transmog package
import transmog as tm


def main():
    """Run the flattening basics example."""
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
    output_dir = os.path.join(os.path.dirname(__file__), "..", "..", "output")
    os.makedirs(output_dir, exist_ok=True)

    # Example 1: Basic usage with default configuration
    print("\n=== Basic Flattening ===")
    processor = tm.Processor()
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

    # Example 2: Different processing modes
    print("\n=== Different Processing Modes ===")

    # Memory-optimized processing
    processor = tm.Processor.memory_optimized()
    memory_result = processor.process(data=data, entity_name="company")
    print(
        f"Memory-optimized processing created "
        f"{len(memory_result.get_table_names()) + 1} tables"
    )

    # Performance-optimized processing
    processor = tm.Processor.performance_optimized()
    perf_result = processor.process(data=data, entity_name="company")
    print(
        f"Performance-optimized processing created "
        f"{len(perf_result.get_table_names()) + 1} tables"
    )

    # Example 3: Output formats
    print("\n=== Output Formats ===")

    # To Python dictionaries
    dict_output = result.to_dict()
    print(f"Dictionary output has {len(dict_output)} tables")

    # To JSON and CSV bytes
    json_bytes = result.to_json_bytes(indent=2)
    csv_bytes = result.to_csv_bytes()
    print(f"JSON bytes size: {sum(len(v) for v in json_bytes.values())} bytes")
    print(f"CSV bytes size: {sum(len(v) for v in csv_bytes.values())} bytes")

    # Try PyArrow and Parquet if available
    try:
        # PyArrow tables
        pa_tables = result.to_pyarrow_tables()
        print(f"PyArrow tables created: {len(pa_tables)} tables")

        # Parquet bytes
        parquet_bytes = result.to_parquet_bytes(compression="snappy")
        print(
            f"Parquet bytes size: {sum(len(v) for v in parquet_bytes.values())} bytes"
        )
    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")

    # Write to files
    print("\n=== Writing to Files ===")

    # Write to JSON
    json_dir = os.path.join(output_dir, "json")
    json_outputs = result.write_all_json(base_path=json_dir)
    print(f"Wrote {len(json_outputs)} JSON files to {json_dir}")

    # Write to CSV
    csv_dir = os.path.join(output_dir, "csv")
    csv_outputs = result.write_all_csv(base_path=csv_dir)
    print(f"Wrote {len(csv_outputs)} CSV files to {csv_dir}")

    # Try to write to Parquet
    try:
        parquet_dir = os.path.join(output_dir, "parquet")
        parquet_outputs = result.write_all_parquet(base_path=parquet_dir)
        print(f"Wrote {len(parquet_outputs)} Parquet files to {parquet_dir}")
    except ImportError:
        print("PyArrow not available. Install with: pip install pyarrow")


if __name__ == "__main__":
    main()
