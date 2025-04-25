"""
Simple example demonstrating Transmog functionality.

This example shows how to flatten a nested JSON structure,
extract child arrays, and save them to Parquet files.
"""

import json
import os
import sys
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
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

    # Initialize processor
    processor = Processor(cast_to_string=True, include_empty=False)

    # Process the data
    result = processor.process(data=data, entity_name="company")

    # Print main table
    print("\n=== Main Table ===")
    pprint(result.get_main_table())

    # Print all child tables
    print("\n=== Child Tables ===")
    for table_path in result.get_table_names():
        formatted_name = result.get_formatted_table_name(table_path)
        table_data = result.get_child_table(table_path)
        print(f"\n-- {formatted_name} ({table_path}) --")
        if table_data:
            pprint(table_data[0])  # Print just the first record
            print(f"...and {len(table_data) - 1} more records")

    # Write to Parquet
    print("\n=== Writing to Parquet ===")
    outputs = result.write_all_parquet(base_path=output_dir, compression="snappy")

    for table_name, file_path in outputs.items():
        print(f"Wrote {table_name} to {file_path}")


if __name__ == "__main__":
    main()
