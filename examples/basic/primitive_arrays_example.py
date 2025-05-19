"""Primitive Arrays Example.

This example demonstrates the handling of primitive arrays in Transmog.
Arrays of primitive values (strings, numbers, booleans) are
extracted and processed as child tables.
"""

import os
import sys

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import transmog as tm


def main():
    """Run the example demonstrating primitive array handling."""
    # Create a test structure with primitive arrays
    data = {
        "id": 123,
        "name": "Example Data",
        "tags": ["python", "data", "json"],
        "scores": [95, 87, 92, 78],
        "flags": [True, False, True],
        "mixed": [
            100,
            "string",
            True,
            None,
        ],  # Contains a null value that is skipped by default
        "nested": {"categories": ["A", "B", "C"]},
    }

    print("\n=== Processing Data with Primitive Arrays ===")
    # Process the data
    processor = tm.Processor()
    result = processor.process(data, entity_name="example")

    # Get all tables
    table_names = result.get_table_names()
    print(f"Generated tables: {table_names}")

    # Examine the main table
    main_table = result.get_main_table()
    print("\nMain table fields:")
    for field in sorted([f for f in main_table[0].keys() if not f.startswith("__")]):
        print(f"  {field}: {main_table[0][field]}")

    # Examine primitive array tables
    for table_name in table_names:
        table = result.get_child_table(table_name)
        print(f"\nTable '{table_name}' ({len(table)} records):")

        # Print sample records
        for i, record in enumerate(table):
            print(f"  Record {i}:")
            for key, value in record.items():
                if key.startswith("__"):
                    continue
                print(f"    {key}: {value}")

            # Only show first 3 records
            if i >= 2:
                remaining = len(table) - 3
                if remaining > 0:
                    print(f"  ... and {remaining} more records")
                break

    print("\n=== Primitive Arrays and Object Arrays ===")
    print("Primitive arrays and object arrays are processed similarly.")
    print("Each item becomes a record in a child table. Primitive values")
    print("are stored in a 'value' field, while object values are stored")
    print("with their original field names.")

    print("\n=== Null Value Handling ===")
    print("Null values in arrays are skipped by default.")
    print("The 'mixed' array contains 4 elements but only 3 are processed.")
    print("The skip_null parameter controls this behavior:")
    print("  processor.process(data, skip_null=False)")


if __name__ == "__main__":
    main()
