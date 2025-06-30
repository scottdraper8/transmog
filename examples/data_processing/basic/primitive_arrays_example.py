"""Primitive Arrays Example v1.1.0.

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

    # Process the data using the simple API
    result = tm.flatten(data, name="example")

    # Get all tables
    table_names = list(result.tables.keys())
    print(f"Generated tables: {table_names}")

    # Examine the main table
    main_table = result.main
    print("\nMain table fields:")
    for field in sorted([f for f in main_table[0].keys() if not f.startswith("_")]):
        print(f"  {field}: {main_table[0][field]}")

    # Examine primitive array tables
    for table_name in table_names:
        table = result.tables[table_name]
        print(f"\nTable '{table_name}' ({len(table)} records):")

        # Print sample records
        for i, record in enumerate(table):
            print(f"  Record {i}:")
            for key, value in record.items():
                if key.startswith("_"):
                    continue
                print(f"    {key}: {value}")

            # Only show first 3 records
            if i >= 2:
                remaining = len(table) - 3
                if remaining > 0:
                    print(f"  ... and {remaining} more records")
                break

    print("\n=== Array Handling Options ===")

    # Example 2: Keep arrays inline instead of extracting
    print("\nExample 2: Keep arrays inline")
    result_inline = tm.flatten(data, name="example", arrays="inline")

    print("With arrays='inline', arrays are kept in the main record:")
    main_record = result_inline.main[0]
    for field in ["tags", "scores", "flags"]:
        if field in main_record:
            print(
                f"  {field}: {main_record[field]} (type: {type(main_record[field]).__name__})"
            )

    print(f"Child tables created: {len(result_inline.tables)}")

    # Example 3: Skip arrays entirely
    print("\nExample 3: Skip arrays")
    result_skip = tm.flatten(data, name="example", arrays="skip")

    print("With arrays='skip', arrays are ignored:")
    main_record = result_skip.main[0]
    print("Fields in main table:")
    for field in sorted(main_record.keys()):
        if not field.startswith("_"):
            print(f"  {field}: {main_record[field]}")

    print(f"Child tables created: {len(result_skip.tables)}")

    print("\n=== Null Value Handling ===")

    # Example 4: Include null values
    print("\nExample 4: Include null values")
    result_with_nulls = tm.flatten(data, name="example", skip_null=False)

    mixed_table = result_with_nulls.tables.get("example_mixed", [])
    print(f"Mixed array table with nulls included ({len(mixed_table)} records):")
    for i, record in enumerate(mixed_table):
        value = record.get("value")
        print(f"  Record {i}: {value} (type: {type(value).__name__})")

    print("\n=== Summary ===")
    print("Primitive arrays and object arrays are processed similarly.")
    print("Each item becomes a record in a child table. Primitive values")
    print("are stored in a 'value' field, while object values are stored")
    print("with their original field names.")
    print("\nArray handling options:")
    print("  arrays='separate' (default): Extract to child tables")
    print("  arrays='inline': Keep as JSON in main record")
    print("  arrays='skip': Ignore arrays completely")
    print("\nNull handling:")
    print("  skip_null=True (default): Skip null values")
    print("  skip_null=False: Include null values in output")


if __name__ == "__main__":
    main()
