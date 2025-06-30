"""Configuration Options.

Demonstrates different ways to configure the Transmog flattening process.

Learning Objectives:
- How to use configuration options
- Understanding different array handling modes
- Customizing ID and naming behavior
- Error handling options
"""

import transmog as tm


def main():
    """Run the configuration example."""
    # Sample data for testing configurations
    data = {
        "id": 123,
        "name": "Example Entity",
        "details": {
            "category": "Test",
            "active": True,
            "score": 92.5,
            "tags": ["tag1", "tag2", "tag3"],
        },
        "items": [
            {"id": "i1", "name": "Item 1", "price": 10.99},
            {"id": "i2", "name": "Item 2", "price": 20.99},
        ],
    }

    # Example 1: Default configuration
    print("\n=== Default Configuration ===")
    result = tm.flatten(data, name="entity")
    print_result_overview(result)

    # Example 2: Custom Separators
    print("\n\n=== Custom Separators ===")

    # Dot notation
    result = tm.flatten(data, name="entity", separator=".")
    print("\nDot notation fields:")
    for key in list(result.main[0].keys())[:5]:
        print(f"  - {key}")

    # Custom separator
    result = tm.flatten(data, name="entity", separator="__")
    print("\nDouble underscore fields:")
    for key in list(result.main[0].keys())[:5]:
        print(f"  - {key}")

    # Example 3: ID Configuration
    print("\n\n=== ID Configuration ===")

    # Use existing field as ID
    result = tm.flatten(data, name="entity", id_field="id")
    print("\nUsing existing 'id' field:")
    print(f"Main record has '_id' field: {'_id' in result.main[0]}")
    print(f"Main record has 'id' field: {'id' in result.main[0]}")

    # Custom parent ID field name
    result = tm.flatten(data, name="entity", parent_id_field="_parent")
    if result.tables:
        first_child_table = list(result.tables.values())[0]
        print(
            f"\nParent field name: {[k for k in first_child_table[0] if 'parent' in k]}"
        )

    # Example 4: Array Handling Modes
    print("\n\n=== Array Handling Modes ===")

    # Default: separate tables
    result_separate = tm.flatten(data, name="entity", arrays="separate")
    print(f"\narrays='separate' (default): {len(result_separate.all_tables)} tables")

    # Keep arrays inline
    result_inline = tm.flatten(data, name="entity", arrays="inline")
    print(f"arrays='inline': {len(result_inline.all_tables)} tables")
    if "details_tags" in result_inline.main[0]:
        print(f"  Tags field in main: {result_inline.main[0]['details_tags']}")

    # Skip arrays entirely
    result_skip = tm.flatten(data, name="entity", arrays="skip")
    print(f"arrays='skip': {len(result_skip.all_tables)} tables")

    # Example 5: Data Type Handling
    print("\n\n=== Data Type Handling ===")

    # Default: convert to strings
    result_string = tm.flatten(data, name="entity")
    print("\nDefault (cast to string):")
    print(f"  Score type: {type(result_string.main[0]['details_score'])}")
    print(f"  Active type: {type(result_string.main[0]['details_active'])}")

    # Preserve original types
    result_types = tm.flatten(data, name="entity", preserve_types=True)
    print("\nWith preserve_types=True:")
    print(f"  Score type: {type(result_types.main[0]['details_score'])}")
    print(f"  Active type: {type(result_types.main[0]['details_active'])}")

    # Example 6: Null and Empty Handling
    print("\n\n=== Null and Empty Handling ===")

    data_with_nulls = {
        "id": 1,
        "name": "Test",
        "empty_string": "",
        "null_value": None,
        "empty_list": [],
        "empty_dict": {},
    }

    # Default behavior
    result_default = tm.flatten(data_with_nulls, name="test")
    print("\nDefault (skip_null=True, skip_empty=True):")
    print(f"Fields in result: {list(result_default.main[0].keys())}")

    # Include nulls and empties
    result_all = tm.flatten(
        data_with_nulls, name="test", skip_null=False, skip_empty=False
    )
    print("\nWith skip_null=False, skip_empty=False:")
    print(f"Fields in result: {list(result_all.main[0].keys())}")

    # Example 7: Error Handling
    print("\n\n=== Error Handling ===")

    problematic_data = [
        {"id": 1, "name": "Good record"},
        {"id": "invalid", "bad_field": float("inf")},  # This might cause issues
        {"id": 3, "name": "Another good record"},
    ]

    # Skip errors
    result = tm.flatten(problematic_data, name="records", errors="skip")
    print(f"\nerrors='skip': Processed {len(result.main)} records")

    # Warn about errors (would log warnings)
    result = tm.flatten(problematic_data, name="records", errors="warn")
    print(f"errors='warn': Processed {len(result.main)} records")

    # Example 8: Performance Options
    print("\n\n=== Performance Options ===")

    # Low memory mode
    result = tm.flatten(data, name="entity", low_memory=True)
    print("\nLow memory mode enabled")

    # Custom batch size
    result = tm.flatten(data, name="entity", batch_size=5000)
    print("Custom batch size set to 5000")

    # Example 9: Nested Depth Control
    print("\n\n=== Nested Depth Control ===")

    deeply_nested = {
        "level1": {"level2": {"level3": {"level4": {"level5": {"value": "deep"}}}}}
    }

    # Default threshold
    result = tm.flatten(deeply_nested, name="nested")
    print(f"\nDefault nested_threshold: {list(result.main[0].keys())}")

    # Higher threshold
    result = tm.flatten(deeply_nested, name="nested", nested_threshold=10)
    print(f"With nested_threshold=10: {list(result.main[0].keys())}")


def print_result_overview(result):
    """Print overview of flattening result."""
    print(f"Tables created: {len(result.all_tables)}")
    print(f"Main table records: {len(result.main)}")
    if result.tables:
        print(f"Child tables: {list(result.tables.keys())}")


if __name__ == "__main__":
    main()
