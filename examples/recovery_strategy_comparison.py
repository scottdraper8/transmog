"""
Recovery Strategy Comparison Example

This example demonstrates the different recovery strategies in Transmog by comparing
their behavior with problematic data. It showcases the benefits of the partial recovery
strategy and the new simplified configuration options.
"""

import os
import sys
import json
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from src package
from transmog import Processor


def print_header(title):
    """Print a section header."""
    print("\n" + "=" * 80)
    print(f" {title} ".center(80, "="))
    print("=" * 80)


def create_problematic_data():
    """Create a dataset with various problematic elements."""
    # Create a dataset with problematic elements that would typically cause
    # strict recovery to fail, but where partial recovery can help
    data = {
        "id": 1,
        "name": "Test Record",
        "valid_field": "This is fine",
        # Problem 1: Invalid number (NaN)
        "metrics": {
            "valid_metric": 42,
            "invalid_metric": float("nan"),
        },
        # Problem 2: Missing/null values
        "details": {
            "description": None,
            "category": "",
        },
        # Problem 3: Array with mixed valid/invalid data
        "items": [
            {"id": 101, "name": "Valid Item", "price": 10.99},
            {"id": 102, "name": None, "price": float("inf")},  # Invalid price
            {"id": 103, "name": "Another Valid", "price": 20.50},
        ],
    }

    return data


def compare_strategies():
    """Compare the behavior of different recovery strategies."""
    print_header("Recovery Strategy Comparison")

    print(
        "This example demonstrates how the different recovery strategies handle problematic data.\n"
    )

    # Create problematic data
    data = create_problematic_data()

    # Create processors with different strategies using the factory methods
    processors = {
        "Strict Recovery": Processor().with_error_handling(
            recovery_strategy="strict",
            allow_malformed_data=False,  # Make sure it fails on invalid data
        ),
        "Skip & Log Recovery": Processor().with_error_handling(
            recovery_strategy="skip", allow_malformed_data=True
        ),
        "Partial Recovery": Processor.with_partial_recovery(),  # Using the new factory method
    }

    # Process data with each strategy
    results = {}

    for name, processor in processors.items():
        print(f"\nTrying with {name}...")
        try:
            result = processor.process(data, entity_name="record")
            results[name] = {
                "success": True,
                "main_table": result.get_main_table(),
                "child_tables": {
                    name: result.get_child_table(name)
                    for name in result.get_table_names()
                    if name != "main"
                },
            }
            print(f"✓ Successful processing with {name}")
        except Exception as e:
            results[name] = {"success": False, "error": str(e)}
            print(f"✗ Failed with error: {str(e)}")

    # Display and compare results
    print_header("Processing Results")

    for name, result in results.items():
        print(f"\n{name}:")
        if result["success"]:
            main_table = result["main_table"]
            print(f"  ✓ Successfully processed {len(main_table)} main records")

            # Check for error indicators in partial recovery
            if name == "Partial Recovery":
                error_markers = []
                if any("_error" in record for record in main_table):
                    error_markers.append("_error")

                if error_markers:
                    print(f"  ℹ Found error markers: {', '.join(error_markers)}")

            # Report on child tables
            if "child_tables" in result:
                child_tables = result["child_tables"]
                if child_tables:
                    print(f"  ✓ Extracted {len(child_tables)} child tables:")
                    for table_name, table_data in child_tables.items():
                        print(f"    - {table_name}: {len(table_data)} records")

                        # For the items table, show how many items have error markers in partial mode
                        if name == "Partial Recovery" and "items" in table_name.lower():
                            error_count = sum(
                                1 for item in table_data if "_error" in item
                            )
                            if error_count > 0:
                                print(f"      ℹ {error_count} items have error markers")
                else:
                    print("  ℹ No child tables extracted")
        else:
            print(f"  ✗ Processing failed: {result['error']}")

    # Show the output from partial recovery
    if results.get("Partial Recovery", {}).get("success", False):
        print_header("Partial Recovery Output")
        print("Here's what the 'Partial Recovery' strategy produced:")

        main_record = results["Partial Recovery"]["main_table"][0]

        # Print a simplified version of the record
        simplified = {
            k: v
            for k, v in main_record.items()
            if not k.startswith("__") and k != "items"
        }
        print("\nMain record (simplified):")
        pprint(simplified)

        # Show error markers separately
        error_markers = {
            k: v
            for k, v in main_record.items()
            if k.startswith("_") and not k.startswith("__")
        }
        if error_markers:
            print("\nError markers:")
            pprint(error_markers)

        # Show items table if it exists
        items_table = results["Partial Recovery"]["child_tables"].get("record_items")
        if items_table:
            print("\nItems table:")
            for i, item in enumerate(items_table):
                # Simplified view of each item
                print(f"\nItem {i + 1}:")
                item_simplified = {
                    k: v
                    for k, v in item.items()
                    if not k.startswith("__") and not k.startswith("_")
                }
                pprint(item_simplified)

                # Show error markers separately
                item_errors = {
                    k: v
                    for k, v in item.items()
                    if k.startswith("_") and not k.startswith("__")
                }
                if item_errors:
                    print("Error markers:")
                    pprint(item_errors)

    print_header("Conclusion")
    print("""
Key observations:
1. Strict recovery fails completely when encountering any error
2. Skip & log recovery may lose entire records, even when only parts are problematic
3. Partial recovery preserves valid data while marking problematic sections
4. The new with_partial_recovery() factory method makes configuration simple

This demonstrates why partial recovery is valuable for:
- Data migration from legacy systems
- Processing API responses with inconsistent structures
- Handling circular references in complex objects
- Recovering data from malformed files
""")


if __name__ == "__main__":
    compare_strategies()
