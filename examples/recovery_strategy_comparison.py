"""Recovery Strategy Comparison Example.

This example demonstrates the different recovery strategies in Transmog by comparing
their behavior with problematic data. It showcases the benefits of the partial recovery
strategy and the new simplified configuration options.
"""

import os
import sys
from pprint import pprint

# Add parent directory to path to import transmog without installing
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog package
from transmog import Processor
from transmog.error import LENIENT, SKIP, STRICT


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
        "This example demonstrates how the different recovery strategies handle "
        "problematic data.\n"
    )

    # Create problematic data
    data = create_problematic_data()

    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "output", "recovery_strategies"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Create processors with different strategies
    processors = {
        "Strict Recovery": Processor.with_error_recovery(
            STRICT,  # Uses the exported constant
            allow_malformed_data=False,  # Make sure it fails on invalid data
        ),
        "Skip & Log Recovery": Processor.with_error_recovery(
            SKIP,  # Uses the exported constant
            allow_malformed_data=True,
        ),
        "Partial Recovery": Processor.with_error_recovery(
            LENIENT  # Uses the exported constant for partial (lenient) recovery
        ),
    }

    # Process data with each strategy
    results = {}

    for name, processor in processors.items():
        print(f"\nTrying with {name}...")
        try:
            result = processor.process(data, entity_name="record")

            # Save the result to JSON for inspection
            output_file = os.path.join(
                output_dir, f"{name.lower().replace(' ', '_')}.json"
            )
            result.write_all_json(output_file)

            results[name] = {
                "success": True,
                "main_table": result.get_main_table(),
                "child_tables": {
                    name: result.get_child_table(name)
                    for name in result.get_table_names()
                    if name != "main"
                },
                "output_file": output_file,
            }
            print(f"✓ Successful processing with {name}")
            print(f"  Output saved to: {output_file}")
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
                for record in main_table:
                    for key in record:
                        if key.startswith("_error_"):
                            error_markers.append(key)
                            break

                if error_markers:
                    print("  ℹ Found error markers in records")

            # Report on child tables
            if "child_tables" in result:
                child_tables = result["child_tables"]
                if child_tables:
                    print(f"  ✓ Extracted {len(child_tables)} child tables:")
                    for table_name, table_data in child_tables.items():
                        print(f"    - {table_name}: {len(table_data)} records")

                        # For the items table, show how many items have error markers
                        # in partial mode
                        if name == "Partial Recovery" and "items" in table_name.lower():
                            error_count = sum(
                                1
                                for item in table_data
                                if any(k.startswith("_error_") for k in item)
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
            if not k.startswith("__") and k != "items" and not k.startswith("_error_")
        }
        print("\nMain record (simplified):")
        pprint(simplified)

        # Show error markers separately
        error_markers = {
            k: v for k, v in main_record.items() if k.startswith("_error_")
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
                    if not k.startswith("__") and not k.startswith("_error_")
                }
                pprint(item_simplified)

                # Show error markers separately
                item_errors = {k: v for k, v in item.items() if k.startswith("_error_")}
                if item_errors:
                    print("Error markers:")
                    pprint(item_errors)

    print_header("Conclusion")
    print("""
Key observations:
1. Strict recovery (STRICT) fails completely when encountering any error
2. Skip & log recovery (SKIP) may lose entire records, even when only parts are
    problematic
3. Partial recovery (LENIENT) preserves valid data while marking problematic sections

This demonstrates why partial recovery is valuable for:
- Data migration from legacy systems
- Processing API responses with inconsistent structures
- Social media or user-generated content analysis
- Exploratory data analysis where you want to see all valid data

Usage in your code:
```python
from transmog import Processor
from transmog.error import STRICT, LENIENT, SKIP

# Default - strict recovery (fails on errors)
processor = Processor.default()

# Skip & log recovery (skips problematic records)
processor = Processor.with_error_recovery(SKIP)

# Partial recovery (preserves valid portions)
processor = Processor.with_error_recovery(LENIENT)
```
""")

    print(f"\nAll results written to: {output_dir}")


def main():
    """Run the example."""
    compare_strategies()


if __name__ == "__main__":
    main()
