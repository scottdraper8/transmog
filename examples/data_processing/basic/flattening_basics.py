"""Flattening Basics.

Demonstrates core functionality for flattening nested JSON structures.

Learning Objectives:
- How to flatten nested data with one function call
- How to access flattened tables intuitively
- How to save data in different formats easily
"""

import transmog as tm
from pprint import pprint


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

    # Example 1: Basic flattening - ONE LINE!
    print("=== Basic Flattening ===")
    result = tm.flatten(data, name="company")

    # Display the result
    print(f"\nCreated {len(result.all_tables)} tables:")
    print(result)

    # Access main table easily
    print("\nMain Table (first record):")
    pprint(result.main[0])

    # Access child tables intuitively
    print("\nContacts Table:")
    for contact in result.tables["company_contacts"]:
        print(f"  - {contact['name']} ({contact['type']})")

    # Example 2: Custom options
    print("\n\n=== Custom Options ===")

    # Use dot notation and preserve types
    result = tm.flatten(data, name="company", separator=".", preserve_types=True)

    print("Fields with dot notation:")
    for key in list(result.main[0].keys())[:5]:
        print(f"  - {key}")

    # Example 3: Saving is simple
    print("\n\n=== Saving Data ===")

    # Save to JSON
    files = result.save("output/company.json")
    print(f"Saved to JSON: {len(files)} files")

    # Save to CSV
    files = result.save("output/company.csv")
    print(f"Saved to CSV: {len(files)} files")

    # Save to Parquet (if available)
    try:
        files = result.save("output/company.parquet")
        print(f"Saved to Parquet: {len(files)} files")
    except ImportError:
        print("Parquet requires pyarrow: pip install pyarrow")

    # Example 4: Working with arrays
    print("\n\n=== Array Handling ===")

    # Keep arrays inline instead of separate tables
    result_inline = tm.flatten(data, name="company", arrays="inline")
    print(f"\nWith arrays='inline': {len(result_inline.tables)} child tables")

    # Skip arrays entirely
    result_skip = tm.flatten(data, name="company", arrays="skip")
    print(f"With arrays='skip': {len(result_skip.tables)} child tables")

    # Example 5: Performance options
    print("\n\n=== Performance Options ===")

    # Low memory mode for large datasets
    result = tm.flatten(data, name="company", low_memory=True)
    print(f"Low memory mode: {len(result.main)} records processed")

    # Custom batch size
    result = tm.flatten(data, name="company", batch_size=5000)
    print(f"Custom batch size: {len(result.main)} records processed")


if __name__ == "__main__":
    main()
