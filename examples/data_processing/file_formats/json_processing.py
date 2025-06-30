"""JSON Processing Example.

Demonstrates working with JSON data using Transmog.

Learning Objectives:
- Processing nested JSON structures
- Handling arrays and nested objects
- Customizing flattening behavior
- Outputting to different formats
"""

import json
import os

import transmog as tm


def load_sample_json():
    """Load or create sample JSON data."""
    sample_data = {
        "organization": {
            "id": "org-001",
            "name": "TechCorp Industries",
            "founded": 2010,
            "active": True,
            "address": {
                "street": "123 Tech Boulevard",
                "city": "San Francisco",
                "state": "CA",
                "zip": "94105",
                "country": "USA",
            },
            "departments": [
                {
                    "id": "dept-001",
                    "name": "Engineering",
                    "budget": 5000000,
                    "head": "Jane Smith",
                    "teams": [
                        {"name": "Frontend", "lead": "Alice Johnson", "members": 12},
                        {"name": "Backend", "lead": "Bob Wilson", "members": 15},
                        {"name": "DevOps", "lead": "Charlie Brown", "members": 8},
                    ],
                },
                {
                    "id": "dept-002",
                    "name": "Marketing",
                    "budget": 2000000,
                    "head": "David Lee",
                    "teams": [
                        {
                            "name": "Digital Marketing",
                            "lead": "Eve Martinez",
                            "members": 6,
                        },
                        {"name": "Content", "lead": "Frank Garcia", "members": 4},
                    ],
                },
                {
                    "id": "dept-003",
                    "name": "Sales",
                    "budget": 3000000,
                    "head": "Grace Kim",
                    "teams": [],
                },
            ],
            "products": [
                {
                    "id": "prod-001",
                    "name": "CloudSync Pro",
                    "category": "SaaS",
                    "price": 99.99,
                    "features": [
                        "Real-time sync",
                        "End-to-end encryption",
                        "Multi-platform",
                    ],
                    "versions": [
                        {"version": "1.0", "release_date": "2020-01-15"},
                        {"version": "2.0", "release_date": "2021-06-20"},
                        {"version": "3.0", "release_date": "2023-03-10"},
                    ],
                },
                {
                    "id": "prod-002",
                    "name": "DataAnalyzer",
                    "category": "Analytics",
                    "price": 199.99,
                    "features": ["ML-powered insights", "Custom dashboards"],
                    "versions": [{"version": "1.0", "release_date": "2021-09-01"}],
                },
            ],
        }
    }

    return sample_data


def main():
    """Run the JSON processing example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "json_processing"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Load sample JSON data
    data = load_sample_json()

    print("=== JSON Processing Example ===")

    # Example 1: Basic JSON Processing
    print("\n=== Basic JSON Processing ===")

    # Process JSON data - it's this simple!
    result = tm.flatten(data, name="company")

    # Display overview
    print(f"\nFlattened into {len(result.all_tables)} tables:")
    print(result)

    # Access main table
    print("\nMain table sample:")
    if result.main:
        record = result.main[0]
        print(f"  Organization ID: {record.get('organization_id')}")
        print(f"  Name: {record.get('organization_name')}")
        print(f"  Founded: {record.get('organization_founded')}")
        print(f"  City: {record.get('organization_address_city')}")

    # Example 2: Custom Separator (Dot Notation)
    print("\n\n=== Custom Separator (Dot Notation) ===")

    # Use dot notation for nested fields
    result = tm.flatten(data, name="company", separator=".")

    # Show field names with dots
    print("\nField names with dot notation:")
    for key in list(result.main[0].keys())[:8]:
        print(f"  {key}")

    # Example 3: Preserving Data Types
    print("\n\n=== Preserving Data Types ===")

    # Keep original data types instead of converting to strings
    result = tm.flatten(data, name="company", preserve_types=True)

    # Check data types
    if result.main:
        record = result.main[0]
        print("\nData types preserved:")
        print(f"  founded (year): {type(record.get('organization_founded'))}")
        print(f"  active (bool): {type(record.get('organization_active'))}")
        print(f"  name (str): {type(record.get('organization_name'))}")

    # Example 4: Working with Arrays
    print("\n\n=== Working with Arrays ===")

    # Default behavior - arrays become separate tables
    result = tm.flatten(data, name="company")

    # Access child tables
    print("\nDepartments table:")
    departments = result.tables.get("company_organization_departments", [])
    for dept in departments:
        print(f"  - {dept.get('name')} (Budget: ${dept.get('budget'):,})")

    print("\nProducts table:")
    products = result.tables.get("company_organization_products", [])
    for prod in products:
        print(f"  - {prod.get('name')} (${prod.get('price')})")

    # Find nested arrays (teams within departments)
    print("\nTeams table (nested array):")
    teams = result.tables.get("company_organization_departments_teams", [])
    for team in teams[:3]:  # Show first 3
        print(f"  - {team.get('name')} team ({team.get('members')} members)")

    # Example 5: Array Handling Options
    print("\n\n=== Array Handling Options ===")

    # Keep arrays inline
    result_inline = tm.flatten(data, name="company", arrays="inline")
    print(f"\nWith arrays='inline': {len(result_inline.tables)} child tables")

    # Skip arrays entirely
    result_skip = tm.flatten(data, name="company", arrays="skip")
    print(f"With arrays='skip': {len(result_skip.tables)} child tables")

    # Example 6: Saving to Different Formats
    print("\n\n=== Saving to Different Formats ===")

    # Save to JSON
    json_path = os.path.join(output_dir, "company.json")
    files = result.save(json_path)
    print(f"\nSaved to JSON: {len(files)} files")

    # Save to CSV
    csv_path = os.path.join(output_dir, "company.csv")
    files = result.save(csv_path)
    print(f"Saved to CSV: {len(files)} files")

    # Save to Parquet (if available)
    try:
        parquet_path = os.path.join(output_dir, "company.parquet")
        files = result.save(parquet_path)
        print(f"Saved to Parquet: {len(files)} files")
    except ImportError:
        print("Parquet requires pyarrow: pip install pyarrow")

    # Example 7: Custom ID Fields
    print("\n\n=== Custom ID Fields ===")

    # Use existing 'id' fields instead of generating synthetic ones
    result = tm.flatten(data, name="company", id_field="id")

    # Check if natural IDs are used
    dept = result.tables.get("company_organization_departments", [])[0]
    print(f"\nDepartment ID field: {dept.get('id', 'Not found')}")
    print(f"Has generated _id field: {'_id' in dept}")

    # Example 8: Deep Nesting Control
    print("\n\n=== Deep Nesting Control ===")

    # Create deeply nested data
    deep_data = {
        "level1": {
            "level2": {"level3": {"level4": {"level5": {"value": "Very deep!"}}}}
        }
    }

    # Default threshold
    result_default = tm.flatten(deep_data, name="deep")
    print(f"\nDefault nested_threshold (4):")
    print(f"  Field name: {list(result_default.main[0].keys())}")

    # Higher threshold
    result_high = tm.flatten(deep_data, name="deep", nested_threshold=10)
    print(f"\nWith nested_threshold=10:")
    print(f"  Field name: {list(result_high.main[0].keys())}")

    print(f"\n\nAll output files saved to: {output_dir}")


if __name__ == "__main__":
    main()
