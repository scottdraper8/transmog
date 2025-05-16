"""Example Name: JSON Processing.

Demonstrates: Processing JSON data with Transmog.

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/processing/json-handling.html
- https://transmog.readthedocs.io/en/latest/api/process.html

Learning Objectives:
- How to process nested JSON structures
- How to handle arrays and objects within JSON
- How to configure JSON processing behavior
- How to output processed data in different formats
"""

import json
import os

# Import from transmog package
import transmog as tm


def load_sample_json(filepath=None):
    """Load sample JSON data from file or generate if not available."""
    if filepath and os.path.exists(filepath):
        with open(filepath) as f:
            return json.load(f)

    # Generate sample data if file not provided or not found
    return {
        "organization": {
            "id": 1001,
            "name": "Example Corp",
            "founded": 1995,
            "active": True,
            "address": {
                "street": "123 Business Ave",
                "city": "Enterprise",
                "state": "CA",
                "zip": "94000",
                "coordinates": {"latitude": 37.7749, "longitude": -122.4194},
            },
            "contact": {
                "email": "info@example.com",
                "phone": "555-1234",
                "website": "https://example.com",
            },
            "departments": [
                {
                    "id": 101,
                    "name": "Engineering",
                    "head_count": 50,
                    "teams": [
                        {"name": "Frontend", "members": 15},
                        {"name": "Backend", "members": 20},
                        {"name": "DevOps", "members": 10},
                        {"name": "QA", "members": 5},
                    ],
                },
                {
                    "id": 102,
                    "name": "Marketing",
                    "head_count": 25,
                    "teams": [
                        {"name": "Digital", "members": 10},
                        {"name": "Content", "members": 8},
                        {"name": "Analytics", "members": 7},
                    ],
                },
                {
                    "id": 103,
                    "name": "Finance",
                    "head_count": 15,
                    "teams": [
                        {"name": "Accounting", "members": 8},
                        {"name": "Investment", "members": 7},
                    ],
                },
            ],
            "products": [
                {
                    "id": "P001",
                    "name": "Product A",
                    "category": "Software",
                    "price": 99.99,
                    "features": ["Feature 1", "Feature 2", "Feature 3"],
                    "versions": [
                        {"version": "1.0", "release_date": "2020-01-15"},
                        {"version": "1.1", "release_date": "2020-06-30"},
                        {"version": "2.0", "release_date": "2021-03-10"},
                    ],
                },
                {
                    "id": "P002",
                    "name": "Product B",
                    "category": "Hardware",
                    "price": 299.99,
                    "features": ["Feature A", "Feature B"],
                    "versions": [
                        {"version": "MK1", "release_date": "2019-05-20"},
                        {"version": "MK2", "release_date": "2021-02-15"},
                    ],
                },
            ],
        }
    }


def main():
    """Run the JSON processing example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "json_processing"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Load or generate sample JSON data
    data = load_sample_json()

    print("=== JSON Processing Example ===")

    # Example 1: Basic JSON Processing
    print("\n=== Basic JSON Processing ===")

    # Create a processor with default configuration
    processor = tm.Processor()

    # Process the JSON data
    result = processor.process(data=data, entity_name="company")

    # Print the main table
    print("\nMain Table:")
    main_table = result.get_main_table()
    if main_table:
        # Print the first record, limited to a few fields
        record = main_table[0]
        for key in [
            "__extract_id",
            "organization_id",
            "organization_name",
            "organization_founded",
        ]:
            if key in record:
                print(f"{key}: {record[key]}")

    # List all generated tables
    print("\nGenerated Tables:")
    for table_name in sorted(result.get_table_names()):
        formatted_name = result.get_formatted_table_name(table_name)
        table_data = result.get_child_table(table_name)
        print(f"- {formatted_name} ({len(table_data)} records)")

    # Example 2: Custom JSON Processing Configuration
    print("\n=== Custom JSON Processing Configuration ===")

    # Create a processor with custom configuration for JSON
    config = (
        tm.TransmogConfig.default()
        .with_naming(
            separator=".",  # Use dot notation for nested fields
            abbreviate_table_names=False,  # Don't abbreviate table names
        )
        .with_processing(
            cast_to_string=False,  # Keep original data types
            skip_null=False,  # Include null values
            include_empty=True,  # Include empty values
        )
    )

    processor = tm.Processor(config=config)

    # Process the JSON data with custom configuration
    result = processor.process(data=data, entity_name="company")

    # Print sample of the main table with custom configuration
    print("\nMain Table with Custom Configuration:")
    main_table = result.get_main_table()
    if main_table:
        # Print the first record, limited to a few fields to show differences
        record = main_table[0]
        for key in [
            "__extract_id",
            "organization.id",
            "organization.name",
            "organization.address.city",
        ]:
            if key in record:
                print(f"{key}: {record[key]}")

    # Example 3: Working with Arrays in JSON
    print("\n=== Working with Arrays in JSON ===")

    # Access child tables containing array data
    departments_table = result.get_child_table("company_organization_departments")
    products_table = result.get_child_table("company_organization_products")

    # Print information about array tables
    if departments_table:
        print(f"\nDepartments Table ({len(departments_table)} records):")
        print(
            f"First department: id={departments_table[0].get('id', 'N/A')}, "
            f"name={departments_table[0].get('name', 'N/A')}"
        )

    if products_table:
        print(f"\nProducts Table ({len(products_table)} records):")
        print(
            f"First product: id={products_table[0].get('id', 'N/A')}, "
            f"name={products_table[0].get('name', 'N/A')}"
        )

    # Find nested array tables (teams within departments)
    teams_table = result.get_child_table("company_organization_departments_teams")
    if teams_table:
        print(f"\nTeams Table ({len(teams_table)} records):")
        print(
            f"Sample team: name={teams_table[0].get('name', 'N/A')}, "
            f"members={teams_table[0].get('members', 'N/A')}"
        )

    # Example 4: Output Processed JSON to Different Formats
    print("\n=== Output Processed JSON to Different Formats ===")

    # Write to JSON files
    json_dir = os.path.join(output_dir, "json")
    json_files = result.write_all_json(base_path=json_dir, indent=2)
    print(f"Wrote {len(json_files)} JSON files to {json_dir}")

    # Write to CSV files
    csv_dir = os.path.join(output_dir, "csv")
    csv_files = result.write_all_csv(base_path=csv_dir)
    print(f"Wrote {len(csv_files)} CSV files to {csv_dir}")

    # Try to write to Parquet if available
    try:
        parquet_dir = os.path.join(output_dir, "parquet")
        parquet_files = result.write_all_parquet(base_path=parquet_dir)
        print(f"Wrote {len(parquet_files)} Parquet files to {parquet_dir}")
    except ImportError:
        print("PyArrow not available. Parquet output skipped.")

    print(f"\nAll output files written to: {output_dir}")


if __name__ == "__main__":
    main()
