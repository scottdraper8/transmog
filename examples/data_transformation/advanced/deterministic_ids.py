"""Example Name: Deterministic IDs.

Demonstrates: Using deterministic IDs for consistent record identification

Related Documentation:
- https://transmog.readthedocs.io/en/latest/user/advanced/deterministic-ids.html
- https://transmog.readthedocs.io/en/latest/tutorials/intermediate/customizing-id-generation.html

Learning Objectives:
- How to configure deterministic ID generation
- How to define ID generation rules for different tables
- How to ensure consistency across multiple processing runs
- How to use custom ID fields for data relationships
"""

import hashlib
import os

# Import from transmog package
import transmog as tm


def generate_sample_data():
    """Generate sample data for deterministic ID demonstration."""
    return {
        "organization": {
            "id": "ORG-001",
            "name": "Example Corporation",
            "industry": "Technology",
            "founded": 1995,
            "departments": [
                {
                    "id": "DEPT-ENG",
                    "name": "Engineering",
                    "employees": [
                        {
                            "id": "EMP-101",
                            "name": "Alice Johnson",
                            "role": "Software Engineer",
                        },
                        {"id": "EMP-102", "name": "Bob Smith", "role": "QA Specialist"},
                    ],
                },
                {
                    "id": "DEPT-MKT",
                    "name": "Marketing",
                    "employees": [
                        {
                            "id": "EMP-201",
                            "name": "Carol Williams",
                            "role": "Marketing Manager",
                        },
                        {
                            "id": "EMP-202",
                            "name": "David Brown",
                            "role": "Content Strategist",
                        },
                    ],
                },
            ],
            "locations": [
                {
                    "id": "LOC-001",
                    "name": "Headquarters",
                    "address": {"city": "San Francisco", "state": "CA"},
                },
                {
                    "id": "LOC-002",
                    "name": "East Coast Office",
                    "address": {"city": "New York", "state": "NY"},
                },
            ],
        }
    }


def main():
    """Run the deterministic IDs example."""
    # Create output directory
    output_dir = os.path.join(
        os.path.dirname(__file__), "..", "data", "output", "deterministic_ids"
    )
    os.makedirs(output_dir, exist_ok=True)

    # Generate sample data
    data = generate_sample_data()

    print("=== Deterministic IDs Example ===")

    # Example 1: Default Random IDs (non-deterministic)
    print("\n=== Default Random IDs (Non-Deterministic) ===")

    # Process with default (random) IDs
    default_processor = tm.Processor()
    default_result = default_processor.process(data=data, entity_name="company")

    # Show the generated random IDs
    main_table = default_result.get_main_table()
    if main_table:
        print("Default-generated ID (random UUID):")
        print(f"  Main record ID: {main_table[0].get('__transmog_id', 'N/A')}")

    # Get a child table to show its IDs too
    departments_table = default_result.get_child_table(
        "company_organization_departments"
    )
    if departments_table and len(departments_table) > 0:
        print(
            f"  Department record ID: "
            f"{departments_table[0].get('__transmog_id', 'N/A')}"
        )
        print(
            f"  Parent reference: "
            f"{departments_table[0].get('__parent_transmog_id', 'N/A')}"
        )

    # Example 2: Basic Deterministic IDs
    print("\n=== Basic Deterministic IDs ===")

    # Create processor with basic deterministic ID configuration
    config = tm.TransmogConfig.with_deterministic_ids(
        source_field={
            "": "organization.id"  # Use organization.id for the root level
        }
    )
    basic_deterministic_processor = tm.Processor(config)

    # Process the data twice to demonstrate consistency
    basic_result_1 = basic_deterministic_processor.process(
        data=data, entity_name="company"
    )
    basic_result_2 = basic_deterministic_processor.process(
        data=data, entity_name="company"
    )

    # Show the deterministic IDs from both runs
    main_table_1 = basic_result_1.get_main_table()
    main_table_2 = basic_result_2.get_main_table()

    if main_table_1 and main_table_2:
        id_1 = main_table_1[0].get("__transmog_id", "N/A")
        id_2 = main_table_2[0].get("__transmog_id", "N/A")
        print(f"First run main record ID:  {id_1}")
        print(f"Second run main record ID: {id_2}")
        print(f"IDs are identical: {id_1 == id_2}")

    # Example 3: Comprehensive Deterministic IDs for All Tables
    print("\n=== Comprehensive Deterministic IDs for All Tables ===")

    # Create processor with detailed deterministic ID configuration
    id_mapping = {
        "": "organization.id",  # Root level ID field
        "company_organization_departments": "id",  # Department ID field
        "company_organization_departments_employees": "id",  # Employee ID field
        "company_organization_locations": "id",  # Location ID field
    }

    config = tm.TransmogConfig.with_deterministic_ids(source_field=id_mapping)
    detailed_processor = tm.Processor(config)
    detailed_result = detailed_processor.process(data=data, entity_name="company")

    # Show the deterministic IDs for different tables
    print("Deterministic IDs for different tables:")

    # Main table ID
    main_table = detailed_result.get_main_table()
    if main_table:
        print(f"Main table ID: {main_table[0].get('__transmog_id', 'N/A')}")

    # Department table IDs
    departments = detailed_result.get_child_table("company_organization_departments")
    if departments:
        print("\nDepartment IDs:")
        for dept in departments:
            print(
                f"  Department '{dept.get('name', 'Unknown')}': "
                f"{dept.get('__transmog_id', 'N/A')}"
            )

    # Employee table IDs
    employees = detailed_result.get_child_table(
        "company_organization_departments_employees"
    )
    if employees:
        print("\nEmployee IDs:")
        for emp in employees[:2]:  # Show first two employees
            print(
                f"  Employee '{emp.get('name', 'Unknown')}': "
                f"{emp.get('__transmog_id', 'N/A')}"
            )

    # Example 4: Custom ID Generation Function
    print("\n=== Custom ID Generation Function ===")

    # Define a custom ID generation function
    def custom_id_generator(record, table_name, field_names=None):
        """Generate a custom deterministic ID based on specific fields."""
        if not field_names:
            # Default fields to use for ID generation
            if table_name == "":  # Root table
                field_names = ["organization.id", "organization.name"]
            elif "departments" in table_name:
                field_names = ["id", "name"]
            elif "employees" in table_name:
                field_names = ["id", "name", "role"]
            elif "locations" in table_name:
                field_names = ["id", "name", "address.city"]
            else:
                field_names = ["id"]

        # Extract values for the specified fields
        values = []
        for field in field_names:
            parts = field.split(".")
            value = record
            try:
                for part in parts:
                    value = value[part]
                values.append(str(value))
            except (KeyError, TypeError):
                values.append("null")

        # Create a deterministic hash from the values
        # Note: Using SHA-256 for better security. In a real application,
        # choose an appropriate hashing algorithm for your security needs
        key = ":".join(values)
        return hashlib.sha256(key.encode()).hexdigest()

    # Create configuration with custom ID generation
    custom_config = tm.TransmogConfig.with_custom_id_generation(
        strategy=custom_id_generator
    )

    custom_processor = tm.Processor(config=custom_config)
    custom_result = custom_processor.process(data=data, entity_name="company")

    # Show the custom-generated deterministic IDs
    print("Custom-generated deterministic IDs:")

    # Main table ID
    main_table = custom_result.get_main_table()
    if main_table:
        print(f"Main table ID: {main_table[0].get('__transmog_id', 'N/A')}")

    # Department table IDs
    departments = custom_result.get_child_table("company_organization_departments")
    if departments:
        print("\nDepartment IDs (custom generated):")
        for dept in departments:
            print(
                f"  Department '{dept.get('name', 'Unknown')}': "
                f"{dept.get('__transmog_id', 'N/A')}"
            )

    # Example 5: Relationship Preservation Across Processing Runs
    print("\n=== Relationship Preservation Across Processing Runs ===")

    # Process the data multiple times to show relationship consistency
    config = tm.TransmogConfig.with_deterministic_ids(source_field=id_mapping)
    processor = tm.Processor(config)

    # First run
    run1_result = processor.process(data=data, entity_name="company")

    # Second run
    run2_result = processor.process(data=data, entity_name="company")

    # Verify relationships are preserved between runs
    print("Verifying relationships between processing runs:")

    # Check department-employee relationships
    run1_depts = run1_result.get_child_table("company_organization_departments")
    run2_depts = run2_result.get_child_table("company_organization_departments")

    run1_employees = run1_result.get_child_table(
        "company_organization_departments_employees"
    )
    run2_employees = run2_result.get_child_table(
        "company_organization_departments_employees"
    )

    if run1_depts and run2_depts and run1_employees and run2_employees:
        # Get IDs for the first department in each run
        dept1_id_run1 = run1_depts[0].get("__transmog_id", "N/A")
        dept1_id_run2 = run2_depts[0].get("__transmog_id", "N/A")

        print(f"Department ID (Run 1): {dept1_id_run1}")
        print(f"Department ID (Run 2): {dept1_id_run2}")
        print(f"Department IDs match: {dept1_id_run1 == dept1_id_run2}")

        # Get employees for this department in each run
        dept1_employees_run1 = [
            e for e in run1_employees if e.get("__parent_transmog_id") == dept1_id_run1
        ]
        dept1_employees_run2 = [
            e for e in run2_employees if e.get("__parent_transmog_id") == dept1_id_run2
        ]

        print(f"Number of employees in department (Run 1): {len(dept1_employees_run1)}")
        print(f"Number of employees in department (Run 2): {len(dept1_employees_run2)}")

        # Check if each employee has the same ID in both runs
        if dept1_employees_run1 and dept1_employees_run2:
            print("\nEmployee ID Comparison:")
            for i in range(min(len(dept1_employees_run1), len(dept1_employees_run2))):
                emp1 = dept1_employees_run1[i]
                emp2 = dept1_employees_run2[i]

                name1 = emp1.get("name", "Unknown")
                _ = emp2.get("name", "Unknown")  # Unused but kept for symmetry

                id1 = emp1.get("__transmog_id", "N/A")
                id2 = emp2.get("__transmog_id", "N/A")

                print(f"  Employee: {name1}")
                print(f"    ID (Run 1): {id1}")
                print(f"    ID (Run 2): {id2}")
                print(f"    IDs match: {id1 == id2}")

    # Write results to files for inspection
    run1_result.write_all_json(base_path=os.path.join(output_dir, "run1"), indent=2)

    run2_result.write_all_json(base_path=os.path.join(output_dir, "run2"), indent=2)

    print(f"\nOutput files written to: {output_dir}")


if __name__ == "__main__":
    main()
