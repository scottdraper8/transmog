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
    print("\n=== Example 1: Default Random IDs (Non-Deterministic) ===")
    print("By default, Transmog generates random UUIDs for record identification")

    # Process with default (random) IDs - two runs
    default_result_1 = tm.flatten(data, name="company")
    default_result_2 = tm.flatten(data, name="company")

    # Show the generated random IDs
    main_table_1 = default_result_1.main
    main_table_2 = default_result_2.main

    if main_table_1 and main_table_2:
        id_1 = main_table_1[0].get("_id", "N/A")
        id_2 = main_table_2[0].get("_id", "N/A")
        print(f"First run main record ID:  {id_1}")
        print(f"Second run main record ID: {id_2}")
        print(f"IDs are identical: {id_1 == id_2}")

    # Example 2: Using Natural IDs for Deterministic Behavior
    print("\n=== Example 2: Using Natural IDs (Simple Approach) ===")
    print("Enable natural ID detection to use existing ID fields")

    # Process with natural IDs enabled
    natural_result_1 = tm.flatten(data, name="company", natural_ids=True)
    natural_result_2 = tm.flatten(data, name="company", natural_ids=True)

    # Show the natural IDs from both runs
    natural_main_1 = natural_result_1.main
    natural_main_2 = natural_result_2.main

    if natural_main_1 and natural_main_2:
        id_1 = natural_main_1[0].get("_id", "N/A")
        id_2 = natural_main_2[0].get("_id", "N/A")
        print(f"First run main record ID:  {id_1}")
        print(f"Second run main record ID: {id_2}")
        print(f"IDs are identical: {id_1 == id_2}")

    # Show child table IDs too
    if "company_organization_departments" in natural_result_1.tables:
        dept_table_1 = natural_result_1.tables["company_organization_departments"]
        dept_table_2 = natural_result_2.tables["company_organization_departments"]

        if dept_table_1 and dept_table_2:
            dept_id_1 = dept_table_1[0].get("_id", "N/A")
            dept_id_2 = dept_table_2[0].get("_id", "N/A")
            print(f"First department ID (Run 1): {dept_id_1}")
            print(f"First department ID (Run 2): {dept_id_2}")
            print(f"Department IDs match: {dept_id_1 == dept_id_2}")

    # Example 3: Advanced Deterministic Configuration
    print("\n=== Example 3: Advanced Deterministic Configuration ===")
    print("For fine-grained control, use the Processor class with custom configuration")

    # Import advanced classes for deterministic configuration
    from transmog.config import TransmogConfig
    from transmog.process import Processor

    # Create processor with deterministic ID configuration
    id_mapping = {
        "": "organization.id",  # Root level ID field
        "company_organization_departments": "id",  # Department ID field
        "company_organization_departments_employees": "id",  # Employee ID field
        "company_organization_locations": "id",  # Location ID field
    }

    config = TransmogConfig.with_deterministic_ids(source_field=id_mapping)
    detailed_processor = Processor(config)

    # Process data twice to show consistency
    detailed_result_1 = detailed_processor.process(data=data, entity_name="company")
    detailed_result_2 = detailed_processor.process(data=data, entity_name="company")

    # Show the deterministic IDs for different tables
    print("Deterministic IDs for different tables:")

    # Main table ID
    main_table_1 = detailed_result_1.get_main_table()
    main_table_2 = detailed_result_2.get_main_table()
    if main_table_1 and main_table_2:
        id_1 = main_table_1[0].get("__transmog_id", "N/A")
        id_2 = main_table_2[0].get("__transmog_id", "N/A")
        print(f"Main table ID (Run 1): {id_1}")
        print(f"Main table ID (Run 2): {id_2}")
        print(f"Main IDs match: {id_1 == id_2}")

    # Department table IDs
    departments_1 = detailed_result_1.get_child_table(
        "company_organization_departments"
    )
    departments_2 = detailed_result_2.get_child_table(
        "company_organization_departments"
    )
    if departments_1 and departments_2:
        print("\nDepartment IDs:")
        for i, (dept1, dept2) in enumerate(zip(departments_1, departments_2)):
            name = dept1.get("name", "Unknown")
            id_1 = dept1.get("__transmog_id", "N/A")
            id_2 = dept2.get("__transmog_id", "N/A")
            print(
                f"  Department '{name}': {id_1} (Run 1), {id_2} (Run 2), Match: {id_1 == id_2}"
            )

    # Example 4: Custom ID Generation Function
    print("\n=== Example 4: Custom ID Generation Function ===")
    print("Create custom deterministic IDs using a custom function")

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
        key = ":".join(values)
        return hashlib.sha256(key.encode()).hexdigest()[:16]  # Truncate for readability

    # Create configuration with custom ID generation
    custom_config = TransmogConfig.with_custom_id_generation(
        strategy=custom_id_generator
    )

    custom_processor = Processor(config=custom_config)
    custom_result_1 = custom_processor.process(data=data, entity_name="company")
    custom_result_2 = custom_processor.process(data=data, entity_name="company")

    # Show the custom-generated deterministic IDs
    print("Custom-generated deterministic IDs:")

    # Main table ID
    main_table_1 = custom_result_1.get_main_table()
    main_table_2 = custom_result_2.get_main_table()
    if main_table_1 and main_table_2:
        id_1 = main_table_1[0].get("__transmog_id", "N/A")
        id_2 = main_table_2[0].get("__transmog_id", "N/A")
        print(f"Main table ID (Run 1): {id_1}")
        print(f"Main table ID (Run 2): {id_2}")
        print(f"Custom IDs match: {id_1 == id_2}")

    # Department table IDs
    departments_1 = custom_result_1.get_child_table("company_organization_departments")
    departments_2 = custom_result_2.get_child_table("company_organization_departments")
    if departments_1 and departments_2:
        print("\nDepartment IDs (custom generated):")
        for i, (dept1, dept2) in enumerate(zip(departments_1, departments_2)):
            name = dept1.get("name", "Unknown")
            id_1 = dept1.get("__transmog_id", "N/A")
            id_2 = dept2.get("__transmog_id", "N/A")
            print(
                f"  Department '{name}': {id_1} (Run 1), {id_2} (Run 2), Match: {id_1 == id_2}"
            )

    # Example 5: Relationship Preservation Across Processing Runs
    print("\n=== Example 5: Relationship Preservation ===")
    print("Demonstrating how deterministic IDs preserve relationships")

    # Use the natural IDs approach for relationship demonstration
    run1_result = tm.flatten(data, name="company", natural_ids=True)
    run2_result = tm.flatten(data, name="company", natural_ids=True)

    # Check department-employee relationships
    if (
        "company_organization_departments" in run1_result.tables
        and "company_organization_departments_employees" in run1_result.tables
    ):
        run1_depts = run1_result.tables["company_organization_departments"]
        run2_depts = run2_result.tables["company_organization_departments"]
        run1_employees = run1_result.tables[
            "company_organization_departments_employees"
        ]
        run2_employees = run2_result.tables[
            "company_organization_departments_employees"
        ]

        if run1_depts and run2_depts and run1_employees and run2_employees:
            # Get IDs for the first department in each run
            dept1_id_run1 = run1_depts[0].get("_id", "N/A")
            dept1_id_run2 = run2_depts[0].get("_id", "N/A")

            print(f"Department ID (Run 1): {dept1_id_run1}")
            print(f"Department ID (Run 2): {dept1_id_run2}")
            print(f"Department IDs match: {dept1_id_run1 == dept1_id_run2}")

            # Get employees for this department in each run
            dept1_employees_run1 = [
                e for e in run1_employees if e.get("_parent_id") == dept1_id_run1
            ]
            dept1_employees_run2 = [
                e for e in run2_employees if e.get("_parent_id") == dept1_id_run2
            ]

            print(
                f"Number of employees in department (Run 1): {len(dept1_employees_run1)}"
            )
            print(
                f"Number of employees in department (Run 2): {len(dept1_employees_run2)}"
            )

            # Check if each employee has the same ID in both runs
            if dept1_employees_run1 and dept1_employees_run2:
                print("\nEmployee ID Comparison:")
                for emp1, emp2 in zip(dept1_employees_run1, dept1_employees_run2):
                    name1 = emp1.get("name", "Unknown")
                    id1 = emp1.get("_id", "N/A")
                    id2 = emp2.get("_id", "N/A")

                    print(f"  Employee: {name1}")
                    print(f"    ID (Run 1): {id1}")
                    print(f"    ID (Run 2): {id2}")
                    print(f"    IDs match: {id1 == id2}")

    # Example 6: Practical Use Cases
    print("\n=== Example 6: Practical Use Cases ===")

    print("When to use deterministic IDs:")
    print("1. Data versioning and change tracking")
    print("2. Incremental data loading")
    print("3. Data synchronization between systems")
    print("4. Reproducible data processing pipelines")
    print("5. Testing and debugging data transformations")

    print("\nApproaches comparison:")
    print("- Default (Random): Best for one-time processing, no ID requirements")
    print("- Natural IDs: Simple, uses existing ID fields automatically")
    print("- Advanced Config: Fine-grained control over ID generation")
    print("- Custom Function: Maximum flexibility for complex ID requirements")

    # Example 7: Save results for comparison
    print("\n=== Example 7: Saving Results ===")

    # Save results from different runs to demonstrate consistency
    run1_output = os.path.join(output_dir, "run1")
    run2_output = os.path.join(output_dir, "run2")

    os.makedirs(run1_output, exist_ok=True)
    os.makedirs(run2_output, exist_ok=True)

    # Save natural ID results
    run1_result.save(os.path.join(run1_output, "main.json"))
    run2_result.save(os.path.join(run2_output, "main.json"))

    # Save individual tables
    for table_name, table_data in run1_result.tables.items():
        run1_result.save(
            os.path.join(run1_output, f"{table_name}.json"), table=table_name
        )

    for table_name, table_data in run2_result.tables.items():
        run2_result.save(
            os.path.join(run2_output, f"{table_name}.json"), table=table_name
        )

    print(f"Run 1 results saved to: {run1_output}")
    print(f"Run 2 results saved to: {run2_output}")
    print("Compare the files to verify ID consistency between runs")

    # Example 8: Performance considerations
    print("\n=== Example 8: Performance Considerations ===")

    print("Performance comparison of different ID strategies:")

    import time

    # Time different approaches
    approaches = [
        ("Random IDs", lambda: tm.flatten(data, name="company")),
        ("Natural IDs", lambda: tm.flatten(data, name="company", natural_ids=True)),
    ]

    for name, func in approaches:
        start_time = time.time()
        for _ in range(10):  # Run 10 times
            result = func()
        end_time = time.time()
        avg_time = (end_time - start_time) / 10
        print(f"- {name}: {avg_time:.4f} seconds average")

    print("\nPerformance notes:")
    print("- Random IDs: Fastest, no additional processing")
    print("- Natural IDs: Slightly slower due to ID field detection")
    print("- Custom functions: Speed depends on complexity of function")
    print("- Deterministic approaches enable caching and incremental processing")

    print("\n=== Example Completed Successfully ===")
    print("\nKey takeaways:")
    print("1. Use natural_ids=True for simple deterministic behavior")
    print("2. Use advanced configuration for fine-grained control")
    print("3. Custom functions provide maximum flexibility")
    print("4. Deterministic IDs enable reproducible data processing")
    print("5. Consider performance trade-offs when choosing approach")
    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    main()
