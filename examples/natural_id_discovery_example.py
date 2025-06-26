"""Example demonstrating natural ID discovery in transmog.

This example shows how transmog can discover and use existing ID fields
in your data instead of always generating synthetic IDs.
"""

import transmog as tm

# Sample data with natural ID fields
data = [
    {
        "id": "COMP-001",  # Natural ID at top level
        "name": "TechCorp",
        "departments": [
            {
                "id": "DEPT-HR",  # Natural ID in child records
                "name": "Human Resources",
                "employees": [
                    {
                        "employee_id": "EMP-001",
                        "name": "Alice",
                    },  # Different ID field name
                    {"employee_id": "EMP-002", "name": "Bob"},
                ],
            },
            {
                "id": "DEPT-ENG",
                "name": "Engineering",
                "employees": [
                    {"name": "Charlie"},  # No natural ID - will get transmog ID
                    {"name": "David"},
                ],
            },
        ],
        "products": [
            {
                "sku": "PROD-001",  # Custom ID field name
                "name": "Widget Pro",
            },
            {
                "name": "Widget Lite"  # No ID field
            },
        ],
    }
]

# Example 1: Default behavior - discovers common ID fields automatically
print("Example 1: Default natural ID discovery")
print("-" * 50)

processor = tm.Processor.with_natural_ids()
result = processor.process(data, entity_name="company")

# Display results
print(f"\nMain table name: {result.entity_name}")
print(f"Available child tables: {list(result.child_tables.keys())}")

print("\nMain table:")
for record in result.main_table:
    print(f"  ID: {record.get('id', record.get('__transmog_id'))}")
    print(f"  Name: {record.get('name')}")
    print()

print("\nDepartments table:")
dept_table = result.child_tables.get("company_departments", [])
for record in dept_table:
    print(f"  ID: {record.get('id', record.get('__transmog_id'))}")
    print(f"  Parent ID: {record.get('__parent_transmog_id')}")
    print(f"  Name: {record.get('name')}")
    print()

# Example 2: Custom ID field mapping
print("\n\nExample 2: Custom ID field mapping")
print("-" * 50)

# Map specific tables to their ID fields
id_mapping = {
    "company_departments_employees": "employee_id",
    "company_products": "sku",
    "*": "id",  # Default for all other tables
}

processor = tm.Processor.with_natural_ids(id_field_mapping=id_mapping)
result = processor.process(data, entity_name="company")

print("\nEmployees table (using employee_id):")
emp_table = result.child_tables.get("company_departments_employees", [])
for record in emp_table:
    natural_id = record.get("employee_id")
    transmog_id = record.get("__transmog_id")
    print(f"  Natural ID: {natural_id if natural_id else 'None'}")
    print(
        f"  Transmog ID: "
        f"{transmog_id if transmog_id and not natural_id else 'Not added'}"
    )
    print(f"  Parent ID: {record.get('__parent_transmog_id')}")
    print(f"  Name: {record.get('name')}")
    print()

# Example 3: Custom ID field patterns
print("\n\nExample 3: Custom ID field patterns")
print("-" * 50)

# Define custom patterns to check for ID fields
custom_patterns = ["id", "ID", "sku", "employee_id", "product_code"]

processor = tm.Processor.with_natural_ids(id_field_patterns=custom_patterns)
result = processor.process(data, entity_name="company")

print("\nProducts table (checking custom patterns):")
prod_table = result.child_tables.get("company_products", [])
for record in prod_table:
    # Check which ID was used
    if "sku" in record and "__transmog_id" not in record:
        print(f"  Using natural ID 'sku': {record['sku']}")
    else:
        print(f"  Using transmog ID: {record.get('__transmog_id')}")
    print(f"  Name: {record.get('name')}")
    print()

# Example 4: Force transmog IDs (old behavior)
print("\n\nExample 4: Force transmog IDs (old behavior)")
print("-" * 50)

# Create a processor that always adds transmog IDs regardless of natural IDs
processor = tm.Processor.with_natural_ids().with_metadata(force_transmog_id=True)
result = processor.process(data, entity_name="company")

print("\nMain table (with force_transmog_id=True):")
for record in result.main_table:
    natural_id = record.get("id")
    transmog_id = record.get("__transmog_id")
    print(f"  Natural ID: {natural_id}")
    print(f"  Transmog ID: {transmog_id}")
    if natural_id and transmog_id:
        print("  Both IDs present - natural ID preserved, transmog ID added")
    print()

# Export results
print("\n\nExporting results with natural IDs...")
result.write_all_json("examples/output/natural_ids")
