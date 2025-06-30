"""Example demonstrating natural ID discovery in transmog v1.1.0.

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

# Use the simple API - it automatically discovers natural IDs
result = tm.flatten(data, name="company", id_field="id")

# Display results
print("\nMain table name: company")
print(f"Available child tables: {list(result.tables.keys())}")

print("\nMain table:")
for record in result.main:
    print(f"  ID: {record.get('id', record.get('_id'))}")
    print(f"  Name: {record.get('name')}")
    print()

print("\nDepartments table:")
dept_table = result.tables.get("company_departments", [])
for record in dept_table:
    print(f"  ID: {record.get('id', record.get('_id'))}")
    print(f"  Parent ID: {record.get('_parent_id')}")
    print(f"  Name: {record.get('name')}")
    print()

# Example 2: Custom ID field mapping
print("\n\nExample 2: Custom ID field mapping")
print("-" * 50)

# Map specific tables to their ID fields
id_mapping = {
    "company_departments_employees": "employee_id",
    "company_products": "sku",
}

result = tm.flatten(data, name="company", id_field=id_mapping)

print("\nEmployees table (using employee_id):")
emp_table = result.tables.get("company_departments_employees", [])
for record in emp_table:
    natural_id = record.get("employee_id")
    transmog_id = record.get("_id")
    print(f"  Natural ID: {natural_id if natural_id else 'None'}")
    print(
        f"  Transmog ID: "
        f"{transmog_id if transmog_id and not natural_id else 'Not added'}"
    )
    print(f"  Parent ID: {record.get('_parent_id')}")
    print(f"  Name: {record.get('name')}")
    print()

# Example 3: Using specific ID field patterns
print("\n\nExample 3: Using specific ID field patterns")
print("-" * 50)

# For advanced use cases, access the full processor
from transmog.config import TransmogConfig
from transmog.process import Processor

# Define custom patterns to check for ID fields
custom_patterns = ["id", "ID", "sku", "employee_id", "product_code"]

config = TransmogConfig.default().with_metadata(id_field_patterns=custom_patterns)
processor = Processor(config)
result_advanced = processor.process(data, entity_name="company")

print("\nProducts table (checking custom patterns):")
prod_table = result_advanced.child_tables.get("company_products", [])
for record in prod_table:
    # Check which ID was used
    if "sku" in record and "_id" not in record:
        print(f"  Using natural ID 'sku': {record['sku']}")
    else:
        print(f"  Using transmog ID: {record.get('_id')}")
    print(f"  Name: {record.get('name')}")
    print()

# Example 4: Simple string-based ID field
print("\n\nExample 4: Simple string-based ID field")
print("-" * 50)

# Use employee_id as the ID field for all tables
result = tm.flatten(data, name="company", id_field="employee_id")

print("\nAll tables will try to use 'employee_id' where available:")
for table_name, table_data in result.all_tables.items():
    print(f"\n{table_name} table:")
    for record in table_data[:2]:  # Show first 2 records
        emp_id = record.get("employee_id")
        transmog_id = record.get("_id")
        if emp_id:
            print(f"  Using employee_id: {emp_id}")
        else:
            print(f"  Using generated _id: {transmog_id}")
        print(f"  Name: {record.get('name', 'N/A')}")
        if len(table_data) > 2:
            print("  ...")
        break

# Export results
print("\n\nExporting results with natural IDs...")
result.save("examples/output/natural_ids.json")
print("Results saved to examples/output/natural_ids.json")
