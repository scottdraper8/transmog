# Using Natural IDs in Your Data

This tutorial demonstrates how to use Transmog's natural ID discovery feature to leverage existing ID fields in
data instead of generating synthetic IDs.

## Prerequisites

- Basic understanding of Transmog
- Python 3.8 or newer
- Transmog 1.1.0 or newer

## Introduction

Many datasets already contain well-defined unique identifiers. Transmog's natural ID discovery feature allows use of existing IDs instead of generating synthetic ones, which is useful for:

- Maintaining data lineage and traceability
- Preserving referential integrity with external systems
- Making output data more readable and meaningful

## Step 1: Prepare Your Data

Let's start with a sample dataset that contains various natural ID fields:

```python
data = {
    "id": "COMP-001",  # Standard ID field
    "name": "TechCorp",
    "departments": [
        {
            "dept_code": "DEPT-HR",  # Custom ID field
            "name": "Human Resources",
            "employees": [
                {
                    "employee_id": "EMP-001",  # Another custom ID field
                    "name": "Alice",
                },
                {"employee_id": "EMP-002", "name": "Bob"},
            ],
        },
        {
            "dept_code": "DEPT-ENG",
            "name": "Engineering",
            "employees": [
                {"name": "Charlie"},  # No ID field
                {"name": "David"},
            ],
        },
    ],
}
```

Notice how different entities use different field names for their IDs.

## Step 2: Basic Natural ID Discovery

The simplest way to use natural ID discovery is to use the `id_field="auto"` parameter:

```python
import transmog as tm

# Process the data with automatic ID discovery
result = tm.flatten(data, name="company", id_field="auto")
```

With this configuration, Transmog will check for common ID field names like "id", "ID", "uuid", etc. In our
example, only the top-level "id" field will be recognized.

## Step 3: Customize ID Field Detection

To recognize the custom ID fields in the data, provide a mapping of table names to ID field names:

```python
# Create a mapping of tables to their ID fields
id_mapping = {
    "": "id",                              # Main table uses "id" field
    "company_departments": "dept_code",           # Departments table uses "dept_code" field
    "company_departments_employees": "employee_id" # Employees table uses "employee_id" field
}

# Process with custom ID mapping
result = tm.flatten(data, name="company", id_field=id_mapping)
```

Now Transmog will use:

- "id" for the main company table
- "dept_code" for the departments table
- "employee_id" for the employees table

## Step 4: Inspect the Results

Let's examine the processed data to see how natural IDs were used:

```python
# Check main table
print("Main table:")
for record in result.main:
    print(f"  ID: {record.get('id')}")
    print(f"  Name: {record.get('name')}")
    print(f"  Has transmog ID: {'_id' in record}")
    print()

# Check departments table
print("Departments table:")
dept_table = result.tables["company_departments"]
for record in dept_table:
    print(f"  ID: {record.get('dept_code')}")
    print(f"  Name: {record.get('name')}")
    print(f"  Parent ID: {record.get('_parent_id')}")
    print(f"  Has transmog ID: {'_id' in record}")
    print()

# Check employees table
print("Employees table:")
emp_table = result.tables["company_departments_employees"]
for record in emp_table:
    print(f"  ID: {record.get('employee_id', 'N/A')}")
    print(f"  Name: {record.get('name')}")
    print(f"  Parent ID: {record.get('_parent_id')}")
    print(f"  Has transmog ID: {'_id' in record}")
    print()
```

You'll notice that:

1. Records with natural IDs still have an `_id` field that contains the natural ID value
2. Records without natural IDs get a generated `_id` field
3. Parent-child relationships are maintained using the IDs

## Step 5: Alternative Approach - Single ID Field

A single ID field can be specified for all tables:

```python
# Process with a single ID field
result = tm.flatten(data, name="company", id_field="id")
```

This approach is simpler but less flexible than the mapping approach. It will only use the specified field as the ID.

## Step 6: Export the Results

Finally, let's export the processed data to see the results:

```python
# Export to any format with a single call
result.save("output/natural_ids")

# Or specify the format explicitly
result.save("output/natural_ids.json")  # JSON format
result.save("output/natural_ids.csv")   # CSV format
```

## Advanced: Handling Mixed ID Fields

In real-world data, some records might have natural IDs while others don't. Transmog handles this gracefully:

```python
# Mixed data
mixed_data = {
    "id": "COMP-001",
    "name": "TechCorp",
    "departments": [
        {"dept_code": "DEPT-HR", "name": "HR"},  # Has natural ID
        {"name": "Engineering"},  # No natural ID
    ],
}

# Process with natural ID discovery
result = tm.flatten(
    mixed_data, 
    name="company", 
    id_field={"": "id", "company_departments": "dept_code"}
)

# Check departments table
dept_table = result.tables["company_departments"]
for record in dept_table:
    print(f"  Name: {record.get('name')}")
    print(f"  Natural ID: {record.get('dept_code', 'None')}")
    print(f"  ID field: {record.get('_id')}")
    print()
```

The HR department uses its natural ID, while the Engineering department gets a synthetic ID.

## Combining with Other Features

Natural ID discovery can be combined with other Transmog features:

```python
# Combine with custom delimiter and error handling
result = tm.flatten(
    data,
    name="company",
    id_field={"": "id", "company_departments": "dept_code"},
    delimiter="__",            # Use double underscore as delimiter
    error_handling="skip"      # Skip records with errors
)
```

## Conclusion

Natural ID discovery allows you to leverage existing IDs in your data while still maintaining all the benefits of
Transmog's processing capabilities. By customizing the ID field detection, you can handle a wide variety of data
structures and naming conventions.

## Next Steps

- Learn about [deterministic IDs](../../user/advanced/deterministic-ids.md) for another approach to ID generation
- Explore [error handling strategies](../advanced/error-recovery-strategies.md) for dealing with problematic data
- See how to [optimize performance](../advanced/optimizing-memory-usage.md) for large datasets
