# Using Natural IDs in Your Data

This tutorial demonstrates how to use Transmog's natural ID discovery feature to leverage existing ID fields in
your data instead of always generating synthetic IDs.

## Prerequisites

- Basic understanding of Transmog
- Python 3.8 or newer
- Transmog 1.1.0 or newer

## Introduction

Many datasets already contain well-defined unique identifiers. Transmog's natural ID discovery feature allows you
to use these existing IDs instead of generating synthetic ones, which can be useful for:

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

The simplest way to use natural ID discovery is to create a processor with the `with_natural_ids()` method:

```python
import transmog as tm

# Create a processor with natural ID discovery enabled
processor = tm.Processor.with_natural_ids()

# Process the data
result = processor.process(data, entity_name="company")
```

With this configuration, Transmog will check for common ID field names like "id", "ID", "uuid", etc. In our
example, only the top-level "id" field will be recognized.

## Step 3: Customize ID Field Detection

To recognize the custom ID fields in our data, we can provide a mapping of table names to ID field names:

```python
# Create a mapping of tables to their ID fields
id_mapping = {
    "company_departments": "dept_code",
    "company_departments_employees": "employee_id",
    "*": "id"  # Default for all other tables
}

# Create processor with custom ID mapping
processor = tm.Processor.with_natural_ids(id_field_mapping=id_mapping)

# Process the data
result = processor.process(data, entity_name="company")
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
for record in result.main_table:
    print(f"  ID: {record.get('id')}")
    print(f"  Name: {record.get('name')}")
    print(f"  Has transmog ID: {'__transmog_id' in record}")
    print()

# Check departments table
print("Departments table:")
dept_table = result.child_tables["company_departments"]
for record in dept_table:
    print(f"  ID: {record.get('dept_code')}")
    print(f"  Name: {record.get('name')}")
    print(f"  Parent ID: {record.get('__parent_transmog_id')}")
    print(f"  Has transmog ID: {'__transmog_id' in record}")
    print()

# Check employees table
print("Employees table:")
emp_table = result.child_tables["company_departments_employees"]
for record in emp_table:
    print(f"  ID: {record.get('employee_id', 'N/A')}")
    print(f"  Name: {record.get('name')}")
    print(f"  Parent ID: {record.get('__parent_transmog_id')}")
    print(f"  Has transmog ID: {'__transmog_id' in record}")
    print()
```

You'll notice that:

1. Records with natural IDs don't have a `__transmog_id` field
2. Records without natural IDs (like some employees) do have a `__transmog_id` field
3. Parent-child relationships are maintained using the natural IDs

## Step 5: Alternative Approach - Custom ID Field Patterns

Instead of mapping tables to specific fields, you can also provide a list of field names to check:

```python
# Define custom patterns to check for ID fields
custom_patterns = ["id", "dept_code", "employee_id", "uuid", "code"]

# Create processor with custom ID patterns
processor = tm.Processor.with_natural_ids(id_field_patterns=custom_patterns)

# Process the data
result = processor.process(data, entity_name="company")
```

This approach is simpler but less precise than the mapping approach. It will check each record for the specified
fields in order and use the first one found.

## Step 6: Export the Results

Finally, let's export the processed data to see the results:

```python
# Export to JSON
result.write_all_json("output/natural_ids")

# Or export to CSV
result.write_all_csv("output/natural_ids")
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
processor = tm.Processor.with_natural_ids(id_field_mapping={"company_departments": "dept_code"})
result = processor.process(mixed_data, entity_name="company")

# Check departments table
dept_table = result.child_tables["company_departments"]
for record in dept_table:
    print(f"  Name: {record.get('name')}")
    print(f"  Natural ID: {record.get('dept_code', 'None')}")
    print(f"  Transmog ID: {record.get('__transmog_id', 'None')}")
    print()
```

You'll see that the HR department uses its natural ID, while the Engineering department gets a synthetic ID.

## Conclusion

Natural ID discovery allows you to leverage existing IDs in your data while still maintaining all the benefits of
Transmog's processing capabilities. By customizing the ID field detection, you can handle a wide variety of data
structures and naming conventions.

## Next Steps

- Learn about [deterministic IDs](../../user/advanced/deterministic-ids.md) for another approach to ID generation
- Explore [error handling strategies](../advanced/error-recovery-strategies.md) for dealing with problematic data
- See how to [optimize performance](../advanced/optimizing-memory-usage.md) for large datasets
