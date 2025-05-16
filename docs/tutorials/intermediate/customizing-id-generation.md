# Customizing ID Generation

This tutorial demonstrates how to customize the ID generation process in Transmog to create deterministic,
meaningful identifiers.

## Why Customize ID Generation?

By default, Transmog generates random UUIDs for records. However, customizing ID generation offers several benefits:

- **Deterministic Processing**: Same input always produces the same output
- **Meaningful IDs**: Create IDs that contain useful information
- **Consistent References**: Ensure consistent references across multiple runs
- **Integration**: Align with existing systems that expect specific ID formats

## Built-in ID Generation Options

Transmog provides three main ID generation approaches:

1. **Random UUIDs** (default) - Non-deterministic, unique identifiers
2. **Field-based deterministic IDs** - Based on field values
3. **Custom function-based ID generation** - Complete control over ID generation

## Basic Deterministic IDs

Let's start with field-based deterministic IDs:

```python
from transmog import TransmogProcessor, TransmogConfig

# Sample data
employee_data = {
    "department": "Engineering",
    "employeeId": "E12345",
    "name": "Jane Smith",
    "position": "Senior Developer",
    "projects": [
        {"projectId": "P100", "name": "API Redesign"},
        {"projectId": "P200", "name": "Database Migration"}
    ]
}

# Configure with deterministic IDs based on specific fields
config = TransmogConfig().with_deterministic_ids(
    id_fields=["employeeId"],  # Use employeeId field for the main table
    child_id_fields={
        "projects": ["projectId"]  # Use projectId field for projects table
    }
)

# Create processor with custom configuration
processor = TransmogProcessor(config)

# Process the data
result = processor.process_data(employee_data)

# Display the results
tables = result.to_dict()
for table_name, records in tables.items():
    print(f"\n=== {table_name} ===")
    for record in records:
        print(f"ID: {record['id']}, Data: {record}")
```

## Advanced Deterministic IDs

For more complex scenarios, you can combine multiple fields and customize the format:

```python
# Configure with multi-field deterministic IDs
config = TransmogConfig().with_deterministic_ids(
    id_fields=["department", "employeeId"],  # Combine department and employeeId
    id_field_separator="-",  # Use hyphen as separator
    child_id_prefix="child_",  # Add prefix to child IDs
    child_id_fields={
        "projects": ["projectId", "name"]  # Combine projectId and name
    }
)

processor = TransmogProcessor(config)
result = processor.process_data(employee_data)
```

## Custom ID Generation Function

For complete control, you can provide a custom ID generation function:

```python
import hashlib

# Define a custom ID generation function
def hash_based_id_generator(record, path):
    """Generate an ID by hashing relevant fields based on the table path"""
    # For the main table
    if not path:
        # Use employee ID if available, otherwise hash the whole record
        if "employeeId" in record:
            return f"EMP-{record['employeeId']}"
        else:
            record_str = str(sorted(record.items()))
            return hashlib.md5(record_str.encode()).hexdigest()[:10]

    # For the projects table
    elif path[-1] == "projects":
        if "projectId" in record:
            return f"PROJ-{record['projectId']}"
        else:
            return f"PROJ-{hashlib.md5(str(record).encode()).hexdigest()[:8]}"

    # Default for any other tables
    else:
        return f"ID-{hashlib.md5(str(record).encode()).hexdigest()[:8]}"

# Configure with the custom ID function
config = TransmogConfig().with_id_generation(id_generator=hash_based_id_generator)

processor = TransmogProcessor(config)
result = processor.process_data(employee_data)
```

## Creating Hierarchical IDs

You can create IDs that reflect the data hierarchy:

```python
# Configure hierarchical IDs
config = TransmogConfig().with_deterministic_ids(
    id_fields=["employeeId"],
    include_parent_ids=True,  # Include parent ID in child IDs
    child_id_fields={
        "projects": ["projectId"]
    }
)

processor = TransmogProcessor(config)
result = processor.process_data(employee_data)

# This will create IDs like:
# employee: "E12345"
# projects: "E12345_P100", "E12345_P200"
```

## Using Deterministic IDs with File Processing

Deterministic IDs are especially useful when processing files:

```python
import json

# Save our data to a file
with open("employee.json", "w") as f:
    json.dump(employee_data, f)

# Configure with deterministic IDs
config = TransmogConfig().with_deterministic_ids(
    id_fields=["employeeId"],
    child_id_fields={
        "projects": ["projectId"]
    }
)

# Process the file
processor = TransmogProcessor(config)
result = processor.process_file("employee.json")

# Write to output files
result.write_all_json("output_directory")

# Process the same file again - will produce identical IDs
result2 = processor.process_file("employee.json")
```

## Best Practices for ID Generation

1. **Choose Stable Fields**: Use fields that don't change frequently for IDs
2. **Avoid Privacy-Sensitive Fields**: Don't include sensitive data in IDs
3. **Consider ID Length**: Balance between readability and uniqueness
4. **Test for Collisions**: Ensure your ID generation strategy doesn't create duplicates
5. **Document Your Approach**: Make your ID generation strategy clear to others

## Next Steps

- Explore [error recovery strategies](../advanced/error-recovery-strategies.md)
- Learn about [optimizing memory usage](../../user/advanced/performance-optimization.md)
- Try [streaming large datasets](./streaming-large-datasets.md)
