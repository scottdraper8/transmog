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
2. **Field-based deterministic IDs** - Based on a single field value
3. **Custom function-based ID generation** - Complete control over ID generation,
including using multiple fields

## Basic Deterministic IDs

Let's start with field-based deterministic IDs using a single field:

```python
from transmog import Processor, TransmogConfig

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

# Configure with deterministic IDs based on a specific field
# Note: with_deterministic_ids only accepts a single field name or mapping
config = TransmogConfig.with_deterministic_ids("employeeId")  # Use employeeId field for IDs
processor = Processor(config=config)

# Or use the processor factory method directly
processor = Processor.with_deterministic_ids("employeeId")

# Process the data
result = processor.process(data=employee_data, entity_name="employee")

# Display the results
tables = result.to_dict()
for table_name, records in tables.items():
    print(f"\n=== {table_name} ===")
    for record in records:
        print(f"ID: {record['__extract_id']}, Data: {record}")
```

## Using Different Fields for Different Tables

You can specify different ID fields for different tables using a dictionary mapping:

```python
# Map table paths to their ID fields
id_mapping = {
    "": "employeeId",              # Root level uses "employeeId" field
    "employee_projects": "projectId"  # Projects table uses "projectId" field
}

# Configure with the mapping
processor = Processor.with_deterministic_ids(id_mapping)

# Process the data
result = processor.process(data=employee_data, entity_name="employee")
```

## Multiple Fields for Deterministic IDs

To use multiple fields for deterministic ID generation, you need to use a custom ID function:

```python
import hashlib

# Define a custom ID generation function
def multi_field_id_generator(record):
    """Generate an ID by combining multiple fields"""
    # For the main record, combine department and employeeId
    department = record.get("department", "")
    employee_id = record.get("employeeId", "")

    # Create a combined string and return it
    return f"{department}-{employee_id}"

# Configure with custom ID generation
processor = Processor.with_custom_id_generation(multi_field_id_generator)

# Process the data
result = processor.process(data=employee_data, entity_name="employee")
```

## Path-Aware Custom ID Generation

For more complete control, you can create a custom ID generator that handles different tables:

```python
import hashlib

# Define a custom ID generation function
def path_aware_id_generator(record):
    """Generate an ID based on record content and type"""
    # For employee records
    if "employeeId" in record:
        department = record.get("department", "")
        employee_id = record.get("employeeId", "")
        return f"EMP-{department}-{employee_id}"

    # For project records
    elif "projectId" in record:
        project_id = record.get("projectId", "")
        project_name = record.get("name", "")
        # Create a hash of the name to keep IDs compact
        name_hash = hashlib.md5(project_name.encode()).hexdigest()[:8]
        return f"PROJ-{project_id}-{name_hash}"

    # Default fallback
    else:
        record_str = str(sorted(record.items()))
        return hashlib.md5(record_str.encode()).hexdigest()[:16]

# Configure with custom ID generation
processor = Processor.with_custom_id_generation(path_aware_id_generator)

# Process the data
result = processor.process(data=employee_data, entity_name="employee")
```

## Including Parent IDs in Child Records

When using custom ID generation, you can create hierarchical IDs that include parent information:

```python
def hierarchical_id_generator(record):
    """Create hierarchical IDs that include parent information when available"""
    # Check if this is a child record with parent reference
    if "__parent_extract_id" in record:
        parent_id = record.get("__parent_extract_id", "")

        # For project records
        if "projectId" in record:
            project_id = record.get("projectId", "")
            return f"{parent_id}_PROJ_{project_id}"

    # For employee (root) records
    elif "employeeId" in record:
        return f"EMP_{record['employeeId']}"

    # Default random ID fallback
    return None  # Return None to use random UUID

# Set up the processor with custom ID generation
processor = Processor.with_custom_id_generation(hierarchical_id_generator)
```

## Using Deterministic IDs with File Processing

Deterministic IDs are especially useful when processing files:

```python
import json

# Save our data to a file
with open("employee.json", "w") as f:
    json.dump(employee_data, f)

# Configure with deterministic IDs
processor = Processor.with_deterministic_ids("employeeId")

# Process the file
result = processor.process_file("employee.json", entity_name="employee")

# Write to output files
result.write_all_json("output_directory")

# Process the same file again - will produce identical IDs
result2 = processor.process_file("employee.json", entity_name="employee")
```

## Best Practices for ID Generation

1. **Choose Stable Fields**: Use fields that don't change frequently for IDs
2. **Avoid Privacy-Sensitive Fields**: Don't include sensitive data in IDs
3. **Consider ID Length**: Balance between readability and uniqueness
4. **Test for Collisions**: Ensure your ID generation strategy doesn't create duplicates
5. **Document Your Approach**: Make your ID generation strategy clear to others

## Example Implementation

For a complete implementation example, see the deterministic_ids.py
file at `../../../examples/data_transformation/advanced/deterministic_ids.py`.

Key aspects demonstrated in the example:

- Basic deterministic IDs using organization ID field
- Comprehensive ID mapping for all tables in a nested structure
- Custom ID generation using multiple fields and hash functions
- ID consistency verification across multiple processing runs
- Output to JSON files to examine ID relationships

## Next Steps

- Explore [error recovery strategies](../advanced/error-recovery-strategies.md)
- Learn about [optimizing memory usage](../../user/advanced/performance-optimization.md)
- Try [streaming large datasets](./streaming-large-datasets.md)
