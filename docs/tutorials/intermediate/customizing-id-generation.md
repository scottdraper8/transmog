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
import transmog as tm

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

# Process with deterministic IDs based on a specific field
result = tm.flatten(
    data=employee_data,
    name="employee",
    id_field="employeeId"  # Use employeeId field for IDs
)

# Display the results
print("\n=== employee ===")
for record in result.main:
    print(f"ID: {record['_id']}, Data: {record}")

print("\n=== employee_projects ===")
for record in result.tables["employee_projects"]:
    print(f"ID: {record['_id']}, Data: {record}")
```

## Using Different Fields for Different Tables

You can specify different ID fields for different tables using a dictionary mapping:

```python
# Map table names to their ID fields
id_mapping = {
    "": "employeeId",              # Main table uses "employeeId" field
    "employee_projects": "projectId"  # Projects table uses "projectId" field
}

# Process with the mapping
result = tm.flatten(
    data=employee_data,
    name="employee",
    id_field=id_mapping
)

# Display the results
print("\n=== employee ===")
for record in result.main:
    print(f"ID: {record['_id']}")  # Will contain employeeId value

print("\n=== employee_projects ===")
for record in result.tables["employee_projects"]:
    print(f"ID: {record['_id']}")  # Will contain projectId value
```

## Multiple Fields for Deterministic IDs

To use multiple fields for deterministic ID generation, you can use transform functions:

```python
# Define transform functions for ID generation
def employee_id_generator(record):
    """Generate an ID by combining multiple fields"""
    department = record.get("department", "")
    employee_id = record.get("employeeId", "")
    return f"{department}-{employee_id}"

def project_id_generator(record):
    """Generate an ID for project records"""
    project_id = record.get("projectId", "")
    project_name = record.get("name", "")
    return f"{project_id}-{project_name.replace(' ', '_')}"

# Process with transforms
result = tm.flatten(
    data=employee_data,
    name="employee",
    transforms={
        "employeeId": employee_id_generator,  # Transform employeeId field
        "projectId": project_id_generator     # Transform projectId field
    },
    id_field={
        "": "employeeId",                # Use transformed employeeId for main table
        "employee_projects": "projectId"  # Use transformed projectId for projects table
    }
)
```

## Advanced ID Generation with Transforms

For more complex scenarios, you can create custom transforms for different fields:

```python
import hashlib

# Define transform functions
def create_employee_id(record):
    """Create a deterministic employee ID"""
    department = record.get("department", "")
    employee_id = record.get("employeeId", "")
    return f"EMP-{department}-{employee_id}"

def create_project_id(record):
    """Create a deterministic project ID"""
    project_id = record.get("projectId", "")
    project_name = record.get("name", "")
    # Create a hash of the name to keep IDs compact
    name_hash = hashlib.md5(project_name.encode()).hexdigest()[:8]
    return f"PROJ-{project_id}-{name_hash}"

# Process with advanced transforms
result = tm.flatten(
    data=employee_data,
    name="employee",
    transforms={
        "employeeId": create_employee_id,  # Transform employeeId
        "projectId": create_project_id     # Transform projectId
    },
    id_field={
        "": "employeeId",                # Use transformed employeeId for main table
        "employee_projects": "projectId"  # Use transformed projectId for projects table
    }
)
```

## Creating Hierarchical IDs

You can create hierarchical IDs by accessing parent information in your transforms:

```python
# First, process the data normally to get parent-child relationships
result = tm.flatten(
    data=employee_data,
    name="employee",
    id_field={
        "": "employeeId",
        "employee_projects": "projectId"
    }
)

# Access the parent-child relationships
employee = result.main[0]
employee_id = employee["_id"]

projects = result.tables["employee_projects"]
for project in projects:
    parent_id = project["_parent_id"]  # Contains the parent's ID
    project_id = project["projectId"]

    # Create a hierarchical ID
    hierarchical_id = f"{parent_id}_PROJ_{project_id}"
    print(f"Hierarchical ID: {hierarchical_id}")
```

## Using Deterministic IDs with File Processing

Deterministic IDs are especially useful when processing files:

```python
import json

# Save our data to a file
with open("employee.json", "w") as f:
    json.dump(employee_data, f)

# Process the file with deterministic IDs
result = tm.flatten_file(
    "employee.json",
    name="employee",
    id_field="employeeId"
)

# Save to output files
result.save("output_directory")

# Process the same file again - will produce identical IDs
result2 = tm.flatten_file(
    "employee.json",
    name="employee",
    id_field="employeeId"
)

# Verify IDs are the same
assert result.main[0]["_id"] == result2.main[0]["_id"]
```

## Using Natural IDs

If your data already contains good ID fields, you can use the "auto" option to automatically discover them:

```python
# Process with natural ID discovery
result = tm.flatten(
    data=employee_data,
    name="employee",
    id_field="auto"  # Automatically discover ID fields
)
```

## Best Practices for ID Generation

1. **Choose Stable Fields**: Use fields that don't change frequently for IDs
2. **Avoid Privacy-Sensitive Fields**: Don't include sensitive data in IDs
3. **Consider ID Length**: Balance between readability and uniqueness
4. **Test for Collisions**: Ensure your ID generation strategy doesn't create duplicates
5. **Document Your Approach**: Make your ID generation strategy clear to others

## Example Implementation

For a complete implementation example, see the deterministic_ids.py
file at [GitHub](https://github.com/scottdraper8/transmog/blob/main/examples/data_transformation/advanced/deterministic_ids.py).

Key aspects demonstrated in the example:

- Basic deterministic IDs using organization ID field
- Comprehensive ID mapping for all tables in a nested structure
- Custom ID generation using multiple fields and hash functions
- ID consistency verification across multiple processing runs
- Output to JSON files to examine ID relationships

## Next Steps

- Explore [error handling strategies](../../user/advanced/error-handling.md)
- Learn about [optimizing memory usage](../../user/advanced/performance-optimization.md)
- Try [streaming large datasets](./streaming-large-datasets.md)
