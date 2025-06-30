# Natural ID Discovery

Transmog can discover and use existing ID fields in your data, avoiding the generation of synthetic IDs when
natural keys already exist. This feature is particularly useful when:

- Your data already contains well-defined unique identifiers
- You want to preserve original IDs for data traceability
- You need to maintain referential integrity with external systems

## Basic Usage

By default, Transmog generates synthetic UUIDs for all records. To enable natural ID discovery:

```python
import transmog as tm

# Process data using natural IDs
result = tm.flatten(data, name="company", id_field="auto")

# Access the results as usual
main_table = result.main
```

With natural ID discovery enabled (`id_field="auto"`), Transmog will:

1. Look for common ID fields in each record (like "id", "uuid", "code", etc.)
2. Use the first valid ID field found instead of generating a synthetic ID
3. Fall back to generating a synthetic ID if no suitable ID field is found

## Customizing ID Field Detection

### Specifying Exact ID Field Names

You can specify the exact field name to use as the ID:

```python
# Use a specific field as the ID
result = tm.flatten(data, name="company", id_field="company_id")
```

### Table-Specific ID Fields

Different tables often use different field names for IDs. You can map specific tables to their ID fields:

```python
# Map specific tables to their ID fields
id_mapping = {
    "": "id",                              # Main table uses "id" field
    "company_departments_employees": "employee_id",  # Employees table uses "employee_id"
    "company_products": "sku",             # Products table uses "sku"
}

result = tm.flatten(data, name="company", id_field=id_mapping)
```

The empty string key `""` represents the main table. All other keys should match the generated table names.

## ID Field Selection Rules

When determining which field to use as an ID, Transmog follows these rules:

1. If `id_field` is a string (other than "auto"):
   - Use that specific field name for all tables
   - Generate synthetic IDs if the field doesn't exist or has no value

2. If `id_field` is a dictionary:
   - For each table, check if there's a specific mapping
   - Use the specified field if it exists and has a value
   - Generate synthetic IDs if the field doesn't exist or has no value

3. If `id_field` is "auto":
   - Check common ID field names ("id", "uuid", "code", etc.)
   - Use the first valid field found
   - Fall back to generating a synthetic ID if no suitable field is found

A field is considered suitable if:
- It exists in the record
- Its value is not null/None
- Its value is a scalar type (string, number)
- If it's a string, it's not empty

## Parent-Child Relationships

When using natural IDs, parent-child relationships are maintained using the natural IDs:

```python
# With natural ID discovery
result = tm.flatten(data, name="company", id_field="auto")

# Child records will reference parent's natural ID
dept = result.tables["company_departments"][0]
print(dept["_parent_id"])  # Will contain parent's natural ID
```

## Mixed ID Fields

Transmog handles records with mixed ID field presence gracefully:

- Records with natural IDs use those IDs
- Records without natural IDs get synthetic IDs
- All records maintain proper parent-child relationships

## Forcing Synthetic IDs

To revert to the default behavior of always adding synthetic IDs:

```python
# Default behavior always adds synthetic IDs
result = tm.flatten(data, name="company")
```

## Example

```python
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
                        "employee_id": "EMP-001",  # Different ID field name
                        "name": "Alice",
                    },
                    {"employee_id": "EMP-002", "name": "Bob"},
                ],
            },
        ],
    }
]

# Custom ID field mapping
id_mapping = {
    "": "id",                              # Main table uses "id" field
    "company_departments_employees": "employee_id",  # Employees table uses "employee_id"
}

# Process with natural ID discovery
result = tm.flatten(data, name="company", id_field=id_mapping)

# Save results
result.save("output/natural_ids")
```

## Best Practices

- Use consistent ID field names within each entity type
- Configure table-specific mappings when different entities use different ID field names
- Ensure ID fields contain unique values within their respective tables
- Use string IDs for best compatibility across different output formats
