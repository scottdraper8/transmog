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

# Create a processor that uses natural IDs
processor = tm.Processor.with_natural_ids()

# Process data as usual
result = processor.process(data, entity_name="company")
```

With natural ID discovery enabled, Transmog will:

1. Look for common ID fields in each record (like "id", "uuid", "code", etc.)
2. Use the first valid ID field found instead of generating a synthetic ID
3. Fall back to generating a synthetic ID if no suitable ID field is found

## Customizing ID Field Detection

### Custom ID Field Patterns

You can provide custom patterns to check for ID fields:

```python
# Define custom patterns to check for ID fields
custom_patterns = ["id", "ID", "sku", "employee_id", "product_code"]

processor = tm.Processor.with_natural_ids(id_field_patterns=custom_patterns)
```

The patterns are checked in order, and the first matching field with a valid value is used.

### Table-Specific ID Fields

Different tables often use different field names for IDs. You can map specific tables to their ID fields:

```python
# Map specific tables to their ID fields
id_mapping = {
    "company_departments_employees": "employee_id",
    "company_products": "sku",
    "*": "id",  # Default for all other tables
}

processor = tm.Processor.with_natural_ids(id_field_mapping=id_mapping)
```

The `"*"` key acts as a wildcard default for tables not explicitly mapped.

## ID Field Selection Rules

When determining which field to use as an ID, Transmog follows these rules:

1. Check table-specific mapping if provided
2. Check wildcard mapping (`"*"`) if provided
3. Check each pattern in the ID field patterns list
4. Fall back to generating a synthetic ID if no suitable field is found

A field is considered suitable if:

- It exists in the record
- Its value is not null/None
- Its value is a scalar type (string, number)
- If it's a string, it's not empty

## Parent-Child Relationships

When using natural IDs, parent-child relationships are maintained using the natural IDs:

```python
# With natural ID discovery
result = processor.with_natural_ids().process(data, entity_name="company")

# Child records will reference parent's natural ID
dept = result.child_tables["company_departments"][0]
print(dept["__parent_transmog_id"])  # Will contain parent's natural ID
```

## Mixed ID Fields

Transmog handles records with mixed ID field presence gracefully:

- Records with natural IDs use those IDs
- Records without natural IDs get synthetic IDs
- All records maintain proper parent-child relationships

## Forcing Synthetic IDs

To revert to the default behavior of always adding synthetic IDs:

```python
# Default processor always adds transmog IDs
processor = tm.Processor()
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
    "company_departments_employees": "employee_id",
    "*": "id",  # Default for all other tables
}

# Process with natural ID discovery
processor = tm.Processor.with_natural_ids(id_field_mapping=id_mapping)
result = processor.process(data, entity_name="company")

# Export results
result.write_all_json("output/natural_ids")
```

## Best Practices

- Use consistent ID field names within each entity type
- Configure table-specific mappings when different entities use different ID field names
- Ensure ID fields contain unique values within their respective tables
- Use string IDs for best compatibility across different output formats
