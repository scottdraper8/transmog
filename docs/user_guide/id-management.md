# ID Management

This guide covers how Transmog handles record identification, including automatic ID generation,
natural ID fields, and relationship management.

## ID System Overview

Transmog uses ID fields to:

- Uniquely identify each record
- Link child records to their parents
- Maintain data relationships during transformation
- Enable data reconstruction and joining

## Automatic ID Generation

By default, Transmog generates unique IDs for all records:

```python
import transmog as tm

data = {
    "product": {
        "name": "Laptop",
        "reviews": [
            {"rating": 5, "comment": "Great"},
            {"rating": 4, "comment": "Good"}
        ]
    }
}

result = tm.flatten(data, name="products")

# Automatic IDs are generated
print("Main record:", result.main[0])
# {'product_name': 'Laptop', '_id': 'generated_unique_id'}

print("Review records:", result.tables["products_reviews"])
# [
#   {'rating': '5', 'comment': 'Great', '_parent_id': 'generated_unique_id'},
#   {'rating': '4', 'comment': 'Good', '_parent_id': 'generated_unique_id'}
# ]
```

### ID Field Names

Default ID field names can be customized:

```python
# Custom parent ID field name
result = tm.flatten(
    data,
    name="products",
    parent_id_field="parent_ref"
)

# Child records use custom parent field name
print(result.tables["products_reviews"][0])
# {'rating': '5', 'comment': 'Great', 'parent_ref': 'generated_id'}
```

## Natural ID Fields

Existing ID fields in the data can be used instead of generated ones:

### Single ID Field

```python
data = {
    "product": {
        "product_id": "PROD123",
        "name": "Gaming Laptop",
        "reviews": [
            {"review_id": "REV456", "rating": 5},
            {"review_id": "REV789", "rating": 4}
        ]
    }
}

# Use existing product_id field
result = tm.flatten(data, name="products", id_field="product_id")

print("Main record:", result.main[0])
# {'product_id': 'PROD123', 'product_name': 'Gaming Laptop', '_id': 'PROD123'}

print("Review records:", result.tables["products_reviews"])
# [
#   {'review_id': 'REV456', 'rating': '5', '_parent_id': 'PROD123'},
#   {'review_id': 'REV789', 'rating': '4', '_parent_id': 'PROD123'}
# ]
```

### Table-Specific ID Fields

Different tables can use different ID fields:

```python
data = {
    "company": {
        "company_id": "COMP123",
        "name": "TechCorp",
        "employees": [
            {"employee_id": "EMP001", "name": "Alice"},
            {"employee_id": "EMP002", "name": "Bob"}
        ],
        "offices": [
            {"office_id": "OFF001", "city": "San Francisco"},
            {"office_id": "OFF002", "city": "New York"}
        ]
    }
}

# Different ID fields for different tables
result = tm.flatten(data, name="company", id_field={
    "": "company_id",                    # Main table uses company_id
    "company_employees": "employee_id",   # Employee table uses employee_id
    "company_offices": "office_id"       # Office table uses office_id
})

print("Employee records:", result.tables["company_employees"])
# [
#   {'employee_id': 'EMP001', 'name': 'Alice', '_parent_id': 'COMP123', '_id': 'EMP001'},
#   {'employee_id': 'EMP002', 'name': 'Bob', '_parent_id': 'COMP123', '_id': 'EMP002'}
# ]
```

### Fallback to Generated IDs

When specified ID fields are missing, Transmog falls back to generated IDs:

```python
data = [
    {"product_id": "PROD123", "name": "Laptop"},     # Has ID field
    {"name": "Mouse"},                               # Missing ID field
    {"product_id": "PROD456", "name": "Keyboard"}    # Has ID field
]

result = tm.flatten(data, name="products", id_field="product_id")

# Records with missing ID fields get generated IDs
for record in result.main:
    print(f"Name: {record['name']}, ID: {record['_id']}")
# Name: Laptop, ID: PROD123
# Name: Mouse, ID: generated_id
# Name: Keyboard, ID: PROD456
```

## Relationship Management

### Parent-Child Links

Child records always reference their parent through the parent ID field:

```python
# Build parent-child relationship map
def map_relationships(result):
    parent_map = {}

    # Index main records by ID
    for record in result.main:
        parent_map[record["_id"]] = {
            "parent": record,
            "children": {}
        }

    # Map child records to parents
    for table_name, records in result.tables.items():
        for record in records:
            parent_id = record["_parent_id"]
            if parent_id in parent_map:
                if table_name not in parent_map[parent_id]["children"]:
                    parent_map[parent_id]["children"][table_name] = []
                parent_map[parent_id]["children"][table_name].append(record)

    return parent_map

relationships = map_relationships(result)
```

### Multi-Level Hierarchies

Nested arrays create multi-level ID relationships:

```python
data = {
    "company": {
        "company_id": "COMP123",
        "departments": [
            {
                "dept_id": "DEPT001",
                "name": "Engineering",
                "teams": [
                    {"team_id": "TEAM001", "name": "Frontend"},
                    {"team_id": "TEAM002", "name": "Backend"}
                ]
            }
        ]
    }
}

result = tm.flatten(data, name="company", id_field={
    "": "company_id",
    "company_departments": "dept_id",
    "company_departments_teams": "team_id"
})

# Three-level hierarchy
print("Company:", result.main[0]["_id"])           # COMP123
print("Department:", result.tables["company_departments"][0]["_id"])  # DEPT001
print("Team parent:", result.tables["company_departments_teams"][0]["_parent_id"])  # DEPT001
```

## ID Generation Strategies

### Deterministic IDs

Natural IDs provide deterministic, reproducible results:

```python
# Same data with same natural IDs produces identical results
data1 = {"product_id": "PROD123", "name": "Laptop"}
data2 = {"product_id": "PROD123", "name": "Laptop"}

result1 = tm.flatten(data1, name="products", id_field="product_id")
result2 = tm.flatten(data2, name="products", id_field="product_id")

# IDs are deterministic
assert result1.main[0]["_id"] == result2.main[0]["_id"]
print("Deterministic ID:", result1.main[0]["_id"])  # PROD123
```

### Generated ID Consistency

Generated IDs are unique within a processing session but not across sessions:

```python
# Generated IDs are unique but not deterministic across runs
result1 = tm.flatten(data, name="products")  # No id_field specified
result2 = tm.flatten(data, name="products")  # No id_field specified

# IDs will be different between runs
print("Run 1 ID:", result1.main[0]["_id"])
print("Run 2 ID:", result2.main[0]["_id"])
```

## ID Validation and Quality

### Missing ID Detection

Check for records with missing natural IDs:

```python
def check_id_coverage(result, expected_id_field):
    """Check how many records have natural vs generated IDs."""
    natural_ids = 0
    generated_ids = 0

    for record in result.main:
        if expected_id_field in record and record[expected_id_field]:
            natural_ids += 1
        else:
            generated_ids += 1

    return natural_ids, generated_ids

natural, generated = check_id_coverage(result, "product_id")
print(f"Natural IDs: {natural}, Generated IDs: {generated}")
```

### ID Uniqueness Validation

Verify ID uniqueness across tables:

```python
def validate_id_uniqueness(result):
    """Validate that all IDs are unique."""
    all_ids = set()
    duplicates = []

    # Check main table IDs
    for record in result.main:
        id_value = record["_id"]
        if id_value in all_ids:
            duplicates.append(id_value)
        all_ids.add(id_value)

    # Check child table IDs (if they have _id fields)
    for table_name, records in result.tables.items():
        for record in records:
            if "_id" in record:
                id_value = record["_id"]
                if id_value in all_ids:
                    duplicates.append(id_value)
                all_ids.add(id_value)

    return duplicates

duplicates = validate_id_uniqueness(result)
if duplicates:
    print(f"Duplicate IDs found: {duplicates}")
```

### Orphaned Record Detection

Check for child records without valid parents:

```python
def find_orphaned_records(result):
    """Find child records with invalid parent references."""
    main_ids = {record["_id"] for record in result.main}
    orphaned = {}

    for table_name, records in result.tables.items():
        table_orphans = []
        for record in records:
            parent_id = record["_parent_id"]
            if parent_id not in main_ids:
                table_orphans.append(record)

        if table_orphans:
            orphaned[table_name] = table_orphans

    return orphaned

orphaned = find_orphaned_records(result)
for table, records in orphaned.items():
    print(f"Orphaned records in {table}: {len(records)}")
```

## Advanced ID Scenarios

### Composite Natural IDs

When natural IDs are complex, use string concatenation:

```python
data = [
    {"region": "US", "store": "001", "product": "laptop"},
    {"region": "EU", "store": "002", "product": "mouse"}
]

# Create composite ID from multiple fields
def create_composite_id(record):
    return f"{record.get('region', '')}_{record.get('store', '')}_{record.get('product', '')}"

# Pre-process data to add composite ID
for record in data:
    record["composite_id"] = create_composite_id(record)

result = tm.flatten(data, name="sales", id_field="composite_id")
print("Composite IDs:", [r["_id"] for r in result.main])
# ['US_001_laptop', 'EU_002_mouse']
```

### Conditional ID Fields

Use different ID fields based on data conditions:

```python
def determine_id_field(data):
    """Determine appropriate ID field based on data structure."""
    if isinstance(data, list) and data:
        sample = data[0]
        if "primary_id" in sample:
            return "primary_id"
        elif "id" in sample:
            return "id"
        elif "uuid" in sample:
            return "uuid"
    return None  # Use generated IDs

# Determine ID field dynamically
id_field = determine_id_field(data)
result = tm.flatten(data, name="records", id_field=id_field)
```

## Metadata Enhancement

### Timestamp Addition

Add processing timestamps to records:

```python
# Add timestamps for audit trails
result = tm.flatten(
    data,
    name="events",
    id_field="event_id",
    add_timestamp=True
)

# Records include timestamp metadata
print("Record with timestamp:", result.main[0])
# {'event_id': 'EVT123', 'name': 'User Login', '_id': 'EVT123', '_timestamp': '2024-01-15T10:30:00'}
```

### Custom Metadata

Add custom metadata during processing:

```python
# Custom metadata can be added post-processing
result = tm.flatten(data, name="records", id_field="record_id")

# Add processing metadata
processing_info = {
    "processed_at": "2024-01-15T10:30:00",
    "version": "1.0",
    "source": "api_import"
}

for record in result.main:
    record.update(processing_info)
```

## Best Practices

### ID Field Selection

Choose ID fields based on requirements:

```python
# For reproducible results
result = tm.flatten(data, name="products", id_field="product_id")

# For simplicity when IDs don't matter
result = tm.flatten(data, name="products")  # Use generated IDs

# For complex scenarios with multiple entity types
result = tm.flatten(data, name="entities", id_field={
    "": "entity_id",
    "entities_children": "child_id",
    "entities_metadata": "meta_id"
})
```

### ID Validation Pipeline

Implement ID validation in data pipelines:

```python
def validate_and_process(data, id_config):
    """Validate IDs before processing."""
    # Pre-validation
    if id_config.get("required_fields"):
        missing = check_required_id_fields(data, id_config["required_fields"])
        if missing:
            raise ValueError(f"Missing required ID fields: {missing}")

    # Process data
    result = tm.flatten(data, name="validated", **id_config)

    # Post-validation
    duplicates = validate_id_uniqueness(result)
    if duplicates:
        raise ValueError(f"Duplicate IDs detected: {duplicates}")

    orphaned = find_orphaned_records(result)
    if orphaned:
        print(f"Warning: Orphaned records found in {list(orphaned.keys())}")

    return result
```

## Next Steps

- **[Output Formats](output-formats.md)** - Choose formats that preserve ID relationships
- **[Error Handling](error-handling.md)** - Handle ID-related processing errors
- **[Performance Guide](../developer_guide/performance.md)** - Optimize ID processing for large datasets
