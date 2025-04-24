# Data Transforms

Transmog offers powerful data transformation capabilities that allow you to modify, enrich, clean, and restructure your data during processing. This guide explains the various transform options available and how to use them effectively.

## Transform Types

Transmog supports several types of transforms:

1. **Global Transforms** - Applied to the entire data structure
2. **Path-Specific Transforms** - Applied only to values at specific paths
3. **Field Mappings** - Transform data while mapping between field names
4. **Type Transforms** - Convert values to specific data types
5. **Multi-Stage Transforms** - Chain multiple transforms together

## Global Transforms

Global transforms are functions that process the entire data structure:

```python
import transmog as tm

def add_metadata(data):
    """Add metadata fields to the record."""
    if isinstance(data, dict):
        data["processed_at"] = "2023-06-15"
        data["version"] = "1.0"
    return data

# Create a processor with the transform
processor = tm.Processor(transforms=[add_metadata])

# Process data
result = processor.process({"id": "123", "name": "Test"}, entity_name="item")

# Output will include the added metadata fields
```

### Global Transform Function Requirements

Global transform functions must:

1. Accept a single parameter (the data to transform)
2. Return the transformed data
3. Handle different data types appropriately (dicts, lists, etc.)
4. Be safe to apply to any part of the data structure

## Path-Specific Transforms

Path transforms are applied only to values that match specific paths:

```python
import transmog as tm

def capitalize_name(value):
    """Capitalize a name string."""
    if isinstance(value, str):
        return value.title()
    return value

# Create a processor with path-specific transforms
processor = tm.Processor(
    path_transforms={
        "$.name": capitalize_name,           # Apply to the "name" field
        "$.users[*].name": capitalize_name,  # Apply to all user names
    }
)

# Process data with path-specific transforms
data = {
    "name": "main account",
    "users": [
        {"id": 1, "name": "john doe"},
        {"id": 2, "name": "jane smith"}
    ]
}
result = processor.process(data, entity_name="account")

# Output will have capitalized names:
# {
#   "name": "Main Account",
#   "users": [
#     {"id": 1, "name": "John Doe"},
#     {"id": 2, "name": "Jane Smith"}
#   ]
# }
```

### Path Transform Function Requirements

Path transform functions must:

1. Accept a single parameter (the value at the specified path)
2. Return the transformed value
3. Be safe to apply to any value of any type that may appear at the specified path

## Field Mappings

Field mappings transform data while also renaming fields:

```python
import transmog as tm
from transmog import FieldMap, DataType

# Define field mappings
field_maps = [
    # Simple rename: 'id' -> 'user_id'
    FieldMap(source_field="id", target_field="user_id", data_type=DataType.STRING),
    
    # Transform and rename: 'name' -> 'full_name'
    FieldMap(
        source_field="name",
        target_field="full_name",
        data_type=DataType.STRING,
        transform=lambda x: x.upper() if x else None,
    ),
    
    # Add default value: 'age' -> 'age_years'
    FieldMap(
        source_field="age",
        target_field="age_years",
        data_type=DataType.INTEGER,
        default_value=0,  # Use default value when null
    ),
    
    # Computed field - not directly mapped from source
    FieldMap(
        target_field="record_status",
        data_type=DataType.STRING,
        compute=lambda row: "Complete" if all(row.values()) else "Incomplete",
    ),
]

# Create processor with field mappings
processor = tm.Processor()

# Process data with field mappings
result = processor.process_csv(
    "input.csv",
    entity_name="users",
    field_maps=field_maps,
    null_values=["", "NULL", "N/A"]
)
```

### Field Map Options

A `FieldMap` can include:

- `source_field`: Original field name in source data
- `target_field`: New field name in output data
- `data_type`: Type to convert the value to (from `DataType` enum)
- `transform`: Function to transform the value
- `default_value`: Value to use if source field is missing or null
- `compute`: Function to compute a value based on the entire row (when no source field)

## Type Transforms

Type transforms convert values to specific data types:

```python
import transmog as tm
from datetime import datetime

def type_transformer(record):
    """Apply type conversions to specific fields."""
    # Convert string to datetime
    if "created_at" in record and isinstance(record["created_at"], str):
        try:
            record["created_at"] = datetime.fromisoformat(record["created_at"])
        except ValueError:
            record["created_at"] = None
    
    # Convert string to numeric
    if "score" in record and isinstance(record["score"], str):
        try:
            record["score"] = float(record["score"])
        except ValueError:
            record["score"] = None
    
    # Convert string to boolean
    if "active" in record and isinstance(record["active"], str):
        record["active"] = record["active"].lower() in ("true", "yes", "1", "t", "y")
    
    return record

# Create processor with type transformer
processor = tm.Processor(transforms=[type_transformer])
```

## Multi-Stage Transforms

You can chain multiple transforms together to apply them in sequence:

```python
import transmog as tm

def clean_data(data):
    """Remove null values and whitespace from strings."""
    if not isinstance(data, dict):
        return data
    
    # Remove null values
    clean_dict = {k: v for k, v in data.items() if v is not None}
    
    # Trim strings
    for key, value in clean_dict.items():
        if isinstance(value, str):
            clean_dict[key] = value.strip()
    
    return clean_dict

def add_calculated_fields(data):
    """Add calculated fields based on existing data."""
    if isinstance(data, dict):
        if "first_name" in data and "last_name" in data:
            data["full_name"] = f"{data['first_name']} {data['last_name']}"
        
        if "price" in data and "quantity" in data:
            try:
                data["total"] = float(data["price"]) * int(data["quantity"])
            except (ValueError, TypeError):
                # Handle conversion errors
                pass
    
    return data

# Chain transforms in processing order
processor = tm.Processor(
    transforms=[
        clean_data,           # First clean the data
        add_calculated_fields # Then add calculated fields
    ]
)
```

## Common Transform Patterns

### Data Cleaning

```python
def clean_data(data):
    """Clean data by removing nulls, trimming strings, etc."""
    if not isinstance(data, dict):
        return data
    
    result = {}
    for key, value in data.items():
        # Skip null values
        if value is None:
            continue
            
        # Trim strings
        if isinstance(value, str):
            value = value.strip()
            # Skip empty strings
            if not value:
                continue
                
        # Clean nested dictionaries
        if isinstance(value, dict):
            value = clean_data(value)
            
        # Clean lists
        if isinstance(value, list):
            value = [
                clean_data(item) if isinstance(item, dict) else item
                for item in value if item is not None
            ]
            
        result[key] = value
    
    return result
```

### Data Enrichment

```python
def enrich_data(data):
    """Add derived fields based on existing data."""
    if not isinstance(data, dict):
        return data
    
    # Add full name
    if "first_name" in data and "last_name" in data:
        if data["first_name"] and data["last_name"]:
            data["full_name"] = f"{data['first_name']} {data['last_name']}"
    
    # Extract domain from email
    if "email" in data and isinstance(data["email"], str) and "@" in data["email"]:
        data["email_domain"] = data["email"].split("@")[-1]
        
    # Categorize based on age
    if "age" in data and data["age"] is not None:
        try:
            age = int(data["age"])
            if age < 18:
                data["age_group"] = "minor"
            elif age < 65:
                data["age_group"] = "adult"
            else:
                data["age_group"] = "senior"
        except (ValueError, TypeError):
            pass
    
    return data
```

### Data Normalization

```python
def normalize_keys(data):
    """Normalize all dictionary keys to snake_case."""
    if not isinstance(data, dict):
        return data
    
    def to_snake_case(s):
        # Convert camelCase or PascalCase to snake_case
        import re
        s = re.sub(r'([A-Z])', r'_\1', s)
        return s.lower().strip('_')
    
    result = {}
    for key, value in data.items():
        # Normalize key
        new_key = to_snake_case(key)
        
        # Recursively normalize nested structures
        if isinstance(value, dict):
            value = normalize_keys(value)
        elif isinstance(value, list):
            value = [
                normalize_keys(item) if isinstance(item, dict) else item
                for item in value
            ]
            
        result[new_key] = value
    
    return result
```

## Best Practices

### Performance

- Keep transforms efficient, especially for large datasets
- Use path-specific transforms when possible to limit scope
- For complex transformations, chain multiple simple transforms
- Test performance with sample data before processing large datasets

### Error Handling

- Make transforms robust to unexpected data types
- Implement proper error handling in transform functions
- Validate output to ensure transforms produce expected results
- Use default values for calculated fields that might fail

### Maintainability

- Keep transforms simple and focused on a single responsibility
- Document each transform function clearly
- Use descriptive names for transform functions
- Write unit tests for complex transforms

## Example: Complete Transformation Pipeline

Here's an example of a complete transformation pipeline:

```python
import transmog as tm
from datetime import datetime

def clean_input(data):
    """Clean incoming data."""
    if not isinstance(data, dict):
        return data
    
    # Remove null values
    result = {k: v for k, v in data.items() if v is not None}
    
    # Trim strings
    for key, value in result.items():
        if isinstance(value, str):
            result[key] = value.strip()
    
    return result

def normalize_fields(data):
    """Normalize field values."""
    if not isinstance(data, dict):
        return data
    
    # Normalize emails to lowercase
    if "email" in data and isinstance(data["email"], str):
        data["email"] = data["email"].lower()
    
    # Normalize phone numbers (remove non-digits)
    if "phone" in data and isinstance(data["phone"], str):
        data["phone"] = ''.join(c for c in data["phone"] if c.isdigit())
    
    return data

def add_computed_fields(data):
    """Add computed fields based on existing data."""
    if not isinstance(data, dict):
        return data
    
    # Add timestamp
    data["processed_at"] = datetime.now().isoformat()
    
    # Calculate age from birthdate
    if "birthdate" in data and isinstance(data["birthdate"], str):
        try:
            birthdate = datetime.fromisoformat(data["birthdate"])
            today = datetime.now()
            age = today.year - birthdate.year - (
                (today.month, today.day) < (birthdate.month, birthdate.day)
            )
            data["age"] = age
        except ValueError:
            # Handle invalid date format
            pass
    
    return data

def categorize_customer(value):
    """Categorize customer based on purchase value."""
    if not isinstance(value, (int, float)):
        return None
    
    if value >= 1000:
        return "platinum"
    elif value >= 500:
        return "gold"
    elif value >= 100:
        return "silver"
    else:
        return "bronze"

# Create processor with multi-stage transformation
processor = tm.Processor(
    # Global transforms applied in sequence
    transforms=[
        clean_input,
        normalize_fields,
        add_computed_fields
    ],
    # Path-specific transforms
    path_transforms={
        "$.total_purchases": categorize_customer
    },
    # Set options for all transforms
    transform_options={
        "validate_output": True,
        "max_depth": 5,
        "ignore_errors": False
    }
)

# Process data with the complete pipeline
result = processor.process(
    {
        "id": "CUST-001",
        "first_name": "  John  ",
        "last_name": "Doe",
        "email": "JOHN@example.com",
        "phone": "(555) 123-4567",
        "birthdate": "1985-06-15",
        "total_purchases": 750,
        "notes": None  # This will be removed
    }, 
    entity_name="customer"
)

# The output will be fully transformed data with:
# - Cleaned input (trimmed strings, removed nulls)
# - Normalized fields (lowercase email, numeric-only phone)
# - Added computed fields (processed_at timestamp, age from birthdate)
# - Categorized customer based on total purchases (gold tier)
```

## Advanced Techniques

### Dynamic Transforms

You can create transforms that adapt based on the data:

```python
def dynamic_transform(data):
    """Apply different transforms based on data type."""
    if not isinstance(data, dict):
        return data
    
    # Apply different logic based on record type
    if "type" in data:
        if data["type"] == "user":
            # Apply user-specific transformations
            if "name" in data:
                data["name"] = data["name"].title()
                
        elif data["type"] == "product":
            # Apply product-specific transformations
            if "price" in data and isinstance(data["price"], (int, float)):
                # Add tax calculation
                data["price_with_tax"] = data["price"] * 1.2
    
    return data
```

### Conditional Transforms

You can apply transforms conditionally:

```python
def conditional_transform(data):
    """Apply transforms only if certain conditions are met."""
    if not isinstance(data, dict):
        return data
    
    # Only transform active records
    if data.get("status") == "active":
        # Apply enrichment
        if "score" in data and isinstance(data["score"], (int, float)):
            if data["score"] >= 90:
                data["tier"] = "premium"
            elif data["score"] >= 70:
                data["tier"] = "standard"
            else:
                data["tier"] = "basic"
    
    return data
```

### Custom Transform Context

You can pass context to transforms for more complex scenarios:

```python
import transmog as tm

def transform_with_context(data, context=None):
    """Use context information in the transform."""
    if not isinstance(data, dict) or not context:
        return data
    
    # Use context values
    if "config" in context:
        config = context["config"]
        
        # Apply transforms based on configuration
        if config.get("add_timestamps", False) and "created" not in data:
            from datetime import datetime
            data["created"] = datetime.now().isoformat()
            
        # Use environment-specific transforms
        env = config.get("environment", "dev")
        if env == "prod":
            # Apply production-specific transforms
            if "sensitive_data" in data:
                del data["sensitive_data"]
    
    return data

# Create processor with context
processor = tm.Processor(
    transforms=[transform_with_context],
    transform_context={
        "config": {
            "add_timestamps": True,
            "environment": "prod"
        }
    }
)
``` 