# JSON Transform

Transmog provides powerful capabilities for transforming JSON data before processing. This feature allows you to modify, restructure, augment, or filter your data prior to extraction.

## Basic Usage

The JSON transform feature allows you to apply transformations to your JSON data using custom Python functions:

```python
import transmog as tm

def add_full_name(data):
    """Add a full_name field by combining first and last name."""
    if isinstance(data, dict) and 'first_name' in data and 'last_name' in data:
        data['full_name'] = f"{data['first_name']} {data['last_name']}"
    return data

# Create a processor with the transform
processor = tm.Processor(json_transform=add_full_name)

# Process data
data = {
    "id": "user123",
    "first_name": "John",
    "last_name": "Doe",
    "age": 30
}
result = processor.process(data, entity_name="user")

# The processed data will contain the new full_name field
print(result.get_main_table()[0]['full_name'])  # Output: John Doe
```

## Targeted Transforms with Path Specifications

For more targeted transformations, you can specify paths where the transform should be applied:

```python
import transmog as tm

def uppercase_name(data):
    """Convert name fields to uppercase."""
    if isinstance(data, dict) and 'name' in data:
        data['name'] = data['name'].upper()
    return data

# Apply the transform only to specific paths
processor = tm.Processor(
    json_transform=uppercase_name,
    json_transform_paths=["$.users[*]", "$.organizations[*]"]
)

# Process data
data = {
    "users": [
        {"id": "u1", "name": "alice"},
        {"id": "u2", "name": "bob"}
    ],
    "organizations": [
        {"id": "o1", "name": "acme corp"}
    ],
    "settings": {
        "name": "config",  # This won't be transformed
        "version": "1.0"
    }
}
result = processor.process(data, entity_name="data")

# Names in users and organizations are uppercase, but not in settings
users_table = result.get_child_table("data_users")
orgs_table = result.get_child_table("data_organizations")
print(users_table[0]['name'])  # Output: ALICE
print(orgs_table[0]['name'])   # Output: ACME CORP
```

## Transform Function Requirements

Transform functions must:

1. Accept a single argument (the data to transform)
2. Return the transformed data
3. Handle different data types appropriately (dicts, lists, etc.)
4. Avoid raising exceptions (handle edge cases gracefully)

## Chaining Multiple Transforms

You can chain multiple transforms together by using a list:

```python
import transmog as tm

def add_full_name(data):
    if isinstance(data, dict) and 'first_name' in data and 'last_name' in data:
        data['full_name'] = f"{data['first_name']} {data['last_name']}"
    return data

def calculate_metrics(data):
    if isinstance(data, dict) and 'sales' in data and isinstance(data['sales'], list):
        data['total_sales'] = sum(item.get('amount', 0) for item in data['sales'])
        data['num_transactions'] = len(data['sales'])
    return data

# Chain transforms in order of execution
processor = tm.Processor(
    json_transform=[add_full_name, calculate_metrics]
)

# Process data
data = {
    "id": "rep123",
    "first_name": "Jane",
    "last_name": "Smith",
    "sales": [
        {"id": "s1", "amount": 100},
        {"id": "s2", "amount": 250},
        {"id": "s3", "amount": 175}
    ]
}
result = processor.process(data, entity_name="sales_rep")

# The data now contains both transformations
main_table = result.get_main_table()
print(main_table[0]['full_name'])     # Output: Jane Smith
print(main_table[0]['total_sales'])   # Output: 525
print(main_table[0]['num_transactions'])  # Output: 3
```

## Path Specification Format

Transmog uses JSONPath syntax for specifying paths:

- `$` represents the root object
- `.property` accesses a property
- `[*]` accesses all items in an array
- `..property` accesses a property recursively at any level

Examples:

- `$.users[*]` - All items in the users array
- `$.users[*].addresses[*]` - All addresses for all users
- `$..name` - All name properties at any level in the document

## Common Use Cases

### Data Enrichment

```python
def enrich_user_data(data):
    if isinstance(data, dict) and 'email' in data:
        # Add domain from email
        email = data['email']
        data['email_domain'] = email.split('@')[-1] if '@' in email else None
        
        # Add user type based on domain
        if data.get('email_domain') == 'company.com':
            data['user_type'] = 'employee'
        else:
            data['user_type'] = 'customer'
    return data
```

### Data Cleaning

```python
def clean_data(data):
    if not isinstance(data, dict):
        return data
        
    # Remove null values
    for key in list(data.keys()):
        if data[key] is None:
            del data[key]
            
    # Trim whitespace from strings
    for key, value in data.items():
        if isinstance(value, str):
            data[key] = value.strip()
            
    return data
```

### Data Restructuring

```python
def restructure_address(data):
    if isinstance(data, dict) and 'address' in data:
        addr = data['address']
        if isinstance(addr, str):
            # Split a single address string into components
            parts = addr.split(',')
            if len(parts) >= 3:
                data['street'] = parts[0].strip()
                data['city'] = parts[1].strip()
                data['zip_code'] = parts[-1].strip()
        elif isinstance(addr, dict):
            # Flatten nested address structure
            for key, value in addr.items():
                data[f"address_{key}"] = value
            del data['address']
    return data
```

## Performance Considerations

- Keep transform functions efficient, especially for large datasets
- Consider using the `json_transform_paths` parameter to limit where transforms are applied
- For complex transformations, chain multiple simple transforms rather than one complex function
- Test your transformations on sample data before processing large datasets

## Best Practices

- Handle different data types gracefully in your transform functions
- Document your transform functions clearly
- Use descriptive names for transform functions
- Keep transforms focused on a single responsibility
- Test edge cases to ensure robust transformations 