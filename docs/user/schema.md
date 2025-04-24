# Schema Definition and Validation

Transmogrify provides a flexible schema system that allows you to define the structure of your data, apply validations, and enforce constraints. This guide explains how to leverage schemas for more controlled and predictable data processing.

## Basic Schema Definition

Schemas in Transmogrify are defined using a dictionary-like structure where keys represent field names and values define field properties:

```python
import transmogrify as tm

# Define a schema
user_schema = {
    "id": {"type": "string", "required": True},
    "name": {"type": "string", "required": True},
    "age": {"type": "integer", "required": False, "min": 0},
    "email": {"type": "string", "pattern": r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"},
    "tags": {"type": "array"}
}

# Create a processor with the schema
processor = tm.Processor(schema=user_schema)

# Process data that conforms to the schema
valid_data = {
    "id": "user123",
    "name": "John Doe",
    "age": 30,
    "email": "john@example.com",
    "tags": ["customer", "active"]
}

result = processor.process(valid_data, entity_name="user")
```

## Field Types and Validation

Transmogrify supports various field types and validation rules:

### Basic Types

- `string`: For text data
- `integer`: For whole numbers
- `number`: For any numeric value (float or integer)
- `boolean`: For true/false values
- `array`: For lists/arrays
- `object`: For nested objects/dictionaries
- `null`: For null/None values
- `any`: For any type of value

### Common Field Properties

- `required`: Whether the field must be present (default: `False`)
- `nullable`: Whether the field can be null (default: `True`)
- `default`: Default value to use if field is missing
- `description`: Human-readable description of the field

### Type-Specific Validations

#### String Validations

```python
"username": {
    "type": "string",
    "min_length": 3,        # Minimum length
    "max_length": 20,       # Maximum length
    "pattern": r"^[a-z0-9_]+$",  # Regex pattern
    "enum": ["admin", "user", "guest"]  # Allowed values
}
```

#### Numeric Validations

```python
"quantity": {
    "type": "integer",
    "min": 1,               # Minimum value
    "max": 100,             # Maximum value
    "multiple_of": 5        # Must be multiple of 5
}
```

#### Array Validations

```python
"tags": {
    "type": "array",
    "min_items": 1,         # Minimum number of items
    "max_items": 10,        # Maximum number of items
    "unique_items": True,   # No duplicate items
    "items": {"type": "string"}  # Each item must be a string
}
```

#### Object Validations

```python
"address": {
    "type": "object",
    "properties": {
        "street": {"type": "string", "required": True},
        "city": {"type": "string", "required": True},
        "zip": {"type": "string", "pattern": r"^\d{5}(-\d{4})?$"}
    }
}
```

## Nested Schemas

You can define schemas for nested objects and arrays:

```python
order_schema = {
    "id": {"type": "string", "required": True},
    "customer": {
        "type": "object",
        "properties": {
            "id": {"type": "string", "required": True},
            "name": {"type": "string", "required": True},
            "email": {"type": "string", "pattern": r"^.+@.+\..+$"}
        }
    },
    "items": {
        "type": "array",
        "items": {
            "type": "object",
            "properties": {
                "product_id": {"type": "string", "required": True},
                "quantity": {"type": "integer", "min": 1, "required": True},
                "price": {"type": "number", "min": 0, "required": True}
            }
        }
    },
    "total": {"type": "number", "min": 0}
}

processor = tm.Processor(schema=order_schema)
```

## Schema Options

When creating a processor, you can configure how schema validation behaves:

```python
processor = tm.Processor(
    schema=my_schema,
    schema_options={
        "strict": True,           # Reject unknown fields
        "fail_on_error": True,    # Raise exception on validation failure
        "coerce_types": True      # Attempt to convert types when possible
    }
)
```

### Available Schema Options

- `strict` (default: `False`): When true, rejects any fields not defined in the schema
- `fail_on_error` (default: `True`): When true, raises a ValidationError for invalid data; when false, adds errors to the result
- `coerce_types` (default: `True`): When true, attempts to convert values to the correct type (e.g., string to int)
- `allow_unknown` (default: `True`): Synonym for `not strict`, allows fields not in schema

## Field Type Coercion

When `coerce_types` is enabled, Transmogrify will attempt to convert values to the correct type:

```python
schema = {
    "id": {"type": "integer"},
    "active": {"type": "boolean"}
}

data = {
    "id": "123",      # String that can be converted to integer
    "active": "true"  # String that can be converted to boolean
}

processor = tm.Processor(schema=schema, schema_options={"coerce_types": True})
result = processor.process(data, entity_name="item")

# Result will have id as integer (123) and active as boolean (True)
```

### Coercion Rules

- `string` → `integer`/`number`: String must be valid numeric representation
- `string` → `boolean`: "true"/"false" (case-insensitive) or "1"/"0"
- `integer` → `string`: Simple string conversion of the number
- `number` → `string`: Simple string conversion of the number
- `string` → `array`: String is split by comma (`,`) by default

## Handling Validation Errors

When validation fails and `fail_on_error` is `True`, a `ValidationError` is raised:

```python
import transmogrify as tm
from transmogrify.exceptions import ValidationError

schema = {
    "age": {"type": "integer", "min": 0, "required": True}
}

processor = tm.Processor(schema=schema)

try:
    # This will fail because age is negative
    result = processor.process({"age": -5}, entity_name="person")
except ValidationError as e:
    print(f"Validation failed: {e}")
    # Access details about the error
    for error in e.errors:
        print(f"Field: {error.field}, Error: {error.message}")
```

When `fail_on_error` is `False`, errors are included in the processing result:

```python
processor = tm.Processor(schema=schema, schema_options={"fail_on_error": False})
result = processor.process({"age": -5}, entity_name="person")

if result.has_errors():
    print("Validation errors occurred:")
    for error in result.get_errors():
        print(f"Field: {error.field}, Error: {error.message}")
```

## Custom Validation Functions

You can define custom validation functions for fields:

```python
def validate_even_number(field, value, error):
    if value % 2 != 0:
        error(field, "Must be an even number")

schema = {
    "count": {
        "type": "integer",
        "validator": validate_even_number
    }
}

processor = tm.Processor(schema=schema)
```

Custom validators should accept three parameters:
1. `field`: The name of the field being validated
2. `value`: The value to validate
3. `error`: A function to call to record validation errors

## Schema Inheritance and Reuse

You can build more complex schemas by combining and extending existing ones:

```python
# Base schema
address_schema = {
    "street": {"type": "string", "required": True},
    "city": {"type": "string", "required": True},
    "state": {"type": "string", "required": True},
    "zip": {"type": "string", "required": True}
}

# Schemas that use the base schema
user_schema = {
    "id": {"type": "string", "required": True},
    "name": {"type": "string", "required": True},
    "billing_address": {
        "type": "object",
        "properties": address_schema
    },
    "shipping_address": {
        "type": "object",
        "properties": address_schema
    }
}
```

## Performance Considerations

- Schema validation adds some processing overhead
- For large datasets, consider using simpler schemas or disabling validation for well-known data sources
- Use `strict: False` when you don't need to validate every field

## Best Practices

- Start with less restrictive schemas and tighten as needed
- Always provide descriptive error messages for custom validators
- Use schemas to document your data structures
- Consider breaking complex schemas into reusable components
- Test your schemas with both valid and invalid data to ensure they behave as expected 