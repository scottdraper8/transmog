# Error Handling

Transmog raises exceptions when errors occur during processing. All exceptions inherit from `TransmogError`.

## Error Types

### TransmogError

Base exception for all Transmog errors. Available as `tm.TransmogError`.

```python
try:
    result = tm.flatten(data)
except tm.TransmogError as e:
    print(f"Transmog error: {e}")
```

### ValidationError

Raised when input data validation or processing fails. Available as `tm.ValidationError`.

```python
# Invalid data type
invalid_data = "not a dict or list"

try:
    result = tm.flatten(invalid_data)
except tm.ValidationError as e:
    print(f"Validation error: {e}")
```

### ConfigurationError

Raised when configuration is invalid. Not exported; handled during initialization.

### OutputError

Raised when writing output fails. Not exported.

## Custom Error Handling

```python
def safe_flatten(data, **kwargs):
    try:
        return tm.flatten(data, **kwargs)
    except tm.ValidationError as e:
        logging.warning("Invalid data: %s", e)
        return None
    except tm.TransmogError as e:
        logging.error("Processing failed: %s", e)
        return None
```

## Examples

### Missing Natural IDs

```python
config = tm.TransmogConfig(id_generation="natural", id_field="id")
data = {"name": "Product"}  # Missing 'id'

try:
    result = tm.flatten(data, config=config)
except tm.TransmogError as e:
    print(f"Error: {e}")
```

### Malformed JSONL

```python
# File with invalid JSON on line 2
try:
    result = tm.flatten("malformed.jsonl")
except tm.TransmogError as e:
    print(f"Error processing file: {e}")
```
