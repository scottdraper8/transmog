# Error Handling

Two recovery strategies control error behavior during processing.

## Recovery Modes

### STRICT Mode (Default)

Stops processing on the first error:

```python
import transmog as tm

# STRICT mode (default)
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.STRICT)

malformed_data = [
    {"name": "Valid Record"},
    {"name": None, "invalid": ...},  # This will cause processing to stop
    {"name": "Never Processed"}
]

try:
    result = tm.flatten(malformed_data, config=config)
except tm.TransmogError as e:
    print(f"Processing failed: {e}")
```

### SKIP Mode

Continue processing, skipping problematic records:

```python
# SKIP mode - continues on errors
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.SKIP)

result = tm.flatten(malformed_data, config=config)

# Problematic records are skipped
print(len(result.main))  # Only valid records included
```

Skipped records do not emit log messages. Add custom logging if needed.

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

Raised when input data validation fails. Available as `tm.ValidationError`.

```python
# Invalid data type
invalid_data = "not a dict or list"

try:
    result = tm.flatten(invalid_data)
except tm.ValidationError as e:
    print(f"Validation error: {e}")
```

### ProcessingError

Raised when record processing fails. Not exported; catch using `tm.TransmogError`.

### ConfigurationError

Raised when configuration is invalid. Not exported; handled during initialization.

### OutputError

Raised when writing output fails. Not exported.

## Custom Logging

Skip mode does not emit log messages. Wrap processing for custom logging:

```python
import logging

records = load_records()
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.SKIP)
result = tm.flatten(records, config=config)

if len(result.main) != len(records):
    logging.warning("Omitted %s records", len(records) - len(result.main))
```

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

# Use skip mode to continue processing
config = tm.TransmogConfig(
    id_generation="natural",
    id_field="id",
    recovery_mode=tm.RecoveryMode.SKIP
)
```

### Malformed JSONL

```python
# File with invalid JSON on line 2
config = tm.TransmogConfig(recovery_mode=tm.RecoveryMode.SKIP)
result = tm.flatten("malformed.jsonl", config=config)
# Processes valid lines, skips invalid lines
```
