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

### MissingDependencyError

Raised when an optional dependency is missing. Available as `tm.MissingDependencyError`.

```python
try:
    result.save("output.parquet")
except tm.MissingDependencyError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install pyarrow")
```

### ConfigurationError

Raised when `TransmogConfig` receives invalid parameters (e.g., `batch_size < 1`,
invalid `id_generation` value). Not exported in the public API — catch using
`TransmogError` as the base class.

### OutputError

Raised when writing output files fails (e.g., permission errors, disk full).
Not exported in the public API — catch using `TransmogError` as the base class.

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

### Missing Optional Dependency

```python
try:
    tm.flatten_stream(data, "output/", output_format="avro")
except tm.MissingDependencyError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install fastavro cramjam")
```

## Troubleshooting

### Common Errors

**"Missing dependency" when saving Parquet/ORC:**
Install PyArrow: `pip install pyarrow`

**"Missing dependency" when saving Avro:**
Install fastavro and cramjam: `pip install fastavro cramjam`

**Schema drift error during Avro streaming:**
When using `flatten_stream()` with Avro output, the schema is locked after the
first batch. If later batches contain fields not present in the first batch, a
schema drift error is raised. Ensure input data has a consistent structure, or
process a representative sample first to establish the schema.

**ConfigurationError on invalid config:**
Catch using `TransmogError` since `ConfigurationError` is not exported:

```python
try:
    config = tm.TransmogConfig(batch_size=-1)
except tm.TransmogError as e:
    print(f"Invalid config: {e}")
```
