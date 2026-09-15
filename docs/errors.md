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

A `pathlib.Path` that does not exist raises `ValidationError: File not found`.
A string ending in a data-file extension (`.json`, `.jsonl`, `.ndjson`,
`.json5`, `.hjson`) that is not an existing path also raises
`ValidationError: File not found`. Other strings are parsed as JSON.

### ConfigurationError

Raised when `TransmogConfig` receives invalid parameters (e.g., `batch_size < 1`,
invalid `id_generation` value). Available as `tm.ConfigurationError`.

```python
try:
    config = tm.TransmogConfig(batch_size=-1)
except tm.ConfigurationError as e:
    print(f"Invalid config: {e}")
```

### OutputError

Raised when writing output files fails (permissions, disk full, invalid
compression). Available as `tm.OutputError`.

```python
try:
    tm.flatten_stream(data, "output/", output_format="csv")
except tm.OutputError as e:
    print(f"Write failed: {e}")
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

try:
    result = tm.flatten(data, config=config)
except tm.ValidationError as e:
    print(f"Error: {e}")
```

### Malformed JSONL

```python
# File with invalid JSON on line 2
try:
    result = tm.flatten("malformed.jsonl")
except tm.ValidationError as e:
    print(f"Error processing file: {e}")
```

## Troubleshooting

**Schema deviation warnings during streaming:**
When using `flatten_stream()`, each batch infers its own schema. If schemas
differ across parts, a `UserWarning` is emitted and details are written to
`_schema_log.json`. Pass `coerce_schema=True` to unify schemas at close time.
See [Schema Drift Tracking](outputs.md#schema-drift-tracking).

**ConfigurationError on invalid config:**

```python
try:
    config = tm.TransmogConfig(batch_size=-1)
except tm.ConfigurationError as e:
    print(f"Invalid config: {e}")
```

**Deep or circular structures:**
`max_depth` (default 100) omits the entire subtree below that depth and emits
a `UserWarning` naming the path where truncation occurred. Circular references
do not raise; they unroll until `max_depth` is reached.
