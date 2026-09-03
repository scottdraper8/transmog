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
A string is treated as a file path only if that path exists; otherwise it is
parsed as JSON. `tm.flatten("data.json")` when the file is missing raises
`ValidationError: Error parsing JSON data`, not file-not-found.

### MissingDependencyError

Raised when a writer dependency is not importable. Available as
`tm.MissingDependencyError`. PyArrow and fastavro are included in the default
install, so this is uncommon unless those packages were removed.

```python
try:
    result.save("output.parquet")
except tm.MissingDependencyError as e:
    print(f"Missing dependency: {e}")
```

### ConfigurationError

Raised when `TransmogConfig` receives invalid parameters (e.g., `batch_size < 1`,
invalid `id_generation` value). Not exported on `tm`; import from
`transmog.exceptions` or catch `TransmogError`.

```python
from transmog.exceptions import ConfigurationError

try:
    config = tm.TransmogConfig(batch_size=-1)
except ConfigurationError as e:
    print(f"Invalid config: {e}")
```

### OutputError

Raised when writing output files fails (permissions, disk full, invalid
compression). Not exported on `tm`; import from `transmog.exceptions` or catch
`TransmogError`.

```python
from transmog.exceptions import OutputError

try:
    tm.flatten_stream(data, "output/", output_format="csv")
except (OutputError, tm.TransmogError) as e:
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
Catch `ConfigurationError` from `transmog.exceptions`, or `TransmogError`:

```python
try:
    config = tm.TransmogConfig(batch_size=-1)
except tm.TransmogError as e:
    print(f"Invalid config: {e}")
```

**Deep or circular structures:**
`max_depth` (default 100) silently omits the entire subtree below that depth.
Circular references do not raise; they unroll until `max_depth` is reached.
There is no warning or log when truncation occurs.
