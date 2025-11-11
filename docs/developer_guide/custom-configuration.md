# Custom Configuration

## Configuration Overview

### TransmogConfig Class

The `TransmogConfig` class contains all configuration parameters:

```python
import transmog as tm

config = tm.TransmogConfig(
    separator=".",
    cast_to_string=False,
    batch_size=2000,
    id_field="id",
    parent_field="parent_id",
)

result = tm.flatten(data, config=config)
```

### Configuration Parameters

**Naming (1 parameter):**

- `separator`: Character to separate nested field names (default: `"_"`)

**Processing (5 parameters):**

- `cast_to_string`: Convert all values to strings (default: `False`)
- `null_handling`: How to handle null and empty values - `NullHandling.SKIP` (default) or `NullHandling.INCLUDE`
- `array_mode`: How to handle arrays - `ArrayMode.SMART`, `SEPARATE`, `INLINE`,
  or `SKIP` (default: `SMART`)
- `batch_size`: Records to process at once (default: `1000`)
- `max_depth`: Maximum recursion depth (default: `100`)

**Metadata (3 parameters):**

- `id_field`: Field name for record IDs (default: `"_id"`)
- `parent_field`: Field name for parent references (default: `"_parent_id"`)
- `time_field`: Field name for timestamps (default: `"_timestamp"`, set to `None` to disable)

**ID Discovery (1 parameter):**

- `id_patterns`: List of field names to check for natural IDs (default: `None`)

**Deterministic IDs (2 parameters):**

- `deterministic_ids`: Generate deterministic IDs based on record content (default: `False`)
- `id_fields`: List of field names for composite deterministic IDs (default: `None`)

**Error Handling (1 parameter):**

- `recovery_mode`: RecoveryMode enum for error handling (default: `RecoveryMode.STRICT`)

## Factory Methods

```python
import transmog as tm

# Default: types preserved, optimized for analytics
config = tm.TransmogConfig()

# CSV: strings, includes empty/null values
config = tm.TransmogConfig.for_csv()

# Memory: small batches, minimal cache
config = tm.TransmogConfig.for_memory()

# Error-tolerant: skip malformed records
config = tm.TransmogConfig.error_tolerant()
```

## Configuration Examples

### Basic Configuration

```python
import transmog as tm

config = tm.TransmogConfig(
    separator=".",
    cast_to_string=False,
    batch_size=5000,
)

result = tm.flatten(data, config=config)
```

### Performance Configuration

```python
import transmog as tm

# Large datasets - customize batch size
config = tm.TransmogConfig(
    batch_size=10000,
)

# Memory-constrained environments
config = tm.TransmogConfig.for_memory()
```

### ID Management

```python
import transmog as tm

# Use existing ID field
config = tm.TransmogConfig(
    id_field="product_id",
    parent_field="category_id",
)

# Discover natural IDs from data
config = tm.TransmogConfig(
    id_patterns=["id", "uuid", "pk"],
)
```

### Array Handling

```python
import transmog as tm

# Smart mode - simple arrays inline, complex arrays extracted
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SMART)

# Extract all arrays to child tables
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SEPARATE)

# Keep all arrays as JSON strings
config = tm.TransmogConfig(array_mode=tm.ArrayMode.INLINE)

# Skip arrays
config = tm.TransmogConfig(array_mode=tm.ArrayMode.SKIP)
```

### Error Handling

```python
import transmog as tm

# Strict mode
config = tm.TransmogConfig(
    recovery_mode=tm.RecoveryMode.STRICT,
)

# Skip errors
config = tm.TransmogConfig(
    recovery_mode=tm.RecoveryMode.SKIP,
)
```

## Advanced Usage

### Using Custom Configuration

```python
import transmog as tm

config = tm.TransmogConfig(
    batch_size=5000,
)

result = tm.flatten(data, name="products", config=config)
```

### Streaming Configuration

```python
import transmog as tm

config = tm.TransmogConfig.for_memory()

tm.flatten_stream(
    large_data,
    "output/",
    output_format="parquet",
    config=config,
)
```

## Configuration Guidelines

### Batch Sizes

- Small datasets: 1000-2000
- Large datasets: 5000-10000
- Memory constrained: 100-500

### Array Modes

- `tm.ArrayMode.SMART`: Default behavior
- `tm.ArrayMode.SEPARATE`: All arrays in child tables
- `tm.ArrayMode.INLINE`: Arrays as JSON strings
- `tm.ArrayMode.SKIP`: Ignore arrays

### Error Strategies

- `tm.RecoveryMode.STRICT`: Fail on errors
- `tm.RecoveryMode.SKIP`: Continue processing, skip problematic records

### Output Format Optimization

```python
import transmog as tm

# CSV output
config = tm.TransmogConfig.for_csv()

# Parquet output
config = tm.TransmogConfig(batch_size=10000)
```
