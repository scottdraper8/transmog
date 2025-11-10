# Custom Configuration

## Configuration Overview

### TransmogConfig Class

The `TransmogConfig` class contains all configuration parameters:

```python
import transmog as tm

config = tm.TransmogConfig(
    separator=".",
    nested_threshold=5,
    cast_to_string=False,
    batch_size=2000,
    id_field="id",
    parent_field="parent_id",
)

result = tm.flatten(data, config=config)
```

### Configuration Parameters

**Naming (2 parameters):**

- `separator`: Character to separate nested field names (default: `"_"`)
- `nested_threshold`: Depth at which to simplify deeply nested names (default: `4`)

**Processing (6 parameters):**

- `cast_to_string`: Convert all values to strings (default: `False`)
- `include_empty`: Include empty values in output (default: `False`)
- `skip_null`: Skip null values (default: `True`)
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

**Error Handling (2 parameters):**

- `recovery_mode`: RecoveryMode enum for error handling (default: `RecoveryMode.STRICT`)
- `allow_malformed_data`: Allow malformed data (default: `False`)

**Cache (1 parameter):**

- `cache_size`: Maximum cache size for value processing optimizations, set to 0 to disable (default: `10000`)

**Advanced (1 parameter):**

- `id_generator`: Custom function for ID generation (default: `None`)

## Factory Methods

```python
import transmog as tm

# Default: types preserved, optimized for Parquet/analytics
config = tm.TransmogConfig()

# CSV: strings, includes empty/null values
config = tm.TransmogConfig.for_csv()

# Memory: small batches, minimal cache
config = tm.TransmogConfig.for_memory()

# Performance: large batches, extended cache for large datasets
config = tm.TransmogConfig.for_parquet()

# Simple: clean field names (id, parent_id, timestamp)
config = tm.TransmogConfig.simple()

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

# Large datasets
config = tm.TransmogConfig.for_parquet()

# Memory-constrained environments
config = tm.TransmogConfig.for_memory()

# Or customize from scratch
config = tm.TransmogConfig(
    batch_size=10000,
    cache_size=50000,
)
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
    allow_malformed_data=False,
)

# Skip errors
config = tm.TransmogConfig(
    recovery_mode=tm.RecoveryMode.SKIP,
    allow_malformed_data=True,
)
```

## Loading Configuration

### From File

```python
import transmog as tm

# Load from JSON file
config = tm.TransmogConfig.from_file("config.json")

# Load from YAML file
config = tm.TransmogConfig.from_file("config.yaml")

# Load from TOML file
config = tm.TransmogConfig.from_file("config.toml")
```

Example configuration file (`config.json`):

```json
{
    "separator": ".",
    "cast_to_string": false,
    "batch_size": 5000,
    "id_field": "id",
    "parent_field": "parent_id"
}
```

### From Environment Variables

```python
import transmog as tm

# Load from environment variables with TRANSMOG_ prefix
config = tm.TransmogConfig.from_env()
```

Environment variables:

```bash
export TRANSMOG_SEPARATOR="."
export TRANSMOG_BATCH_SIZE=5000
export TRANSMOG_CAST_TO_STRING=false
```

## Advanced Usage

### Using with Processor

```python
import transmog as tm
from transmog.process import Processor

config = tm.TransmogConfig(
    batch_size=5000,
    cache_size=50000,
)

processor = Processor(config)
result = processor.process(data, entity_name="products")
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
- `tm.RecoveryMode.SKIP`: Continue processing
- `tm.RecoveryMode.PARTIAL`: Process what's possible

### Output Format Optimization

```python
import transmog as tm

# CSV output
config = tm.TransmogConfig.for_csv()

# Parquet output
config = tm.TransmogConfig.for_parquet()
```
