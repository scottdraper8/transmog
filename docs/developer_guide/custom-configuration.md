# Custom Configuration

## Configuration Overview

### TransmogConfig Class

The `TransmogConfig` class contains all configuration parameters:

```python
from transmog import TransmogConfig, flatten

config = TransmogConfig(
    separator=".",
    nested_threshold=5,
    cast_to_string=False,
    batch_size=2000,
    id_field="id",
    parent_field="parent_id",
)

result = flatten(data, config=config)
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

- `recovery_strategy`: `"strict"`, `"skip"`, or `"partial"` (default: `"strict"`)
- `allow_malformed_data`: Allow malformed data (default: `False`)

**Cache (1 parameter):**

- `cache_size`: Maximum cache size, set to 0 to disable (default: `10000`)

**Advanced (1 parameter):**

- `id_generator`: Custom function for ID generation (default: `None`)

## Factory Methods

```python
# Default: types preserved, optimized for Parquet/analytics
result = tm.flatten(data)

# CSV: strings, includes empty/null values
config = TransmogConfig.for_csv()

# Memory: small batches, minimal cache
config = TransmogConfig.for_memory()

# Performance: large batches, extended cache
config = TransmogConfig.for_performance()

# Simple: clean field names (id, parent_id, timestamp)
config = TransmogConfig.simple()

# Error-tolerant: skip malformed records
config = TransmogConfig.error_tolerant()
```

## Configuration Examples

### Basic Configuration

```python
from transmog import TransmogConfig, flatten

config = TransmogConfig(
    separator=".",
    cast_to_string=False,
    batch_size=5000,
)

result = flatten(data, config=config)
```

### Performance Configuration

```python
# Large datasets
config = TransmogConfig.for_performance()

# Memory-constrained environments
config = TransmogConfig.for_memory()

# Or customize from scratch
config = TransmogConfig(
    batch_size=10000,
    cache_size=50000,
)
```

### ID Management

```python
# Use existing ID field
config = TransmogConfig(
    id_field="product_id",
    parent_field="category_id",
)

# Discover natural IDs from data
config = TransmogConfig(
    id_patterns=["id", "uuid", "pk"],
)
```

### Array Handling

```python
from transmog.types import ArrayMode

# Smart mode - simple arrays inline, complex arrays extracted
config = TransmogConfig(array_mode=ArrayMode.SMART)

# Extract all arrays to child tables
config = TransmogConfig(array_mode=ArrayMode.SEPARATE)

# Keep all arrays as JSON strings
config = TransmogConfig(array_mode=ArrayMode.INLINE)

# Skip arrays
config = TransmogConfig(array_mode=ArrayMode.SKIP)
```

### Error Handling

```python
# Strict mode
config = TransmogConfig(
    recovery_strategy="strict",
    allow_malformed_data=False,
)

# Skip errors
config = TransmogConfig(
    recovery_strategy="skip",
    allow_malformed_data=True,
)
```

## Loading Configuration

### From File

```python
# Load from JSON file
config = TransmogConfig.from_file("config.json")

# Load from YAML file
config = TransmogConfig.from_file("config.yaml")

# Load from TOML file
config = TransmogConfig.from_file("config.toml")
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
# Load from environment variables with TRANSMOG_ prefix
config = TransmogConfig.from_env()
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
from transmog.process import Processor

config = TransmogConfig(
    batch_size=5000,
    cache_size=50000,
)

processor = Processor(config)
result = processor.process(data, entity_name="products")
```

### Streaming Configuration

```python
from transmog import flatten_stream

config = TransmogConfig.for_memory()

flatten_stream(
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

- `SMART`: Default behavior
- `SEPARATE`: All arrays in child tables
- `INLINE`: Arrays as JSON strings
- `SKIP`: Ignore arrays

### Error Strategies

- `strict`: Fail on errors
- `skip`: Continue processing
- `partial`: Process what's possible

### Output Format Optimization

```python
# CSV output
config = TransmogConfig.for_csv()

# Parquet output
config = TransmogConfig.for_parquet()
```
