# Configuration

Transmog uses a configuration system based on the `TransmogConfig` class, which groups configuration
options into logical categories.

## Basic Configuration

Use the default configuration:

```python
import transmog as tm

# Use default configuration
processor = tm.Processor()
```

## Pre-configured Factory Methods

Factory methods for common configuration patterns:

```python
# Create with default configuration
processor = tm.Processor.default()

# Create with memory optimization for large datasets
processor = tm.Processor.memory_optimized()

# Create with performance optimization
processor = tm.Processor.performance_optimized()

# Create with deterministic ID generation
processor = tm.Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "user_orders": "id"           # Order records use "id" field
})

# Create with custom ID generation
def custom_id_strategy(record):
    return f"CUSTOM-{record['id']}"

processor = tm.Processor.with_custom_id_generation(custom_id_strategy)

# Create with partial recovery
processor = tm.Processor.with_partial_recovery()
```

## Configuration Object

Create a custom configuration object:

```python
# Create a custom configuration with the fluent API
config = (
    tm.TransmogConfig.default()
    .with_naming(
        separator=".",
        abbreviate_table_names=False,
        max_table_component_length=30
    )
    .with_processing(
        batch_size=5000,
        cast_to_string=True,
        include_empty=False
    )
    .with_metadata(
        id_field="custom_id",
        parent_field="parent_id",
        time_field="processed_at"
    )
    .with_error_handling(
        recovery_strategy="skip",
        allow_malformed_data=True,
        max_retries=3
    )
    .with_caching(
        enabled=True,
        maxsize=10000,
        clear_after_batch=False
    )
)

# Create processor with this configuration
processor = tm.Processor(config=config)
```

## Processor Configuration Methods

Update a processor's configuration:

```python
# Create a processor with default configuration
processor = tm.Processor()

# Create a new processor with updated naming settings
updated_processor = processor.with_naming(
    separator=".",
    abbreviate_table_names=False
)

# Create a new processor with updated processing settings
updated_processor = processor.with_processing(
    cast_to_string=False,
    batch_size=5000
)

# Create a new processor with updated metadata settings
updated_processor = processor.with_metadata(
    id_field="custom_id"
)

# Create a new processor with updated error handling settings
updated_processor = processor.with_error_handling(
    recovery_strategy="skip"
)

# Create a new processor with updated caching settings
updated_processor = processor.with_caching(
    enabled=True,
    maxsize=50000
)

# Create a new processor with a completely new configuration
updated_processor = processor.with_config(custom_config)
```

## Configuration Components

Transmog's configuration is organized into five components:

### Naming Configuration

Controls table and field naming:

```python
naming_config = tm.NamingConfig(
    separator="_",                # Field name separator
    abbreviate_table_names=True,  # Use abbreviations for table names
    abbreviate_field_names=True,  # Use abbreviations for field names
    max_table_component_length=30,# Maximum length for table name components
    max_field_component_length=30,# Maximum length for field name components
    preserve_leaf_component=True, # Keep the leaf component in full
    preserve_root_component=True, # Keep the root component in full
    custom_abbreviations={        # Custom abbreviations
        "user": "usr",
        "transaction": "txn",
        "information": "info"
    }
)

# Use this configuration component
config = tm.TransmogConfig(naming=naming_config)
```

### Processing Configuration

Controls data processing:

```python
processing_config = tm.ProcessingConfig(
    cast_to_string=True,          # Convert all values to strings
    include_empty=False,          # Include empty values
    skip_null=True,               # Skip null values
    max_nesting_depth=100,        # Maximum nesting depth (None for unlimited)
    path_parts_optimization=True, # Optimize path handling
    visit_arrays=True,            # Process arrays as separate tables
    batch_size=1000,              # Batch size for processing
    processing_mode=tm.ProcessingMode.STANDARD  # Processing mode
)

# Use this configuration component
config = tm.TransmogConfig(processing=processing_config)
```

### Metadata Configuration

Controls metadata generation:

```python
metadata_config = tm.MetadataConfig(
    id_field="__extract_id",     # Field name for record IDs
    parent_field="__parent_extract_id",  # Field name for parent IDs
    time_field="__extract_datetime", # Field name for timestamps
    deterministic_id_fields={    # Fields to use for deterministic IDs
        "users": "user_id",
        "orders": "order_id"
    },
    id_generation_strategy=None  # Optional custom ID generation function
)

# Use this configuration component
config = tm.TransmogConfig(metadata=metadata_config)
```

### Error Handling Configuration

Controls error handling:

```python
error_config = tm.ErrorHandlingConfig(
    allow_malformed_data=False,  # Allow malformed data
    recovery_strategy="strict",  # "strict", "skip", or "partial"
    max_retries=3,               # Maximum retry attempts
    error_log_path=None          # Path for error logging
)

# Use this configuration component
config = tm.TransmogConfig(error_handling=error_config)
```

### Caching Configuration

Controls value processing caching:

```python
caching_config = tm.CachingConfig(
    enabled=True,           # Enable or disable caching
    maxsize=10000,          # Maximum cache size
    clear_after_batch=False # Clear cache after each batch
)

# Use this configuration component
config = tm.TransmogConfig(caching=caching_config)

# Or use the fluent API
config = (
    tm.TransmogConfig.default()
    .with_caching(
        enabled=True,        # Enable or disable caching
        maxsize=50000,       # Maximum cache size
        clear_after_batch=False  # Clear cache after each batch
    )
)

# Use this configuration
processor = tm.Processor(config=config)

# Manually clear the cache
processor.clear_cache()
```

## Processing Modes

Processing modes in `ProcessingMode` enum:

1. **Standard Mode** (`ProcessingMode.STANDARD`)
   - Balanced approach
   - Default configuration

2. **Low Memory Mode** (`ProcessingMode.LOW_MEMORY`)
   - Optimized for memory usage
   - For large datasets or memory-constrained environments
   - Reduces caching and in-memory data

3. **High Performance Mode** (`ProcessingMode.HIGH_PERFORMANCE`)
   - Optimized for processing speed
   - Increases caching

Set the processing mode:

```python
# Option 1: Set via TransmogConfig
config = (
    tm.TransmogConfig.default()
    .with_processing(processing_mode=tm.ProcessingMode.LOW_MEMORY)
)
processor = tm.Processor(config=config)

# Option 2: Use factory method
processor = tm.Processor.memory_optimized()  # Uses LOW_MEMORY mode
processor = tm.Processor.performance_optimized()  # Uses HIGH_PERFORMANCE mode
```

## Configuration Profiles

For reusable configurations:

```python
# Create a configuration
config = tm.TransmogConfig.default().with_naming(separator=".")

# Save configuration to a file
import json
with open("my_config.json", "w") as f:
    json.dump(config.as_dict(), f)

# Load configuration from a file
# Supports JSON, YAML (.yaml/.yml), and TOML (.toml/.tml) formats
loaded_config = tm.load_config("my_config.json")
processor = tm.Processor(config=loaded_config)
```

## Global Settings

Settings that affect all processors:

```python
# Configure global settings
tm.settings.cache_enabled = True
tm.settings.cache_maxsize = 20000
tm.settings.default_batch_size = 1000
```

## Configuration Best Practices

- **Performance vs. Memory**: For large datasets, use `memory_optimized()`
- **ID Consistency**: Use deterministic IDs for consistent record identification
- **Error Recovery**: Choose error handling based on data quality:
  - `strict`: For data where errors indicate problems
  - `skip`: For bulk processing where some records can be skipped
  - `partial`: For extracting data from inconsistent sources
- **Naming Consistency**: Configure naming conventions consistently
  - Use the same separator throughout
  - Consider table name abbreviation for database compatibility
