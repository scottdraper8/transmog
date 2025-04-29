# Configuration

Transmog provides a flexible and intuitive configuration system that allows you to customize how your data is processed. The configuration system is built around the `TransmogConfig` class, which aggregates all configuration options into logical groups.

## Basic Configuration

The simplest way to get started is to use the default configuration:

```python
import transmog as tm

# Use default configuration
config = tm.TransmogConfig.default()
processor = tm.Processor(config=config)
```

## Pre-configured Modes

Transmog provides several pre-configured modes for common use cases:

```python
# Memory-optimized configuration
config = tm.TransmogConfig.memory_optimized()

# Performance-optimized configuration
config = tm.TransmogConfig.performance_optimized()
```

## Custom Configuration

You can create custom configurations using the builder pattern:

```python
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
        allow_malformed_data=True,
        max_retries=3
    )
)
```

## Configuration Components

### Naming Configuration

Controls how tables and fields are named:

```python
naming_config = tm.NamingConfig(
    separator="_",                # Field name separator
    abbreviate_table_names=True,  # Use abbreviations for table names
    abbreviate_field_names=True,  # Use abbreviations for field names
    max_table_component_length=30,# Maximum length for table name components
    preserve_leaf_component=True, # Keep the leaf component in full
    custom_abbreviations={        # Custom abbreviations
        "user": "usr",
        "transaction": "txn"
    }
)
```

### Processing Configuration

Controls how data is processed:

```python
processing_config = tm.ProcessingConfig(
    cast_to_string=True,         # Convert all values to strings
    include_empty=False,         # Include empty values
    skip_null=True,             # Skip null values
    max_nesting_depth=None,     # Maximum nesting depth (None for unlimited)
    path_parts_optimization=True,# Optimize path handling
    visit_arrays=False,         # Process arrays as separate tables
    batch_size=1000,            # Batch size for processing
    processing_mode=tm.ProcessingMode.STANDARD  # Processing mode
)
```

### Metadata Configuration

Controls metadata generation:

```python
metadata_config = tm.MetadataConfig(
    id_field="__extract_id",     # Field name for record IDs
    parent_field="__parent_id",  # Field name for parent IDs
    time_field="__processed_at", # Field name for timestamps
    deterministic_id_fields={    # Fields to use for deterministic IDs
        "users": "user_id",
        "orders": "order_id"
    }
)
```

### Error Handling Configuration

Controls error handling behavior:

```python
error_config = tm.ErrorHandlingConfig(
    allow_malformed_data=False,  # Allow malformed data
    recovery_strategy="strict",  # "strict", "skip", or "partial"
    max_retries=3,              # Maximum retry attempts
    error_log_path=None         # Path for error logging
)
```

## Processing Modes

Transmog provides three processing modes:

1. **Standard Mode** (`ProcessingMode.STANDARD`)
   - Balanced approach
   - Good for most use cases
   - Default configuration

2. **Low Memory Mode** (`ProcessingMode.LOW_MEMORY`)
   - Optimized for memory usage
   - Smaller batch sizes
   - More conservative memory allocation

3. **High Performance Mode** (`ProcessingMode.HIGH_PERFORMANCE`)
   - Optimized for processing speed
   - Larger batch sizes
   - More aggressive memory usage

## Deterministic ID Configuration

Configure deterministic ID generation:

```python
# Using specific fields
config = tm.TransmogConfig.with_deterministic_ids({
    "users": "user_id",
    "orders": "order_id"
})

# Using a custom strategy
def custom_id_strategy(data: dict) -> str:
    return f"{data['type']}_{data['id']}"

config = tm.TransmogConfig.with_custom_id_generation(custom_id_strategy)
```

## Best Practices

1. **Start with Defaults**
   - Use `TransmogConfig.default()` as a starting point
   - Only customize what you need

2. **Choose the Right Mode**
   - Use `memory_optimized()` for large datasets
   - Use `performance_optimized()` for time-critical processing
   - Use `standard()` for general use

3. **Configure Error Handling**
   - Set appropriate recovery strategies
   - Configure error logging if needed
   - Set reasonable retry limits

4. **Optimize Naming**
   - Use abbreviations for large schemas
   - Set appropriate length limits
   - Define custom abbreviations for your domain

5. **Monitor Performance**
   - Adjust batch sizes based on your data
   - Tune memory settings based on your environment
   - Use appropriate processing modes 