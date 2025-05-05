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
processor = tm.Processor(config=config)

# Performance-optimized configuration
config = tm.TransmogConfig.performance_optimized()
processor = tm.Processor(config=config)
```

## Fluent Configuration API

The most flexible way to configure Transmog is using the fluent API, which allows chaining configuration options:

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
        recovery_strategy="skip",
        allow_malformed_data=True,
        max_retries=3
    )
)

# Create processor with this configuration
processor = tm.Processor(config=config)
```

You can also update an existing processor's configuration:

```python
# Create a processor with default configuration
processor = tm.Processor()

# Create a new processor with modified configuration
optimized_processor = processor.with_processing(
    batch_size=10000,
    cast_to_string=False
)
```

## Configuration Components

Transmog's configuration is organized into four main components:

### Naming Configuration

Controls how tables and fields are named:

```python
naming_config = tm.NamingConfig(
    separator="_",                # Field name separator
    abbreviate_table_names=True,  # Use abbreviations for table names
    abbreviate_field_names=True,  # Use abbreviations for field names
    max_table_component_length=30,# Maximum length for table name components
    max_field_component_length=30,# Maximum length for field name components
    preserve_leaf_component=True, # Keep the leaf component in full
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

Controls how data is processed:

```python
processing_config = tm.ProcessingConfig(
    cast_to_string=True,          # Convert all values to strings
    include_empty=False,          # Include empty values
    skip_null=True,               # Skip null values
    max_nesting_depth=None,       # Maximum nesting depth (None for unlimited)
    path_parts_optimization=True, # Optimize path handling
    visit_arrays=False,           # Process arrays as separate tables
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
    parent_field="__parent_id",  # Field name for parent IDs
    time_field="__processed_at", # Field name for timestamps
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

Controls error handling behavior:

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

## Processing Modes

Transmog provides three processing modes through the `ProcessingMode` enum:

1. **Standard Mode** (`ProcessingMode.STANDARD`)
   - Balanced approach
   - Good for most use cases
   - Default configuration

2. **Low Memory Mode** (`ProcessingMode.LOW_MEMORY`)
   - Optimized for memory usage
   - Smaller batch sizes
   - More conservative memory allocation
   - Useful for large datasets or memory-constrained environments

3. **High Performance Mode** (`ProcessingMode.HIGH_PERFORMANCE`)
   - Optimized for processing speed
   - Larger batch sizes
   - More aggressive memory usage
   - Useful for speed-critical applications

```python
# Configure processing mode
config = (
    tm.TransmogConfig.default()
    .with_processing(processing_mode=tm.ProcessingMode.LOW_MEMORY)
)
```

## Conversion Modes

Transmog provides three conversion modes through the `ConversionMode` enum, which control how data is converted and managed in memory when generating output:

1. **Eager Mode** (`ConversionMode.EAGER`)
   - Converts data immediately and keeps all formats in memory
   - Faster for repeated access to the same format
   - Higher memory usage
   - Default mode

2. **Lazy Mode** (`ConversionMode.LAZY`)
   - Converts data only when needed
   - Intermediate conversion results aren't cached
   - Balanced memory usage

3. **Memory Efficient Mode** (`ConversionMode.MEMORY_EFFICIENT`)
   - Minimizes memory usage by clearing intermediate data after conversion
   - Best for very large datasets
   - Slower for repeated conversions to the same format

```python
# Process data
result = processor.process(data, entity_name="records")

# Change the conversion mode to memory efficient
memory_efficient_result = result.with_conversion_mode(tm.ConversionMode.MEMORY_EFFICIENT)

# Output to files with memory efficiency
memory_efficient_result.write_all_parquet("output_dir")
```

## Deterministic ID Configuration

Configure deterministic ID generation for consistent IDs across processing runs:

```python
# Using specific fields for deterministic ID generation
config = tm.TransmogConfig.with_deterministic_ids({
    "": "id",                    # Root level uses "id" field
    "users": "user_id",          # Users table uses "user_id" field
    "orders": "order_id",        # Orders table uses "order_id" field
    "items": "item_id",          # Items table uses "item_id" field
    "users_addresses": "address_id" # Address table uses "address_id" field
})

# Using a custom strategy for complex ID generation
def custom_id_strategy(data: dict) -> str:
    if "id" in data and "type" in data:
        return f"{data['type']}_{data['id']}"
    elif "name" in data:
        return f"name_{data['name']}"
    return None  # Fall back to random UUID

config = tm.TransmogConfig.with_custom_id_generation(custom_id_strategy)
```

## Processing Strategies

Transmog uses different processing strategies depending on your data and configuration:

```python
# InMemoryStrategy - For processing data in memory
processor = tm.Processor(config=config)
result = processor.process(data)

# FileStrategy - For processing data from files
result = processor.process_file("data.json", entity_name="records")

# BatchStrategy - For processing data in batches
result = processor.process_batch(batch_data, entity_name="records")

# ChunkedStrategy - For processing large datasets in chunks
result = processor.process_chunked("large_data.jsonl", entity_name="records", chunk_size=1000)

# CSVStrategy - For processing CSV data
result = processor.process_csv("data.csv", entity_name="records")
```

## Best Practices

1. **Start with Defaults**
   - Use `TransmogConfig.default()` as a starting point
   - Only customize what you need

2. **Choose the Right Mode**
   - Use `memory_optimized()` for large datasets
   - Use `performance_optimized()` for time-critical processing
   - Use `.default()` for general use

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
   
6. **Use Deterministic IDs**
   - For reproducible processing results
   - For data consistency across runs
   - For incremental data processing 