# Basic Concepts

This document explains the core concepts of Transmog and their interactions.

## What is Transmog?

Transmog is a Python library for transforming nested JSON data into flat, structured formats. It performs
the following functions:

- Converting complex nested structures into simpler flat ones
- Maintaining relationships between parent and child entities
- Generating IDs for records
- Supporting multiple output formats

## Quick Overview

The simplest way to understand Transmog is through an example:

```python
import transmog as tm

# Input: Nested JSON
data = {
    "company": "TechCorp",
    "employees": [
        {"name": "Alice", "role": "Engineer"},
        {"name": "Bob", "role": "Designer"}
    ]
}

# Process: One simple call
result = tm.flatten(data, name="companies")

# Output: Flat tables with relationships
print("Main:", result.main)
print("Employees:", result.tables["companies_employees"])
```

This transforms the nested structure into relational tables that can be easily stored in databases or analyzed.

## Core Components

### Simple API

The simple API provides three main functions:

- **`tm.flatten(data, name)`**: Process data in memory
- **`tm.flatten_file(filepath, name)`**: Process files directly
- **`tm.flatten_stream(data, output_dir, name)`**: Stream large datasets to files

### FlattenResult

The `FlattenResult` object provides intuitive access to processed data:

- **`result.main`**: The main flattened table
- **`result.tables`**: Dictionary of child tables
- **`result.all_tables`**: All tables combined
- **`result.save(path)`**: Save to files


### Flattener (Internal)

The flattener converts nested JSON structures into flat dictionaries by:

- Flattening nested objects using path notation (`user_contact_email`)
- Extracting arrays into separate tables
- Maintaining parent-child relationships through IDs

### Processor (Advanced)

For advanced use cases, the full `Processor` class provides:

- Complete configuration control
- Custom processing strategies
- Memory optimization options
- Error handling strategies
- Batch processing capabilities

### Naming System

The naming system controls identifier creation through:

- Managing table names derived from the JSON structure
- Special handling for deeply nested structures
- Implementing different naming conventions (underscore separation by default)
- Generating meaningful field names from JSON paths

### Output Formats

Transmog supports multiple output formats:

- **Memory structures**: Python dictionaries and lists
- **File formats**: JSON, CSV, Parquet

- **Streaming**: Direct-to-file for large datasets

## Processing Flow

The processing flow consists of the following steps:

1. **Input**: Data is read from a source (memory, file, stream)
2. **Flattening**: Nested objects are flattened using dot notation
3. **Array Extraction**: Arrays are moved to separate child tables
4. **ID Generation**: Unique IDs are created for each record
5. **Relationship Linking**: Parent-child relationships are established
6. **Output**: Data is formatted for the requested output

## Data Transformation Examples

### Simple Flattening

```python
# Input
data = {"user": {"name": "John", "age": 30}}

# Process
result = tm.flatten(data, name="users")

# Output
result.main[0]  # {"_id": "...", "user_name": "John", "user_age": "30"}
```

### Array Extraction

```python
# Input
data = {"company": "TechCorp", "employees": [{"name": "Alice"}]}

# Process
result = tm.flatten(data, name="companies")

# Output
result.main[0]  # {"_id": "abc123", "company": "TechCorp"}
result.tables["companies_employees"][0]  # {"_id": "def456", "_parent_id": "abc123", "name": "Alice"}
```

### Deep Nesting

```python
# Input
data = {"user": {"profile": {"settings": {"theme": "dark"}}}}

# Process
result = tm.flatten(data, name="users")

# Output
result.main[0]  # {"_id": "...", "user_profile_settings_theme": "dark"}
```

## Configuration Options

### Simple Options

The simple API accepts common options:

```python
result = tm.flatten(
    data,
    name="users",
    natural_ids=True,        # Use natural IDs from data
    add_timestamp=True,      # Add processing timestamp
    on_error="skip"         # Skip problematic records
)
```

### Advanced Configuration

For complex scenarios, use the full configuration system:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

config = (
    TransmogConfig.default()
    .with_naming(separator="/")           # Use slash separators
    .with_processing(cast_to_string=False) # Keep original types
    .with_metadata(add_timestamp=True)    # Add timestamps
)

processor = Processor(config)
result = processor.process(data, entity_name="users")
```

## Error Handling

Multiple error handling strategies are available:

- **"raise"** (default): Raises errors on any problem
- **"skip"**: Skips problematic records and continues
- **"warn"**: Logs warnings but continues processing

```python
# Handle errors gracefully
result = tm.flatten(problematic_data, name="data", on_error="skip")
```

## Memory and Performance

### Memory Efficiency

For large datasets, use streaming:

```python
# Stream large datasets directly to files
tm.flatten_stream(
    large_dataset,
    output_dir="output/",
    name="big_data",
    format="parquet"
)
```

### Performance Optimization

The simple API automatically optimizes for common cases, but advanced users can tune performance:

```python
from transmog.process import Processor

# Memory-optimized processing
processor = Processor.memory_optimized()
result = processor.process(data, entity_name="users")

# Performance-optimized processing
processor = Processor.performance_optimized()
result = processor.process(data, entity_name="users")
```

## Integration Patterns

### Database Storage

```python
# Flatten data for database storage
result = tm.flatten(json_data, name="records")

# Save as CSV for database import
result.save("database_import.csv")
```

### Data Analysis

```python
# Flatten data for analysis
result = tm.flatten(api_response, name="analytics")
```

### ETL Pipelines

```python
# Process files in an ETL pipeline
for input_file in input_files:
    result = tm.flatten_file(input_file, name="pipeline_data")
    result.save(f"processed/{input_file.stem}.parquet")
```

## Next Steps

- [Getting Started Guide](getting-started.md) - Learn the basic usage patterns
- [Configuration Guide](configuration.md) - Master all configuration options
- [Processing Guide](../processing/processing-overview.md) - Understand data transformation
