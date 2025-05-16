# Processor API Reference

The `Processor` class is the main entry point for processing JSON data in Transmog.

## Import

```python
from transmog import Processor, TransmogConfig, ProcessingMode
```

## Constructor

```python
Processor(config: Optional[TransmogConfig] = None)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| config | TransmogConfig | None | Configuration object. If None, uses default configuration. |

## Configuration

The Processor class uses the `TransmogConfig` class for configuration:

```python
from transmog import Processor, TransmogConfig

# Create with custom configuration
config = (
    TransmogConfig.default()
    .with_naming(
        separator=".",
        abbreviate_table_names=False
    )
    .with_processing(
        cast_to_string=True,
        batch_size=5000
    )
    .with_metadata(
        id_field="custom_id"
    )
    .with_error_handling(
        recovery_strategy="skip"
    )
)

processor = Processor(config=config)
```

See the [Configuration API Reference](config.md) for all configuration options.

## Factory Methods

Factory methods for common configurations:

```python
# Create with default configuration
processor = Processor.default()

# Create with memory optimization for large datasets
processor = Processor.memory_optimized()

# Create with performance optimization
processor = Processor.performance_optimized()

# Create with deterministic ID generation
processor = Processor.with_deterministic_ids({
    "": "id",                     # Root level uses "id" field
    "user_orders": "id"           # Order records use "id" field
})

# Create with custom ID generation
def custom_id_strategy(record):
    return f"CUSTOM-{record['id']}"
processor = Processor.with_custom_id_generation(custom_id_strategy)

# Create with partial recovery
processor = Processor.with_partial_recovery()
```

### Configuration Methods

Create a processor with updated configuration:

```python
# Create a processor with default configuration
processor = Processor()

# Create a new processor with updated naming settings
new_processor = processor.with_naming(separator=".", abbreviate_table_names=False)

# Create a new processor with updated processing settings
new_processor = processor.with_processing(cast_to_string=False, batch_size=5000)

# Create a new processor with updated metadata settings
new_processor = processor.with_metadata(id_field="custom_id")

# Create a new processor with updated error handling settings
new_processor = processor.with_error_handling(recovery_strategy="skip")

# Create a new processor with updated caching settings
new_processor = processor.with_caching(enabled=True, maxsize=50000)

# Create a new processor with a completely new configuration
new_processor = processor.with_config(custom_config)
```

### Partial Recovery Processor

The `with_partial_recovery()` method creates a processor configured to:

1. Use the partial recovery strategy (LENIENT)
2. Enable malformed data processing
3. Cast values to strings to handle numeric type issues

Uses include:

- Data migration from legacy systems
- Processing API responses with inconsistent structures
- Recovering data from malformed files

```python
from transmog import Processor

# Create a processor that will attempt to recover partial data from problematic records
processor = Processor.with_partial_recovery()

# Process data that may contain errors
try:
    result = processor.process(problematic_data, entity_name="records")

    # The output will contain markers for errors but still preserve valid parts
    for record in result.get_main_table():
        if "_error" in record:
            print(f"Record with ID {record.get('id')} had errors: {record['_error']}")
        else:
            print(f"Record with ID {record.get('id')} processed successfully")
except Exception as e:
    print(f"Processing failed despite recovery attempts: {e}")
```

## Processing Methods

### process

Process data with the current configuration.

```python
process(
    data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
    entity_name: str,
    extract_time: Optional[Any] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, List[Dict], str, bytes | Required | JSON data to process (dict, list, JSON string, file path, or bytes) |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
import transmog as tm

processor = tm.Processor()
result = processor.process(data, entity_name="customers")

# Access the processed data
main_table = result.get_main_table()
child_tables = result.get_table_names()
```

### process_batch

Process a batch of records.

```python
process_batch(
    batch_data: List[Dict[str, Any]],
    entity_name: str,
    extract_time: Optional[Any] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| batch_data | List[Dict[str, Any]] | Required | Batch of records to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
batch = [
    {"id": 1, "name": "Record 1"},
    {"id": 2, "name": "Record 2"},
    {"id": 3, "name": "Record 3"}
]
result = processor.process_batch(batch, entity_name="records")
```

### process_file

Process data from a file.

```python
process_file(
    file_path: str,
    entity_name: str,
    extract_time: Optional[Any] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the file to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
result = processor.process_file("data.json", entity_name="records")
```

### process_file_to_format

Process a file and convert the result to a specific format.

```python
process_file_to_format(
    file_path: str,
    entity_name: str,
    output_format: str,
    output_path: Optional[str] = None,
    extract_time: Optional[Any] = None,
    **format_options,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the file to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| output_format | str | Required | Output format (e.g., "json", "csv", "parquet") |
| output_path | str | None | Output path for the files (if None, no files are written) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| **format_options | dict | {} | Format-specific options (e.g., compression, indent) |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
result = processor.process_file_to_format(
    "data.json",
    entity_name="records",
    output_format="parquet",
    output_path="output_dir",
    compression="snappy"
)
```

### process_csv

Process data from a CSV file.

```python
process_csv(
    file_path: str,
    entity_name: str,
    extract_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[List[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    chunk_size: Optional[int] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the CSV file |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| delimiter | str | None | CSV delimiter (default auto-detection) |
| has_header | bool | True | Whether the CSV has a header |
| null_values | List[str] | None | Values to treat as NULL |
| sanitize_column_names | bool | True | Whether to sanitize column names |
| infer_types | bool | True | Whether to infer types from data |
| skip_rows | int | 0 | Number of rows to skip at the beginning |
| quote_char | str | None | Character for quoting fields |
| encoding | str | "utf-8" | File encoding |
| chunk_size | int | None | Process in chunks of this size |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
result = processor.process_csv(
    "data.csv",
    entity_name="records",
    delimiter=",",
    has_header=True,
    infer_types=True
)
```

### process_chunked

Process data in chunks for memory efficiency.

```python
process_chunked(
    data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
    entity_name: str,
    extract_time: Optional[Any] = None,
    chunk_size: Optional[int] = None,
    input_format: str = "auto",
    **format_options,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, List[Dict], str, bytes | Required | Data to process |
| entity_name | str | Required | Name of the entity |
| extract_time | Any | None | Extraction timestamp |
| chunk_size | int | None | Size of chunks to process |
| input_format | str | "auto" | Format of the input data ("auto", "json", "jsonl", "csv") |
| **format_options | dict | {} | Format-specific options |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000,
    input_format="jsonl"
)
```

### stream_process

Process data and stream it directly to output.

```python
stream_process(
    data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
    entity_name: str,
    output_format: str,
    output_destination: Union[str, BinaryIO, StringIO],
    extract_time: Optional[Any] = None,
    **format_options,
) -> None
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, List[Dict], str, bytes | Required | Data to process |
| entity_name | str | Required | Name of the entity |
| output_format | str | Required | Output format |
| output_destination | str, BinaryIO, StringIO | Required | Output destination (path or file-like object) |
| extract_time | Any | None | Extraction timestamp |
| **format_options | dict | {} | Format-specific options |

#### Returns

None. Data is written directly to the output destination.

#### Example

```python
processor.stream_process(
    "large_data.jsonl",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir",
    compression="snappy"
)
```

### stream_process_file

Stream-process a file directly to output.

```python
stream_process_file(
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Union[str, BinaryIO, StringIO],
    extract_time: Optional[Any] = None,
    **format_options,
) -> None
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the file |
| entity_name | str | Required | Name of the entity |
| output_format | str | Required | Output format |
| output_destination | str, BinaryIO, StringIO | Required | Output destination |
| extract_time | Any | None | Extraction timestamp |
| **format_options | dict | {} | Format-specific options |

#### Returns

None. Data is written directly to the output destination.

#### Example

```python
processor.stream_process_file(
    "data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir",
    compression="snappy"
)
```

### stream_process_csv

Stream-process a CSV file directly to output.

```python
stream_process_csv(
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Union[str, BinaryIO, StringIO],
    extract_time: Optional[Any] = None,
    delimiter: Optional[str] = None,
    has_header: bool = True,
    null_values: Optional[List[str]] = None,
    sanitize_column_names: bool = True,
    infer_types: bool = True,
    skip_rows: int = 0,
    quote_char: Optional[str] = None,
    encoding: str = "utf-8",
    **format_options,
) -> None
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the CSV file |
| entity_name | str | Required | Name of the entity |
| output_format | str | Required | Output format |
| output_destination | str, BinaryIO, StringIO | Required | Output destination |
| extract_time | Any | None | Extraction timestamp |
| delimiter | str | None | CSV delimiter |
| has_header | bool | True | Whether the CSV has a header |
| null_values | List[str] | None | Values to treat as NULL |
| sanitize_column_names | bool | True | Whether to sanitize column names |
| infer_types | bool | True | Whether to infer types |
| skip_rows | int | 0 | Number of rows to skip |
| quote_char | str | None | Character for quoting fields |
| encoding | str | "utf-8" | File encoding |
| **format_options | dict | {} | Format-specific options |

#### Returns

None. Data is written directly to the output destination.

### clear_cache

Clear the internal caches used for value processing.

```python
clear_cache() -> None
```

#### Example

```python
processor.clear_cache()
```

## Processing Strategies

The `Processor` class selects the appropriate processing strategy based on the input data:

- `InMemoryStrategy` - For in-memory dictionaries or lists
- `FileStrategy` - For processing file paths
- `ChunkedStrategy` - For processing data in chunks
- `CSVStrategy` - For CSV files
- `BatchStrategy` - For batch processing

The `process` method handles strategy selection automatically.
