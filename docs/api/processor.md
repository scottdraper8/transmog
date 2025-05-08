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

The Processor class uses the `TransmogConfig` class for configuration. Instead of providing multiple parameters to the constructor, you create and configure a `TransmogConfig` object:

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

See the [Configuration API Reference](config.md) for details on all configuration options.

## Factory Methods

Transmog provides convenient factory methods to create processors with common configurations:

```python
# Create with default configuration
processor = Processor.default()

# Create with memory optimization for large datasets
processor = Processor.memory_optimized()

# Create with performance optimization for speed-critical processing
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

# Create with partial recovery for maximizing data yield from problematic sources
processor = Processor.with_partial_recovery()
```

### Configuration Methods

You can also create a new processor with an updated configuration using the configuration methods:

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

# Create a new processor with a completely new configuration
new_processor = processor.with_config(custom_config)
```

### Partial Recovery Processor

The `with_partial_recovery()` method creates a processor configured to maximize data yield from problematic sources by:

1. Using the partial recovery strategy (LENIENT)
2. Enabling malformed data processing
3. Casting values to strings to handle numeric type issues

This is particularly useful for:
- Data migration from legacy systems
- Processing API responses with inconsistent structures
- Recovering data from malformed/corrupted files

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
    use_single_pass: bool = True,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, List[Dict], str, bytes | Required | JSON data to process. Can be a dict, list of dicts, JSON string, file path, or raw bytes |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| use_single_pass | bool | True | Whether to use optimized single-pass processing |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
import transmog as tm

processor = tm.Processor()
result = processor.process(data, entity_name="customers")

# Access the processed data
tables = result.to_dict()
main_table = tables["main"]
child_tables = {k: v for k, v in tables.items() if k != "main"}
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
| file_path | str | Required | Path to the CSV file to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| delimiter | str | None | Column delimiter (auto-detected if None) |
| has_header | bool | True | Whether the file has a header row |
| null_values | List[str] | None | Values to treat as null |
| sanitize_column_names | bool | True | Whether to sanitize column names |
| infer_types | bool | True | Whether to infer column types |
| skip_rows | int | 0 | Number of rows to skip at the beginning |
| quote_char | str | None | Character used for quoting (auto-detected if None) |
| encoding | str | "utf-8" | File encoding |
| chunk_size | int | None | Number of rows to process at a time (None for all) |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
result = processor.process_csv(
    "data.csv",
    entity_name="customers",
    delimiter=",",
    has_header=True,
    infer_types=True
)
```

### process_chunked

Process data in chunks to minimize memory usage.

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
| data | Dict, List[Dict], str, bytes | Required | Data to process (file path, iterator, or data) |
| entity_name | str | Required | Name of the entity (used for table naming) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| chunk_size | int | None | Number of records to process per chunk |
| input_format | str | "auto" | Input format ("auto", "json", "jsonl", "csv") |
| **format_options | dict | {} | Format-specific options |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
# Process a large file in chunks
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000  # Process 1000 records at a time
)

# Process an iterator in chunks
def generate_records():
    for i in range(10000):
        yield {"id": i, "name": f"Record {i}"}

result = processor.process_chunked(
    generate_records(),
    entity_name="records",
    chunk_size=500
)
```

## Streaming Methods

### stream_process

Process data and write directly to the specified output format.

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
| entity_name | str | Required | Name of the entity (used for table naming) |
| output_format | str | Required | Output format ("json", "csv", "parquet") |
| output_destination | str, file-like | Required | Output destination (path or file-like object) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| **format_options | dict | {} | Format-specific options |

#### Example

```python
# Stream process to CSV
processor.stream_process(
    data,
    entity_name="records",
    output_format="csv",
    output_destination="output_dir"
)

# Stream process to a memory buffer
import io
buffer = io.StringIO()
processor.stream_process(
    data,
    entity_name="records",
    output_format="json",
    output_destination=buffer,
    indent=2
)
```

### stream_process_file

Process a file and write directly to the specified output format.

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
| file_path | str | Required | Path to the file to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| output_format | str | Required | Output format ("json", "csv", "parquet") |
| output_destination | str, file-like | Required | Output destination (path or file-like object) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| **format_options | dict | {} | Format-specific options |

#### Example

```python
# Stream process a file to CSV
processor.stream_process_file(
    "data.json",
    entity_name="records",
    output_format="csv",
    output_destination="output_dir"
)
```

### stream_process_csv

Process a CSV file and write directly to the specified output format.

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

Same as `process_csv` plus:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| output_format | str | Required | Output format ("json", "csv", "parquet") |
| output_destination | str, file-like | Required | Output destination (path or file-like object) |
| **format_options | dict | {} | Format-specific options |

#### Example

```python
# Stream process a CSV file to JSON
processor.stream_process_csv(
    "data.csv",
    entity_name="records",
    output_format="json",
    output_destination="output_dir",
    delimiter=",",
    has_header=True
)
```

## Format Conversion

### process_to_format

Process data and write directly to the specified output format.

```python
process_to_format(
    data: Union[Dict[str, Any], List[Dict[str, Any]], str, bytes],
    entity_name: str,
    output_format: str,
    output_path: Optional[str] = None,
    extract_time: Optional[Any] = None,
    auto_detect_mode: bool = True,
    **format_options,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, List[Dict], str, bytes | Required | Data to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| output_format | str | Required | Output format ("json", "csv", "parquet") |
| output_path | str | None | Output path (None for no file output) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| auto_detect_mode | bool | True | Whether to automatically select processing mode based on data size |
| **format_options | dict | {} | Format-specific options |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
# Process and write to CSV files
result = processor.process_to_format(
    data,
    entity_name="records",
    output_format="csv",
    output_path="output_dir",
    include_header=True
)
```

### process_file_to_format

Process a file and write directly to the specified output format.

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
| output_format | str | Required | Output format ("json", "csv", "parquet") |
| output_path | str | None | Output path (None for no file output) |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| **format_options | dict | {} | Format-specific options |

#### Returns

A `ProcessingResult` object containing the processed data.

#### Example

```python
# Process a file and write to Parquet files
result = processor.process_file_to_format(
    "data.json",
    entity_name="records",
    output_format="parquet",
    output_path="output_dir",
    compression="snappy"
)
```