# Processor API Reference

> **User Guide**: For usage guidance and examples, see:
>
> - [Processing Overview](../user/processing/processing-overview.md) - General processing concepts and methods
> - [Streaming Guide](../user/advanced/streaming.md) - Streaming processing techniques
> - [File Processing Guide](../user/processing/file-processing.md) - Processing data from various file formats
>
> For details on the underlying processing components, see the [Process API](process.md).

The `Processor` class is the main entry point for processing JSON data in Transmog. It provides a high-level
interface for transforming nested JSON structures into flat, relational tables.

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

## Configuration Methods

Create a processor with updated configuration:

```python
# Create a processor with default configuration
processor = Processor()

# Create a new processor with updated naming settings
new_processor = processor.with_naming(separator=".", deep_nesting_threshold=4)

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

## Core Processing Methods

### process

Process data with the current configuration.

```python
process(
    data: Union[dict[str, Any], list[dict[str, Any]], str, bytes],
    entity_name: str,
    transmog_time: Optional[Any] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, list[Dict], str, bytes | Required | JSON data to process (dict, list, JSON string, file path, or bytes) |
| entity_name | str | Required | Name of the entity (used for table naming) |
| transmog_time | Any | None | Extraction timestamp (current time if None) |

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

### process_file

Process a file containing JSON data.

```python
process_file(
    file_path: str,
    entity_name: str,
    transmog_time: Optional[Any] = None,
    input_format: str = "auto"
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the file to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| transmog_time | Any | None | Extraction timestamp (current time if None) |
| input_format | str | "auto" | Format of the input file ("json", "jsonl", "auto") |

#### Example

```python
# Process a JSON file
result = processor.process_file("data.json", entity_name="customers")
```

### process_batch

Process a batch of records.

```python
process_batch(
    batch_data: list[dict[str, Any]],
    entity_name: str,
    transmog_time: Optional[Any] = None,
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| batch_data | list[dict[str, Any]] | Required | List of records to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| transmog_time | Any | None | Extraction timestamp (current time if None) |

#### Example

```python
# Process a batch of records
batch = [{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}]
result = processor.process_batch(batch, entity_name="customers")
```

### process_chunked

Process data in chunks to manage memory usage.

```python
process_chunked(
    data: Union[list[dict[str, Any]], str, Iterator[dict[str, Any]]],
    entity_name: str,
    chunk_size: int = 1000,
    transmog_time: Optional[Any] = None,
    input_format: str = "auto"
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | list[Dict], str, Iterator[Dict] | Required | Data to process in chunks |
| entity_name | str | Required | Name of the entity (used for table naming) |
| chunk_size | int | 1000 | Number of records to process in each chunk |
| transmog_time | Any | None | Extraction timestamp (current time if None) |
| input_format | str | "auto" | Format of the input ("json", "jsonl", "auto") |

#### Example

```python
# Process a large dataset in chunks
result = processor.process_chunked(large_data, entity_name="logs", chunk_size=500)
```

### process_csv

Process a CSV file.

```python
process_csv(
    file_path: str,
    entity_name: str,
    delimiter: str = ",",
    has_header: bool = True,
    skip_rows: int = 0,
    quote_char: str = '"',
    null_values: Optional[list[str]] = None,
    encoding: str = "utf-8",
    infer_types: bool = True,
    sanitize_column_names: bool = True,
    transmog_time: Optional[Any] = None
) -> ProcessingResult
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| file_path | str | Required | Path to the CSV file |
| entity_name | str | Required | Name of the entity (used for table naming) |
| delimiter | str | "," | Field delimiter |
| has_header | bool | True | Whether the file has a header row |
| skip_rows | int | 0 | Number of rows to skip at the beginning |
| quote_char | str | '"' | Character for quoting fields |
| null_values | list[str] | None | Values to interpret as NULL |
| encoding | str | "utf-8" | File encoding |
| infer_types | bool | True | Whether to infer data types |
| sanitize_column_names | bool | True | Whether to clean up column names |
| transmog_time | Any | None | Extraction timestamp (current time if None) |

#### Example

```python
# Process a CSV file
result = processor.process_csv(
    "data.csv",
    entity_name="products",
    delimiter=",",
    has_header=True
)
```

## Streaming Processing Methods

### stream_process

Stream process data to an output format without keeping all results in memory.

```python
stream_process(
    data: Union[dict[str, Any], list[dict[str, Any]], str, bytes, Iterator[dict[str, Any]]],
    entity_name: str,
    output_format: str,
    output_destination: Union[str, IO[Any]],
    transmog_time: Optional[Any] = None,
    **format_options
) -> None
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| data | Dict, list[Dict], str, bytes, Iterator[Dict] | Required | Data to process |
| entity_name | str | Required | Name of the entity (used for table naming) |
| output_format | str | Required | Output format ("json", "csv", "parquet") |
| output_destination | str, IO | Required | Output path or file-like object |
| transmog_time | Any | None | Extraction timestamp (current time if None) |
| **format_options | Any | {} | Format-specific options |

#### Example

```python
# Stream process to Parquet
processor.stream_process(
    data,
    entity_name="customers",
    output_format="parquet",
    output_destination="output/customers",
    compression="snappy"
)
```

### stream_process_file

Stream process a file to an output format.

```python
stream_process_file(
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Union[str, IO[Any]],
    transmog_time: Optional[Any] = None,
    input_format: str = "auto",
    **format_options
) -> None
```

#### Example

```python
# Stream process a JSON file to CSV
processor.stream_process_file(
    "data.json",
    entity_name="records",
    output_format="csv",
    output_destination="output/records",
    include_header=True
)
```

### stream_process_csv

Stream process a CSV file to an output format.

```python
stream_process_csv(
    file_path: str,
    entity_name: str,
    output_format: str,
    output_destination: Union[str, IO[Any]],
    delimiter: str = ",",
    has_header: bool = True,
    skip_rows: int = 0,
    quote_char: str = '"',
    null_values: Optional[list[str]] = None,
    encoding: str = "utf-8",
    infer_types: bool = True,
    sanitize_column_names: bool = True,
    transmog_time: Optional[Any] = None,
    **format_options
) -> None
```

#### Example

```python
# Stream process a CSV file to Parquet
processor.stream_process_csv(
    "data.csv",
    entity_name="products",
    output_format="parquet",
    output_destination="output/products",
    delimiter=",",
    has_header=True,
    compression="snappy"
)
```

## Error Handling Methods

### with_error_handling

Create a new processor with updated error handling settings.

```python
with_error_handling(
    recovery_strategy: str = "strict",
    allow_malformed_data: bool = False,
    max_retries: int = 3,
    error_log_path: Optional[str] = None
) -> Processor
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| recovery_strategy | str | "strict" | Recovery strategy ("strict", "skip", "partial") |
| allow_malformed_data | bool | False | Whether to allow malformed data |
| max_retries | int | 3 | Maximum number of retry attempts |
| error_log_path | Optional[str] | None | Path to write error logs to (None for no logging) |

#### Example

```python
# Create a processor that skips records with errors
processor = processor.with_error_handling(recovery_strategy="skip", allow_malformed_data=True)

# Process data that may contain errors
result = processor.process(data_with_errors, entity_name="records")
```

## Related Resources

For more detailed information about specific components and use cases, refer to:

- [Process API](process.md) - Details about the underlying processing components
- [ProcessingResult API](processing-result.md) - Working with processing results
- [Configuration API](config.md) - Advanced configuration options

For usage guidance and examples, see the user guides:

- [Processing Overview](../user/processing/processing-overview.md) - General processing concepts and methods
- [Streaming Guide](../user/advanced/streaming.md) - Streaming processing techniques
- [File Processing Guide](../user/processing/file-processing.md) - Processing data from various file formats
