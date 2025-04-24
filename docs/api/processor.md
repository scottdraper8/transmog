# Processor API Reference

The `Processor` class is the main entry point for processing JSON data in Transmogrify.

## Import

```python
from transmogrify import Processor
from transmogrify.processor import ProcessingMode  # For memory mode options
```

## Constructor

```python
Processor(
    separator: str = "_",
    cast_to_string: bool = True,
    include_empty: bool = False,
    skip_null: bool = True,
    id_field: str = "__extract_id",
    parent_field: str = "__parent_extract_id",
    time_field: str = "__extract_datetime",
    batch_size: int = 1000,
    optimize_for_memory: bool = False,
    max_nesting_depth: Optional[int] = None,
    recovery_strategy: Optional[RecoveryStrategy] = None,
    allow_malformed_data: Optional[bool] = None,
    path_parts_optimization: Optional[bool] = None,
    visit_arrays: Optional[bool] = None,
    abbreviate_table_names: Optional[bool] = None,
    abbreviate_field_names: Optional[bool] = None,
    max_table_component_length: Optional[int] = None,
    max_field_component_length: Optional[int] = None,
    preserve_leaf_component: Optional[bool] = None,
    custom_abbreviations: Optional[Dict[str, str]] = None,
    deterministic_id_fields: Optional[Dict[str, str]] = None,
    id_generation_strategy: Optional[Callable[[Dict[str, Any]], str]] = None,
)
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| separator | str | "_" | Character(s) to use as separator for nested field names |
| cast_to_string | bool | True | Whether to cast all values to strings |
| include_empty | bool | False | Whether to include empty strings in output |
| skip_null | bool | True | Whether to skip null values in output |
| id_field | str | "__extract_id" | Field name for extract ID |
| parent_field | str | "__parent_extract_id" | Field name for parent ID reference |
| time_field | str | "__extract_datetime" | Field name for extract timestamp |
| batch_size | int | 1000 | Default batch size for processing large datasets |
| optimize_for_memory | bool | False | Whether to optimize for memory over speed |
| max_nesting_depth | int | None | Maximum nesting depth to process (None for unlimited) |
| recovery_strategy | RecoveryStrategy | None | Strategy for handling errors during processing |
| allow_malformed_data | bool | None | Whether to try to recover from malformed data |
| path_parts_optimization | bool | None | Whether to use optimization for deep paths |
| visit_arrays | bool | None | Whether to process arrays as field values |
| abbreviate_table_names | bool | None | Whether to abbreviate table names |
| abbreviate_field_names | bool | None | Whether to abbreviate field names |
| max_table_component_length | int | None | Maximum length for table name components |
| max_field_component_length | int | None | Maximum length for field name components |
| preserve_leaf_component | bool | None | Whether to preserve leaf components in paths |
| custom_abbreviations | Dict[str, str] | None | Custom abbreviation dictionary |
| deterministic_id_fields | Dict[str, str] | None | Dictionary mapping paths to field names for deterministic ID generation |
| id_generation_strategy | Callable[[Dict[str, Any]], str] | None | Custom function for ID generation |

### Processing Modes

Transmogrify supports different processing modes that determine memory usage and performance tradeoffs:

```python
from transmogrify.processor import ProcessingMode

# Available modes
ProcessingMode.STANDARD           # Default - balances memory and performance
ProcessingMode.LOW_MEMORY         # Optimizes for memory usage
ProcessingMode.HIGH_PERFORMANCE   # Optimizes for processing speed
```

### ID Generation Options

Transmogrify supports three approaches for ID generation:

1. **Random UUIDs (Default)**: When `deterministic_id_fields` and `id_generation_strategy` are both `None`, random UUIDs are generated for each record.

2. **Field-based Deterministic IDs**: When `deterministic_id_fields` is provided, specified fields at different paths are used to generate consistent IDs.

3. **Custom ID Generation**: When `id_generation_strategy` is provided, the custom function is used to generate IDs based on the record data.

#### Using Deterministic ID Fields

The `deterministic_id_fields` parameter accepts a dictionary where:
- Keys are path patterns (e.g., `""` for root, `"customers"` for customer records, `"customers_orders"` for orders)
- Values are field names within those records that should be used for ID generation

For example:

```python
# Root-level records use the "id" field
# Customer records use the "customer_id" field
# Order records use the "order_number" field
deterministic_id_fields = {
    "": "id",
    "customers": "customer_id",
    "customers_orders": "order_number"
}

processor = Processor(deterministic_id_fields=deterministic_id_fields)
```

#### Using Custom ID Generation

For advanced scenarios, you can provide a custom function that takes a record and returns an ID string:

```python
def custom_id_generator(record):
    # Generate a custom ID based on multiple fields
    return f"CUSTOM-{record.get('id', '')}-{record.get('type', '')}"

processor = Processor(id_generation_strategy=custom_id_generator)
```

## Methods

### process

Process JSON data and return a processing result.

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

A `ProcessingResult` object containing the main records and child tables.

#### Example

```python
data = {
    "id": 123,
    "name": "Example",
    "items": [
        {"id": 1, "name": "Item 1"},
        {"id": 2, "name": "Item 2"}
    ]
}

result = processor.process(data, entity_name="orders")
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
| batch_data | List[Dict[str, Any]] | Required | Batch of JSON records |
| entity_name | str | Required | Name of the entity |
| extract_time | Any | None | Extraction timestamp (current time if None) |

#### Returns

A `ProcessingResult` object containing the main records and child tables.

#### Example

```python
batch = [
    {"id": 1, "name": "Record 1"},
    {"id": 2, "name": "Record 2"}
]

result = processor.process_batch(batch, entity_name="customers")
```

### process_file

Process a JSON or JSONL file.

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
| file_path | str | Required | Path to JSON or JSONL file |
| entity_name | str | Required | Name of the entity |
| extract_time | Any | None | Extraction timestamp (current time if None) |

#### Returns

A `ProcessingResult` object containing the main records and child tables.

#### Example

```python
result = processor.process_file("data.json", entity_name="products")
```

### process_csv

Process a CSV file.

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
| entity_name | str | Required | Name of the entity |
| extract_time | Any | None | Extraction timestamp (current time if None) |
| delimiter | str | None | Delimiter character (auto-detect or comma if None) |
| has_header | bool | True | Whether the file has a header row |
| null_values | List[str] | None | Values to interpret as NULL |
| sanitize_column_names | bool | True | Whether to sanitize column names |
| infer_types | bool | True | Whether to infer types from values |
| skip_rows | int | 0 | Number of rows to skip at the beginning |
| quote_char | str | None | Quote character (default is double quote) |
| encoding | str | "utf-8" | File encoding |
| chunk_size | int | None | Size of chunks to process (default: batch_size) |

#### Returns

A `ProcessingResult` object containing the main records and child tables.

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

Process data in chunks to optimize memory usage.

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
| data | Dict, List[Dict], str, bytes | Required | JSON data to process |
| entity_name | str | Required | Name of the entity |
| extract_time | Any | None | Extraction timestamp |
| chunk_size | int | None | Size of chunks to process (default: batch_size) |
| input_format | str | "auto" | Input format ("auto", "json", "jsonl", "csv") |
| **format_options | Any | | Format-specific options |

#### Returns

A `ProcessingResult` object containing the main records and child tables.

#### Example

```python
# Process a large file in chunks
result = processor.process_chunked(
    "large_data.jsonl",
    entity_name="transactions",
    chunk_size=500
)
```

## Recovery Strategies

Transmogrify provides several recovery strategies for handling errors:

```python
from transmogrify.recovery import (
    StrictRecovery,              # Fail on any error
    SkipAndLogRecovery,          # Log errors and skip problematic records
    PartialProcessingRecovery    # Try to extract partial data from problematic records
)

# Use with the processor
processor = Processor(recovery_strategy=SkipAndLogRecovery())
```

## Memory and Performance Considerations

- The `