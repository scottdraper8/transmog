# Processing Strategies

Transmog implements the Strategy pattern to handle different data processing scenarios efficiently. This
guide explains the various processing strategies available and how to use them effectively.

## What Are Processing Strategies?

Processing strategies in Transmog determine how data is handled during processing:

- How data is read and buffered
- When and how processing occurs
- How memory is managed
- How results are returned

Each strategy is optimized for specific scenarios and data types.

## Available Strategies

Transmog provides several built-in processing strategies:

```python
from transmog.process import (
    ProcessingStrategy,    # Abstract base class
    InMemoryStrategy,      # For in-memory data
    FileStrategy,          # For file processing
    BatchStrategy,         # For batch processing
    ChunkedStrategy,       # For chunked processing
    CSVStrategy,           # For CSV processing
)
```

### InMemoryStrategy

Used for processing data that's already in memory. This is the default strategy when processing Python data structures.

**Best for:**

- Small to medium-sized datasets
- When the entire dataset fits in memory
- When performance is prioritized over memory usage

**Example:**

```python
from transmog import Processor

processor = Processor()

# InMemoryStrategy is used automatically
result = processor.process(
    data={"id": 1, "name": "example", "items": [{"id": 101}, {"id": 102}]},
    entity_name="record"
)
```

### FileStrategy

Used for processing data directly from files. This strategy handles file opening, reading, and format detection.

**Best for:**

- Processing from files without loading the entire file into memory
- When the file format is known (JSON, JSONL)

**Example:**

```python
from transmog import Processor

processor = Processor()

# FileStrategy is used automatically
result = processor.process_file(
    file_path="data.json",
    entity_name="records"
)
```

### BatchStrategy

Used for processing a batch of records at once. This is efficient for handling collections of similar records.

**Best for:**

- Processing batches from streaming sources
- When memory usage needs to be controlled
- Processing collections of records with the same structure

**Example:**

```python
from transmog import Processor

processor = Processor()

# BatchStrategy is used automatically
batch_data = [
    {"id": 1, "name": "Record 1"},
    {"id": 2, "name": "Record 2"},
    {"id": 3, "name": "Record 3"}
]
result = processor.process_batch(
    batch_data=batch_data,
    entity_name="records"
)
```

### ChunkedStrategy

Used for processing large datasets in manageable chunks. This strategy processes data incrementally to
minimize memory usage.

**Best for:**

- Very large datasets
- When memory is constrained
- Processing files that don't fit in memory

**Example:**

```python
from transmog import Processor

processor = Processor()

# ChunkedStrategy is used automatically
result = processor.process_chunked(
    data="large_data.jsonl",  # Can be a file path
    entity_name="records",
    chunk_size=1000  # Process 1000 records at a time
)

# Also works with any iterable
def generate_records():
    for i in range(10000):
        yield {"id": i, "name": f"Record {i}"}

result = processor.process_chunked(
    data=generate_records(),
    entity_name="records",
    chunk_size=500
)
```

### CSVStrategy

Specialized strategy for processing CSV files, handling the unique aspects of CSV parsing and processing.

**Best for:**

- CSV data with headers
- When column types need to be inferred
- Processing tabular data

**Example:**

```python
from transmog import Processor

processor = Processor()

# CSVStrategy is used automatically
result = processor.process_csv(
    file_path="data.csv",
    entity_name="records",
    delimiter=",",
    has_header=True,
    infer_types=True
)
```

## Strategy Selection

Transmog automatically selects the appropriate strategy based on the method called and the input data type:

| Method | Strategy Used |
|--------|--------------|
| `process()` | InMemoryStrategy for dict/list, FileStrategy for str paths |
| `process_file()` | FileStrategy |
| `process_batch()` | BatchStrategy |
| `process_chunked()` | ChunkedStrategy |
| `process_csv()` | CSVStrategy |

## Streaming Strategies

For memory-efficient processing, Transmog also provides streaming variants that write directly to output formats:

```python
from transmog import Processor

processor = Processor()

# Stream process using appropriate strategy
processor.stream_process(
    data=large_data,
    entity_name="records",
    output_format="csv",
    output_destination="output_dir"
)

# Stream process file
processor.stream_process_file(
    file_path="large_data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output_dir"
)

# Stream process CSV
processor.stream_process_csv(
    file_path="data.csv",
    entity_name="records",
    output_format="json",
    output_destination="output_dir"
)
```

## Custom Strategy Implementation

For advanced use cases, you can implement your own processing strategy by extending the `ProcessingStrategy` class:

```python
from transmog.process import ProcessingStrategy
from transmog import Processor, ProcessingResult

class CustomStrategy(ProcessingStrategy):
    """Custom processing strategy for specialized data sources."""

    def __init__(self, processor, config, **options):
        super().__init__(processor, config, **options)
        # Initialize custom properties

    def process(self, data, entity_name, extract_time=None):
        """Custom processing implementation."""
        # Process data in a custom way
        # Return a ProcessingResult
        return ProcessingResult(...)

# Use the custom strategy
processor = Processor()
result = processor.process_with_strategy(
    CustomStrategy,
    data=custom_data,
    entity_name="records"
)
```

## Strategy Configuration

You can influence strategy behavior through configuration:

```python
from transmog import Processor, TransmogConfig

# Configure memory-optimized processing
config = (
    TransmogConfig.memory_optimized()
    .with_processing(
        batch_size=500,  # Smaller batches for lower memory usage
        path_parts_optimization=True  # Optimize for memory
    )
)

processor = Processor(config=config)
```

## Best Practices

1. **Use the appropriate method for your data source**
   - Use `process()` for small in-memory data
   - Use `process_file()` for files
   - Use `process_chunked()` for large datasets
   - Use `process_csv()` for CSV data

2. **Adjust chunk size based on record complexity**
   - Smaller chunks for complex records
   - Larger chunks for simple records

3. **Use streaming methods for very large datasets**
   - `stream_process_file()` instead of `process_file()`
   - `stream_process_csv()` instead of `process_csv()`

4. **Configure memory thresholds appropriately**
   - Use memory-optimized configuration for large data
   - Adjust batch sizes based on available memory

5. **Consider parallel processing for batch strategies**
   - Process independent batches in parallel
   - Combine results afterwards
