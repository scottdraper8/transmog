# Process API

This document describes the lower-level processing components in Transmog. These components form the
foundation of the higher-level [Processor API](processor.md) that most users will interact with.

> **User Guide**: For a user-friendly introduction, see:
>
> - [Processing Overview](../user/processing/processing-overview.md) - General processing concepts and methods
> - [Streaming Guide](../user/advanced/streaming.md) - Streaming processing techniques
> - [File Processing Guide](../user/processing/file-processing.md) - Processing data from various file formats

## Overview of Processing Components

The Transmog processing system is built around the following core components:

- **Processing Strategies** - Classes that implement different approaches to process data
- **Conversion Mode** - Options for controlling memory usage during conversion
- **Processing Functions** - Utility functions for common processing tasks
- **ProcessingResult** - Class that encapsulates processing results

## Relationship to Processor API

While most users will interact with the high-level [Processor API](processor.md), the components described
in this document provide the underlying implementation:

| Processor API Method | Process API Components Used |
|---------------------|----------------------------|
| `processor.process()` | Selects appropriate strategy based on input |
| `processor.process_file()` | Uses `FileStrategy` and `process_file()` function |
| `processor.process_batch()` | Uses `BatchStrategy` |
| `processor.process_chunked()` | Uses `ChunkedStrategy` and `process_chunked()` function |
| `processor.process_csv()` | Uses `CSVStrategy` and `process_csv()` function |
| `processor.stream_*()` methods | Use the various streaming functions |

## Processing Strategies

```python
from transmog.process import (
    ProcessingStrategy,  # Abstract base class
    InMemoryStrategy,    # For in-memory data
    FileStrategy,        # For file processing
    BatchStrategy,       # For batch processing
    ChunkedStrategy,     # For chunked processing
    CSVStrategy          # For CSV processing
)
```

### Base Strategy Class

`ProcessingStrategy` is the abstract base class for all processing strategies:

```python
class ProcessingStrategy(ABC):
    @abstractmethod
    def process(self, data, entity_name: str, extract_time=None) -> ProcessingResult:
        """Process the data and return a ProcessingResult."""
        pass
```

### InMemoryStrategy

`InMemoryStrategy` processes data directly in memory:

```python
from transmog.process import InMemoryStrategy
from transmog import TransmogConfig

config = TransmogConfig.default()
strategy = InMemoryStrategy(config)
result = strategy.process(data, entity_name="records")
```

### FileStrategy

`FileStrategy` processes data from files:

```python
from transmog.process import FileStrategy
from transmog import TransmogConfig

config = TransmogConfig.default()
strategy = FileStrategy(config)
result = strategy.process("data.json", entity_name="records")
```

### BatchStrategy

`BatchStrategy` processes data in batches:

```python
from transmog.process import BatchStrategy
from transmog import TransmogConfig

config = TransmogConfig.default()
strategy = BatchStrategy(config, batch_size=1000)
result = strategy.process(large_data, entity_name="records")
```

### ChunkedStrategy

`ChunkedStrategy` processes large datasets in manageable chunks:

```python
from transmog.process import ChunkedStrategy
from transmog import TransmogConfig

config = TransmogConfig.default()
strategy = ChunkedStrategy(config, chunk_size=1000)
result = strategy.process("large_data.jsonl", entity_name="records")
```

### CSVStrategy

`CSVStrategy` is specialized for processing CSV files:

```python
from transmog.process import CSVStrategy
from transmog import TransmogConfig

config = TransmogConfig.default()
strategy = CSVStrategy(
    config,
    delimiter=",",
    has_header=True,
    infer_types=True
)
result = strategy.process("data.csv", entity_name="records")
```

## Conversion Mode

```python
from transmog.process import ConversionMode
```

The `ConversionMode` enum defines result conversion strategies:

| Mode | Description |
|------|-------------|
| `EAGER` | Convert and cache data immediately |
| `LAZY` | Convert data when needed |
| `MEMORY_EFFICIENT` | Minimize memory usage during conversion |

Usage:

```python
from transmog.process import ConversionMode

# Use eager mode (default)
tables = result.to_dict(conversion_mode=ConversionMode.EAGER)

# Use lazy mode
tables = result.to_dict(conversion_mode=ConversionMode.LAZY)

# Use memory-efficient mode
tables = result.to_dict(conversion_mode=ConversionMode.MEMORY_EFFICIENT)
```

## Low-Level Processing Functions

These functions provide direct access to the processing capabilities without going through the `Processor` class:

```python
from transmog.process import (
    process_file,
    process_file_to_format,
    process_csv,
    process_chunked,
    stream_process,
    stream_process_file,
    stream_process_csv,
    stream_process_file_with_format
)
```

### File Processing Functions

```python
# Process a file and get the result
from transmog.process import process_file
from transmog import TransmogConfig

config = TransmogConfig.default()
result = process_file("data.json", entity_name="records", config=config)
```

```python
# Process a file and write to a specific format
from transmog.process import process_file_to_format
from transmog import TransmogConfig

config = TransmogConfig.default()
result = process_file_to_format(
    "data.json",
    entity_name="records",
    output_format="parquet",
    output_path="output/data.parquet",
    config=config
)
```

```python
# Process a CSV file
from transmog.process import process_csv
from transmog import TransmogConfig

config = TransmogConfig.default()
result = process_csv(
    "data.csv",
    entity_name="records",
    delimiter=",",
    has_header=True,
    config=config
)
```

```python
# Process a large file in chunks
from transmog.process import process_chunked
from transmog import TransmogConfig

config = TransmogConfig.default()
result = process_chunked(
    "large_data.jsonl",
    entity_name="records",
    chunk_size=1000,
    config=config
)
```

### Streaming Functions

```python
# Stream process data directly to output
from transmog.process import stream_process
from transmog import TransmogConfig

config = TransmogConfig.default()
stream_process(
    data,
    entity_name="records",
    output_format="parquet",
    output_destination="output/stream",
    config=config
)
```

```python
# Stream process a file
from transmog.process import stream_process_file
from transmog import TransmogConfig

config = TransmogConfig.default()
stream_process_file(
    "data.json",
    entity_name="records",
    output_format="parquet",
    output_destination="output/stream",
    config=config
)
```

```python
# Stream process a CSV file
from transmog.process import stream_process_csv
from transmog import TransmogConfig

config = TransmogConfig.default()
stream_process_csv(
    "data.csv",
    entity_name="records",
    output_format="parquet",
    output_destination="output/stream",
    delimiter=",",
    has_header=True,
    config=config
)
```

```python
# Stream process a file with explicit format
from transmog.process import stream_process_file_with_format
from transmog import TransmogConfig

config = TransmogConfig.default()
stream_process_file_with_format(
    "data.txt",
    entity_name="records",
    format_type="jsonl",
    output_format="parquet",
    output_destination="output/stream",
    config=config
)
```

## ProcessingResult

The `ProcessingResult` class encapsulates processing results. See [ProcessingResult API](processing-result.md)
for detailed documentation.

```python
from transmog.process import ProcessingResult

# Access the main table
main_table = result.get_main_table()

# Get available table names
table_names = result.get_table_names()

# Access a child table
child_table = result.get_child_table("records_items")
```

## Implementation Details

### Strategy Selection

When using the high-level `Processor` API, the appropriate strategy is selected automatically based on the input type:

| Input Type | Strategy Selected |
|------------|------------------|
| Dict, List[Dict] | InMemoryStrategy |
| File path (str) | FileStrategy |
| CSV file path with process_csv | CSVStrategy |
| Any data with process_chunked | ChunkedStrategy |
| List[Dict] with process_batch | BatchStrategy |

### Strategy Composition

Strategies can be composed or extended to create custom processing behaviors:

```python
from transmog.process import ProcessingStrategy, InMemoryStrategy
from transmog import TransmogConfig

class CustomStrategy(ProcessingStrategy):
    def __init__(self, config):
        self.config = config
        self.memory_strategy = InMemoryStrategy(config)

    def process(self, data, entity_name, extract_time=None):
        # Pre-process data
        processed_data = self.preprocess(data)

        # Use existing strategy for main processing
        return self.memory_strategy.process(
            processed_data,
            entity_name,
            extract_time
        )

    def preprocess(self, data):
        # Custom preprocessing logic
        return data
```

## Related Resources

For higher-level interfaces and usage examples, see:

- [Processor API](processor.md) - High-level API for data processing
- [ProcessingResult API](processing-result.md) - Working with processing results
- [Processing Guide](../user/processing/processing-overview.md) - Conceptual overview and usage examples
