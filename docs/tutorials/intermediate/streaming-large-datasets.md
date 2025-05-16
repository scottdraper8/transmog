# Streaming Large Datasets

This tutorial demonstrates how to efficiently process and transform large datasets using Transmog's streaming
capabilities.

## When to Use Streaming

Streaming is ideal for:

- Processing datasets too large to fit in memory
- Handling continuous data flows
- Improving memory efficiency
- Reducing processing latency

## Prerequisites

- Basic understanding of Transmog concepts
- Completed the [Transform Nested JSON](../basic/transform-nested-json.md) tutorial

## Setting Up Streaming Processing

First, import the necessary components:

```python
from transmog import TransmogProcessor, TransmogConfig
from transmog.process import StreamingResult
import json
```

## Simple Streaming Example

Here's a basic example of streaming a large JSON file:

```python
# Configure a memory-efficient processor
config = TransmogConfig().memory_optimized()
processor = TransmogProcessor(config)

# Example function to stream records from a file
def stream_records_from_file(file_path):
    with open(file_path, 'r') as file:
        # Assuming file contains one JSON object per line
        for line in file:
            yield json.loads(line.strip())

# Process data in streaming mode
file_path = 'large_dataset.json'
streaming_result = processor.process_stream(stream_records_from_file(file_path))

# Process results as they become available
for table_name, record_batch in streaming_result.iter_records():
    print(f"Table: {table_name}, Batch size: {len(record_batch)}")
    # Process or store the batch
    # ...
```

## Writing Streaming Results to Files

You can write streaming results directly to files:

```python
# Configure streaming output
output_path = "output_directory"

# Process stream and write directly to files
streaming_result = processor.process_stream(
    stream_records_from_file(file_path),
    streaming_output=True
)

# Write to JSON files incrementally
streaming_result.write_streaming_json(output_path)

# Alternative: Write to Parquet files incrementally
# streaming_result.write_streaming_parquet(output_path)
```

## Memory Management with Batch Size

Control memory usage by adjusting the batch size:

```python
# Configure with specific batch size
config = TransmogConfig().memory_optimized().with_processing(
    batch_size=100  # Process 100 records at a time
)
processor = TransmogProcessor(config)

# Process stream with controlled batch size
streaming_result = processor.process_stream(
    stream_records_from_file(file_path),
    streaming_output=True
)
```

## Processing Very Large CSV Files

Here's how to stream and process a large CSV file:

```python
from transmog import TransmogProcessor, TransmogConfig
from transmog.io.readers.csv import CSVReader

# Configure for CSV processing
config = TransmogConfig().memory_optimized()
processor = TransmogProcessor(config)

# Create a CSV reader that will stream rows
csv_reader = CSVReader('large_file.csv')

# Process the CSV stream
streaming_result = processor.process_stream(
    csv_reader.stream_rows(),
    streaming_output=True
)

# Write to Parquet files incrementally
streaming_result.write_streaming_parquet("output_directory")
```

## Parallel Processing with Streaming

For even larger datasets, you can combine streaming with parallel processing:

```python
from concurrent.futures import ThreadPoolExecutor
import os

# Configure for parallel processing
config = TransmogConfig().performance_optimized()
processor = TransmogProcessor(config)

# Function to process a chunk of the file
def process_chunk(chunk_file):
    # Create a generator for the chunk
    def stream_chunk():
        with open(chunk_file, 'r') as file:
            for line in file:
                yield json.loads(line.strip())

    # Process the chunk
    result = processor.process_stream(stream_chunk())

    # Write results for this chunk
    chunk_name = os.path.basename(chunk_file).split('.')[0]
    result.write_all_parquet(f"output/chunk_{chunk_name}")

# List of chunk files (previously split large file)
chunk_files = [f"chunks/chunk_{i}.json" for i in range(10)]

# Process chunks in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    executor.map(process_chunk, chunk_files)
```

## Handling Errors in Streaming Mode

Configure error handling for streaming:

```python
from transmog.error import StreamingErrorStrategy

# Configure with custom error handling for streaming
config = TransmogConfig().memory_optimized().with_error_handling(
    streaming_strategy=StreamingErrorStrategy.SKIP_AND_LOG
)
processor = TransmogProcessor(config)

# Process stream with error handling
streaming_result = processor.process_stream(
    stream_records_from_file("data_with_errors.json"),
    streaming_output=True
)

# Check for errors after processing
error_count = streaming_result.error_count
if error_count > 0:
    print(f"Encountered {error_count} errors during streaming")
    # Get error details
    for error in streaming_result.errors:
        print(f"Error: {error.message}, Record: {error.record_id}")
```

## Performance Considerations

When streaming large datasets:

1. **Batch Size**: Adjust based on your memory constraints
2. **Output Format**: Parquet is generally more efficient than JSON or CSV
3. **Error Handling**: Use SKIP_AND_LOG for production systems
4. **Monitoring**: Track memory usage during processing

## Next Steps

- Learn about [optimizing memory usage](../../user/advanced/performance-optimization.md)
- Try [customizing ID generation](./customizing-id-generation.md)
- Explore [error recovery strategies](../advanced/error-recovery-strategies.md)
