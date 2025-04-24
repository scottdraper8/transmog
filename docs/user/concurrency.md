# Concurrency and Parallel Processing

Transmogrify provides features to effectively utilize parallel processing for handling large datasets.

## Overview

Transmogrify can be used with various concurrency methods:

1. **Multithreading**: Using Python's `threading` or `concurrent.futures.ThreadPoolExecutor`
2. **Multiprocessing**: Using Python's `multiprocessing` or `concurrent.futures.ProcessPoolExecutor`

This guide demonstrates how to implement different concurrency patterns with Transmogrify.

## Basic Parallel Processing

For simple parallel processing, you can use Python's `concurrent.futures` module with Transmogrify:

```python
import transmogrify as tm
from concurrent.futures import ThreadPoolExecutor
import json

# Load your data
with open("large_dataset.json", "r") as f:
    data = json.load(f)

# Split data into chunks
def split_into_chunks(data, chunk_size=1000):
    """Split a list into chunks of specified size."""
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    
chunks = split_into_chunks(data, chunk_size=1000)

# Process in parallel
processor = tm.Processor(cast_to_string=True)

with ThreadPoolExecutor(max_workers=4) as executor:
    # Submit processing jobs
    futures = [
        executor.submit(
            processor.process_batch,
            batch_data=chunk,
            entity_name="customers"
        )
        for chunk in chunks
    ]
    
    # Collect results
    results = [future.result() for future in futures]
    
# Combine results
combined_result = tm.ProcessingResult.combine_results(results)

# Write to Parquet
combined_result.write_all_parquet(base_path="output/data")
```

## Chunked Processing

For very large datasets that don't fit in memory, use chunked processing:

```python
import transmogrify as tm
import json

# Initialize processor
processor = tm.Processor(
    cast_to_string=True,
    batch_size=1000,  # Set batch size for memory management
    optimize_for_memory=True  # Optimize for memory over speed
)

# Process a large file in chunks
result = processor.process_chunked(
    "very_large_dataset.json",
    entity_name="records",
    chunk_size=500  # Process 500 records at a time
)
```

## Hybrid Approach: Parallel Chunked Processing

For the best performance with very large datasets, combine chunking with parallelism:

```python
import transmogrify as tm
import json
from concurrent.futures import ProcessPoolExecutor
import os

# Function to process a file chunk
def process_file_chunk(file_path, start_line, num_lines, entity_name):
    # Read specific chunk from file
    records = []
    with open(file_path, 'r') as f:
        # Skip to start position
        for _ in range(start_line):
            next(f)
        # Read chunk
        for _ in range(num_lines):
            try:
                line = next(f).strip()
                if line:
                    records.append(json.loads(line))
            except StopIteration:
                break
    
    # Process chunk
    processor = tm.Processor(cast_to_string=True)
    return processor.process_batch(records, entity_name=entity_name)

# Count total lines in file
def count_lines(file_path):
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

# Process large file in parallel chunks
def process_large_file(file_path, entity_name, chunk_size=10000, workers=4):
    total_lines = count_lines(file_path)
    chunks = [(i, min(chunk_size, total_lines - i)) 
              for i in range(0, total_lines, chunk_size)]
    
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                process_file_chunk,
                file_path,
                start_line,
                num_lines,
                entity_name
            )
            for start_line, num_lines in chunks
        ]
        
        # Collect results
        results = [future.result() for future in futures]
    
    # Combine results
    return tm.ProcessingResult.combine_results(results, entity_name=entity_name)

# Usage
result = process_large_file(
    "huge_dataset.jsonl", 
    entity_name="events",
    chunk_size=50000,
    workers=os.cpu_count()
)
```

## Performance Tuning

When using concurrency, consider these factors:

1. **Number of workers**: Start with `os.cpu_count()` and adjust based on your workload
2. **Chunk size**: Find a balance between too many small chunks (overhead) and too few large chunks (poor parallelization)
3. **Thread vs Process**: Use threads for I/O-bound work (like reading/writing files) and processes for CPU-bound work
4. **Memory Management**: For very large datasets, prioritize memory efficiency with `optimize_for_memory=True`

## Thread Safety

All Transmogrify methods are thread-safe and can be safely called from multiple threads. The library uses:

1. Thread-safe caching mechanisms
2. No shared mutable state between threads
3. Immutable objects for thread safety

## Error Handling in Parallel Processing

When using parallel processing, handle errors properly:

```python
results = []
failed_chunks = []

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(processor.process_batch, chunk, "records"): i
        for i, chunk in enumerate(chunks)
    }
    
    for future in as_completed(futures):
        chunk_index = futures[future]
        try:
            result = future.result()
            results.append(result)
        except Exception as e:
            print(f"Chunk {chunk_index} failed: {str(e)}")
            failed_chunks.append(chunk_index)

# Handle any failed chunks
if failed_chunks:
    print(f"Failed chunks: {failed_chunks}")

# Combine successful results
if results:
    combined_result = tm.ProcessingResult.combine_results(results)
else:
    print("All chunks failed")
```

## Best Practices

1. **Benchmark first**: Compare different concurrency approaches for your specific data
2. **Balance chunk size**: Smaller chunks provide better load balancing but increase overhead
3. **Monitor memory usage**: Watch memory consumption during processing and adjust as needed
4. **Handle errors gracefully**: Always catch and handle exceptions in concurrent processing
5. **Consider data locality**: For distributed processing, try to keep data close to the compute 