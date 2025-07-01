# Streaming and Memory-Efficient Processing

Large datasets require specialized handling to avoid memory exhaustion and optimize processing performance. Transmog provides streaming capabilities and memory optimization features designed for datasets that exceed available system memory.

## Memory-Efficient Functions

### Stream Processing

The `flatten_stream()` function processes data in chunks and writes results directly to files, minimizing memory usage:

```python
import transmog as tm

# Process large dataset with streaming
tm.flatten_stream(
    input_path="large_dataset.jsonl",
    output_dir="output/",
    name="dataset",
    low_memory=True,
    batch_size=500
)
```

### Low Memory Mode

The `low_memory` parameter activates comprehensive memory optimization strategies:

```python
# Memory-optimized processing with adaptive features
result = tm.flatten(
    data=large_data,
    low_memory=True,
    batch_size=1000,        # Starting batch size (adapts automatically)
    skip_empty=True
)

# The system automatically:
# - Monitors memory usage and pressure
# - Adapts batch sizes based on available memory
# - Uses in-place modifications to reduce allocations
# - Applies strategic garbage collection
```

## Batch Processing Configuration

### Batch Size Optimization

Batch size affects memory usage and processing speed:

```python
# Small batches for memory-constrained environments
result = tm.flatten(
    data=data,
    batch_size=100,  # Process 100 records at a time
    low_memory=True
)

# Larger batches for performance
result = tm.flatten(
    data=data,
    batch_size=5000,  # Process 5000 records at a time
    low_memory=False
)
```

### Memory Usage Patterns

Different configurations impact memory consumption. The adaptive memory management system automatically adjusts parameters based on available memory:

| Configuration | Memory Usage | Processing Speed | Adaptive Features | Best For |
|---------------|--------------|------------------|-------------------|----------|
| `batch_size=100, low_memory=True` | Minimal | Moderate | Full optimization | Limited memory |
| `batch_size=1000, low_memory=True` | Low | High | Adaptive sizing | Balanced approach |
| `batch_size=5000, low_memory=False` | Variable | Highest | Minimal adaptation | Abundant memory |

The system achieves consistent throughput (13,000+ records/sec) across different memory configurations through adaptive optimization.

## Streaming Input Sources

### JSONL File Processing

Large JSONL files are processed line by line:

```python
# Stream processing of JSONL file
tm.flatten_stream(
    input_path="data.jsonl",
    output_dir="results/",
    name="records",
    batch_size=1000
)
```

### CSV File Processing

Large CSV files with nested JSON columns:

```python
# Stream processing of CSV with JSON columns
tm.flatten_stream(
    input_path="data.csv",
    output_dir="results/",
    name="records",
    json_columns=["metadata", "attributes"],
    batch_size=2000
)
```

## Output Streaming

### Direct File Output

Results are written directly to files without intermediate storage:

```python
# Stream to multiple output formats
tm.flatten_stream(
    input_path="input.jsonl",
    output_dir="output/",
    name="data",
    formats=["json", "csv", "parquet"],
    low_memory=True
)
```

### Incremental Processing

Large datasets are processed incrementally:

```python
# Process with progress tracking
import logging

logging.basicConfig(level=logging.INFO)

tm.flatten_stream(
    input_path="massive_dataset.jsonl",
    output_dir="results/",
    name="dataset",
    batch_size=1000,
    low_memory=True
)
# INFO: Processed batch 1 (1000 records)
# INFO: Processed batch 2 (2000 records)
# ...
```

## Memory Optimization Strategies

### Data Type Optimization

Preserve minimal data types to reduce memory usage:

```python
result = tm.flatten(
    data=data,
    preserve_types=False,  # Convert all to strings
    skip_null=True,        # Remove null values
    skip_empty=True,       # Remove empty strings/lists
    low_memory=True
)
```

### Field Filtering

Skip unnecessary fields during processing:

```python
# Custom processor for field filtering
from transmog.process import Processor
from transmog.config import TransmogConfig

config = TransmogConfig.memory_optimized()
processor = Processor(config)

# Process with field exclusions
result = processor.process(
    data=data,
    exclude_fields=["debug_info", "temp_data", "cache"]
)
```

## Performance Monitoring

### Memory Usage Tracking

Monitor memory consumption during processing:

```python
import psutil
import os

def track_memory():
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024  # MB

# Before processing
initial_memory = track_memory()

result = tm.flatten(
    data=large_data,
    low_memory=True,
    batch_size=1000
)

# After processing
final_memory = track_memory()
print(f"Memory used: {final_memory - initial_memory:.2f} MB")
```

### Processing Time Optimization

Balance memory usage with processing time:

```python
import time

start_time = time.time()

tm.flatten_stream(
    input_path="data.jsonl",
    output_dir="output/",
    name="data",
    batch_size=2000,
    low_memory=True
)

processing_time = time.time() - start_time
print(f"Processing completed in {processing_time:.2f} seconds")
```

## Error Handling in Streaming

### Resilient Processing

Handle errors gracefully during streaming:

```python
# Continue processing despite errors
tm.flatten_stream(
    input_path="data_with_errors.jsonl",
    output_dir="output/",
    name="data",
    errors="skip",  # Skip problematic records
    batch_size=1000
)
```

### Error Logging

Track processing issues:

```python
import logging

# Configure error logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('processing_errors.log'),
        logging.StreamHandler()
    ]
)

tm.flatten_stream(
    input_path="input.jsonl",
    output_dir="output/",
    name="data",
    errors="warn",  # Log warnings for errors
    batch_size=1000
)
```

## Best Practices

### Memory-Efficient Configuration

Recommended settings for large datasets:

```python
# Optimal streaming configuration
tm.flatten_stream(
    input_path="large_dataset.jsonl",
    output_dir="output/",
    name="dataset",
    batch_size=1000,         # Moderate batch size
    low_memory=True,         # Enable memory optimization
    skip_empty=True,         # Reduce output size
    preserve_types=False,    # Minimize memory usage
    errors="skip",           # Continue on errors
    formats=["parquet"]      # Efficient storage format
)
```

### Disk Space Management

Monitor disk usage during streaming operations:

```python
import shutil

def check_disk_space(path):
    total, used, free = shutil.disk_usage(path)
    return free // (1024**3)  # GB free

# Check available space before processing
free_space = check_disk_space("output/")
if free_space < 10:  # Less than 10GB
    print("Warning: Low disk space available")

# Process with space monitoring
tm.flatten_stream(
    input_path="input.jsonl",
    output_dir="output/",
    name="data",
    batch_size=1000
)
```

### Resource Cleanup

Ensure proper resource cleanup after processing:

```python
import gc

try:
    result = tm.flatten(
        data=large_data,
        low_memory=True,
        batch_size=1000
    )

    # Process results
    print(f"Processed {len(result.main)} main records")

finally:
    # Force garbage collection
    gc.collect()
```

## Integration Examples

### With Data Pipelines

Integrate streaming processing into data pipelines:

```python
def process_daily_data(date_str):
    """Process daily data files efficiently."""
    input_file = f"data/daily_{date_str}.jsonl"
    output_dir = f"processed/{date_str}/"

    tm.flatten_stream(
        input_path=input_file,
        output_dir=output_dir,
        name="daily_records",
        batch_size=2000,
        low_memory=True,
        formats=["parquet", "csv"]
    )

    return output_dir

# Process multiple days
for day in ["2024-01-01", "2024-01-02", "2024-01-03"]:
    output_path = process_daily_data(day)
    print(f"Processed {day} data to {output_path}")
```

### With Monitoring Systems

Integrate with monitoring and alerting:

```python
import logging
from datetime import datetime

def monitored_processing(input_path, output_dir):
    """Process data with comprehensive monitoring."""
    start_time = datetime.now()
    initial_memory = track_memory()

    try:
        tm.flatten_stream(
            input_path=input_path,
            output_dir=output_dir,
            name="monitored_data",
            batch_size=1000,
            low_memory=True,
            errors="warn"
        )

        # Log success metrics
        duration = datetime.now() - start_time
        memory_used = track_memory() - initial_memory

        logging.info(f"Processing completed successfully")
        logging.info(f"Duration: {duration}")
        logging.info(f"Memory used: {memory_used:.2f} MB")

    except Exception as e:
        logging.error(f"Processing failed: {e}")
        raise

# Set up monitoring
logging.basicConfig(level=logging.INFO)
monitored_processing("large_file.jsonl", "output/")
```
