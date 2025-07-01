# Performance Optimization

Transmog performance can be optimized through configuration tuning, data preparation, and processing strategies. This guide covers techniques for maximizing throughput and minimizing processing time.

## Configuration Optimization

### Batch Size Tuning

Batch size significantly impacts processing performance:

```python
import transmog as tm
import time

def benchmark_batch_sizes(data, sizes=[100, 500, 1000, 2000, 5000]):
    """Benchmark different batch sizes."""
    results = {}

    for size in sizes:
        start = time.time()
        result = tm.flatten(data, batch_size=size)
        duration = time.time() - start
        results[size] = duration

    return results

# Find optimal batch size
data = [{"nested": {"field": i}} for i in range(10000)]
timings = benchmark_batch_sizes(data)

for size, duration in timings.items():
    print(f"Batch size {size}: {duration:.2f}s")
```

### Memory vs Speed Trade-offs

Balance memory usage with processing speed. The system includes adaptive memory management that automatically adjusts processing parameters based on available memory:

```python
# High performance configuration
fast_config = {
    "batch_size": 5000,
    "low_memory": False,
    "preserve_types": True
}

# Memory-efficient configuration (uses adaptive batch sizing)
efficient_config = {
    "batch_size": 1000,
    "low_memory": True,
    "preserve_types": False
}

# Benchmark both approaches
data = load_large_dataset()

start = time.time()
fast_result = tm.flatten(data, **fast_config)
fast_time = time.time() - start

start = time.time()
efficient_result = tm.flatten(data, **efficient_config)
efficient_time = time.time() - start

print(f"Fast config: {fast_time:.2f}s")
print(f"Efficient config: {efficient_time:.2f}s")
```

## Data Preparation Optimization

### Input Data Structure

Optimize input data structure for better performance:

```python
# Inefficient: Deeply nested with many empty fields
inefficient_data = {
    "level1": {
        "level2": {
            "level3": {
                "level4": {
                    "value": "data",
                    "empty1": None,
                    "empty2": "",
                    "empty3": []
                }
            }
        }
    }
}

# Efficient: Flatter structure, minimal empty fields
efficient_data = {
    "level1_level2_value": "data",
    "metadata": {"timestamp": "2024-01-01"}
}

# Process with optimized settings
result = tm.flatten(
    efficient_data,
    skip_empty=True,
    skip_null=True,
    nested_threshold=3
)
```

### Array Handling Optimization

Choose optimal array handling strategy:

```python
data_with_arrays = {
    "users": [
        {"id": 1, "name": "Alice"},
        {"id": 2, "name": "Bob"}
    ]
}

# Fastest: Skip arrays if not needed
result_skip = tm.flatten(data_with_arrays, arrays="skip")

# Moderate: Inline small arrays
result_inline = tm.flatten(data_with_arrays, arrays="inline")

# Slowest: Separate tables (most flexible)
result_separate = tm.flatten(data_with_arrays, arrays="separate")
```

## Processing Strategies

### Parallel Processing

Process multiple datasets concurrently:

```python
import concurrent.futures
import transmog as tm

def process_file(file_path):
    """Process a single file."""
    return tm.flatten_file(
        file_path,
        batch_size=2000,
        low_memory=True
    )

# Process multiple files in parallel
file_paths = ["data1.json", "data2.json", "data3.json"]

with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(process_file, path) for path in file_paths]
    results = [future.result() for future in futures]

print(f"Processed {len(results)} files")
```

### Incremental Processing

Process large datasets incrementally:

```python
def process_incrementally(large_dataset, chunk_size=10000):
    """Process dataset in chunks."""
    results = []

    for i in range(0, len(large_dataset), chunk_size):
        chunk = large_dataset[i:i + chunk_size]
        result = tm.flatten(
            chunk,
            name=f"chunk_{i//chunk_size}",
            batch_size=2000
        )
        results.append(result)

        # Optional: Clear memory between chunks
        del chunk

    return results

# Process 100k records in 10k chunks
large_data = generate_large_dataset(100000)
chunk_results = process_incrementally(large_data)
```

## Advanced Configuration

### Memory Optimization Features

Transmog includes adaptive memory management that automatically adjusts processing parameters:

```python
import transmog as tm

# Enable memory tracking and adaptive sizing
result = tm.flatten(
    large_dataset,
    batch_size=1000,           # Starting batch size
    low_memory=True,           # Enable memory-efficient mode
    # Memory optimization is automatic when low_memory=True
)

# The system will:
# - Monitor memory usage during processing
# - Adapt batch sizes based on available memory
# - Use strategic garbage collection to reduce pressure
# - Apply in-place modifications to reduce allocations
```

### Memory Optimization Strategies

The memory optimization system reduces memory usage through several techniques:

- **In-place modifications**: 60-70% reduction in object allocations
- **Efficient path building**: 40-50% reduction in string operations
- **Adaptive caching**: Memory-aware cache sizing that responds to pressure
- **Strategic garbage collection**: Intelligent timing of memory cleanup

### Custom Processor Configuration

Use advanced processor settings for optimal performance:

```python
from transmog.process import Processor
from transmog.config import TransmogConfig

# Performance-optimized configuration with memory awareness
config = (
    TransmogConfig.performance_optimized()
    .with_memory_optimization(
        memory_tracking_enabled=True,
        adaptive_batch_sizing=True,
        memory_pressure_threshold=0.8
    )
    .with_processing(
        batch_size=5000,
        memory_limit="2GB"
    )
    .with_naming(
        separator="_",
        max_depth=5
    )
)

processor = Processor(config)
result = processor.process(data)
```

### Type Preservation Optimization

Optimize type handling based on use case:

```python
# Fast: Convert all to strings
result_strings = tm.flatten(
    data,
    preserve_types=False,
    batch_size=5000
)

# Slower but preserves data integrity
result_typed = tm.flatten(
    data,
    preserve_types=True,
    batch_size=3000
)
```

## Performance Monitoring

### Execution Time Profiling

Profile processing performance:

```python
import time
import cProfile

def profile_processing():
    """Profile flatten operation."""
    data = generate_test_data(10000)

    # Profile with cProfile
    profiler = cProfile.Profile()
    profiler.enable()

    result = tm.flatten(data, batch_size=2000)

    profiler.disable()
    profiler.print_stats(sort='tottime')

    return result

# Run profiling
profile_processing()
```

### Memory Usage Monitoring

Track memory consumption:

```python
import psutil
import os

class MemoryMonitor:
    def __init__(self):
        self.process = psutil.Process(os.getpid())
        self.initial_memory = self.current_memory()

    def current_memory(self):
        return self.process.memory_info().rss / 1024 / 1024  # MB

    def memory_used(self):
        return self.current_memory() - self.initial_memory

# Monitor processing
monitor = MemoryMonitor()

result = tm.flatten(
    large_data,
    batch_size=2000,
    low_memory=True
)

print(f"Memory used: {monitor.memory_used():.2f} MB")
print(f"Records processed: {len(result.main)}")
```

### Throughput Measurement

Measure processing throughput:

```python
def measure_throughput(data, config):
    """Measure records processed per second."""
    start_time = time.time()
    result = tm.flatten(data, **config)
    end_time = time.time()

    duration = end_time - start_time
    record_count = len(result.main)
    throughput = record_count / duration

    return {
        "duration": duration,
        "records": record_count,
        "throughput": throughput
    }

# Test different configurations
configs = [
    {"batch_size": 1000, "low_memory": True},
    {"batch_size": 2000, "low_memory": True},
    {"batch_size": 5000, "low_memory": False}
]

data = generate_test_data(50000)

for i, config in enumerate(configs):
    metrics = measure_throughput(data, config)
    print(f"Config {i+1}: {metrics['throughput']:.0f} records/sec")
```

## Optimization Guidelines

### Choosing Optimal Settings

Recommended configurations for different scenarios:

```python
# High-volume, simple data
high_volume_config = {
    "batch_size": 5000,
    "low_memory": False,
    "preserve_types": False,
    "skip_empty": True,
    "arrays": "inline"
}

# Complex nested data
complex_data_config = {
    "batch_size": 2000,
    "low_memory": True,
    "preserve_types": True,
    "nested_threshold": 6,
    "arrays": "separate"
}

# Memory-constrained environment
memory_constrained_config = {
    "batch_size": 500,
    "low_memory": True,
    "preserve_types": False,
    "skip_empty": True,
    "skip_null": True
}
```

### Performance Testing Framework

Create systematic performance tests:

```python
class PerformanceTest:
    def __init__(self, data_generator):
        self.data_generator = data_generator
        self.results = []

    def test_config(self, config, data_size=10000):
        """Test a specific configuration."""
        data = self.data_generator(data_size)

        start_time = time.time()
        initial_memory = self.get_memory()

        result = tm.flatten(data, **config)

        end_time = time.time()
        final_memory = self.get_memory()

        metrics = {
            "config": config,
            "duration": end_time - start_time,
            "memory_used": final_memory - initial_memory,
            "records_processed": len(result.main),
            "throughput": len(result.main) / (end_time - start_time)
        }

        self.results.append(metrics)
        return metrics

    def get_memory(self):
        process = psutil.Process(os.getpid())
        return process.memory_info().rss / 1024 / 1024

    def best_config(self, metric="throughput"):
        """Find best configuration by metric."""
        return max(self.results, key=lambda x: x[metric])

# Usage
def generate_nested_data(size):
    return [{"nested": {"field": i}} for i in range(size)]

tester = PerformanceTest(generate_nested_data)

# Test multiple configurations
configs = [
    {"batch_size": 1000},
    {"batch_size": 2000},
    {"batch_size": 5000}
]

for config in configs:
    metrics = tester.test_config(config)
    print(f"Config {config}: {metrics['throughput']:.0f} records/sec")

best = tester.best_config("throughput")
print(f"Best config: {best['config']}")
```

## Real-World Optimization Examples

### E-commerce Data Processing

Optimize for typical e-commerce data structures:

```python
# E-commerce product data
ecommerce_data = {
    "products": [
        {
            "id": "prod_123",
            "name": "Widget",
            "price": 19.99,
            "attributes": {
                "color": "red",
                "size": "large",
                "materials": ["plastic", "metal"]
            },
            "reviews": [
                {"rating": 5, "comment": "Great!"},
                {"rating": 4, "comment": "Good"}
            ]
        }
    ]
}

# Optimized processing
result = tm.flatten(
    ecommerce_data,
    batch_size=3000,
    arrays="separate",  # Extract reviews as separate table
    id_field={"products": "id"},  # Use product ID
    preserve_types=True  # Keep price as number
)
```

### Log Data Processing

Optimize for log file processing:

```python
# Log entry structure
log_entries = [
    {
        "timestamp": "2024-01-01T12:00:00Z",
        "level": "INFO",
        "message": "User action",
        "context": {
            "user_id": "user_123",
            "session_id": "sess_456",
            "metadata": {
                "ip": "192.168.1.1",
                "user_agent": "Mozilla/5.0..."
            }
        }
    }
]

# Optimized for log processing
result = tm.flatten(
    log_entries,
    batch_size=10000,  # Large batches for simple structures
    preserve_types=False,  # Everything as strings
    skip_empty=True,  # Remove empty fields
    arrays="skip",  # Skip arrays in logs
    separator="."  # Use dot notation
)
```

### Sensor Data Processing

Optimize for IoT sensor data:

```python
# Sensor readings
sensor_data = [
    {
        "device_id": "sensor_001",
        "timestamp": "2024-01-01T12:00:00Z",
        "readings": {
            "temperature": 23.5,
            "humidity": 65.2,
            "pressure": 1013.25
        },
        "location": {
            "lat": 40.7128,
            "lon": -74.0060
        }
    }
]

# Optimized for numerical data
result = tm.flatten(
    sensor_data,
    batch_size=5000,
    preserve_types=True,  # Keep numerical precision
    arrays="inline",  # Simple structure
    id_field="device_id",  # Use device ID
    add_timestamp=True  # Add processing timestamp
)
```
