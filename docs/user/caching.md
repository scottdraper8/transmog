# Value Processing Cache

Transmog includes a value processing cache to improve performance when processing large datasets with
repeated values. This document explains how to configure and use this cache system.

## Cache Configuration

The cache system can be configured through the `TransmogConfig` object:

```python
import transmog as tm

# Default configuration (enabled with default settings)
processor = tm.Processor()

# Configure with disabled cache for memory-sensitive applications
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(enabled=False)
)

# Configure with larger cache for performance-critical applications
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(maxsize=50000)
)

# Configure to clear cache after batch processing to prevent memory growth
processor = tm.Processor(
    tm.TransmogConfig.default().with_caching(clear_after_batch=True)
)
```

## Configuration Options

The cache configuration has the following options:

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | bool | `True` | Whether caching is enabled |
| `maxsize` | int | `10000` | Maximum number of entries in the LRU cache |
| `clear_after_batch` | bool | `False` | Whether to clear cache after batch processing |

## Predefined Configurations

Transmog provides predefined configurations for common use cases:

- **Default Configuration**:
  - Cache enabled
  - Cache size: 10,000 entries
  - No automatic clearing

- **Memory-Optimized Configuration** (`Processor.memory_optimized()`):
  - Cache enabled
  - Cache size: 1,000 entries (smaller to save memory)
  - Automatic clearing after batch processing

- **Performance-Optimized Configuration** (`Processor.performance_optimized()`):
  - Cache enabled
  - Cache size: 50,000 entries (larger for better hit rate)
  - No automatic clearing

## Manual Cache Management

You can manually clear the cache at any point:

```python
# Clear cache manually
processor.clear_cache()
```

## Cache Behavior

The cache is implemented as an LRU (Least Recently Used) cache, which means:

1. Once the cache reaches its maximum size, the least recently used items are removed first
2. Only simple scalar values (strings, numbers, booleans) are cached
3. Complex objects (dictionaries, lists, etc.) are processed without caching

## When to Adjust Cache Settings

Consider adjusting cache settings in these scenarios:

- **Large Datasets with High Value Diversity**: Increase cache size to improve hit rate
- **Limited Memory Environments**: Reduce cache size or disable caching
- **Long-Running Processes**: Enable `clear_after_batch` to prevent memory accumulation
- **High Value Repetition**: Increase cache size to maximize performance benefit

## Technical Implementation

The cache implementation uses Python's `functools.lru_cache` decorator with a configurable maximum size.
This provides an efficient, thread-safe caching mechanism with minimal overhead.
