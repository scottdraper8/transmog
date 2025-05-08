# Transmog Cache Refactoring Plan

## Current Approach

The current caching implementation maintains two separate global dictionaries (`_standard_process_cache` and `_streaming_process_cache`) with complex key generation logic that differentiates between mutable/immutable objects. It uses selective caching based on value types and lacks mechanisms to bound memory usage or clear the cache automatically.

## Refactoring Goals

1. Simplify cache implementation
2. Prevent unbounded memory growth
3. Provide configurable cache behavior
4. Improve performance with less complexity

## Implementation Plan

### Phase 1: Replace Global Caches with LRU Cache

1. Replace separate global dictionaries with a single `functools.lru_cache` decorator
2. Add configurable maxsize to limit memory usage
3. Simplify the value processing wrapper function
4. Remove context-specific cache selection logic

```python
import functools

# Configure with a reasonable default size
@functools.lru_cache(maxsize=10000)
def _process_value_cached(value_str, cast_to_string, include_empty, skip_null):
    """Process a value with caching based on string representation."""
    return _process_value(value_str, cast_to_string, include_empty, skip_null)

def _process_value_wrapper(value, cast_to_string, include_empty, skip_null):
    """Simplified wrapper that handles edge cases and uses LRU cache."""
    # Special values handled directly without caching
    if value is None or value == "":
        return _process_value(value, cast_to_string, include_empty, skip_null)
    
    # For simple scalars, convert to string for caching
    if isinstance(value, (int, float, bool, str)):
        value_str = str(value)
        return _process_value_cached(value_str, cast_to_string, include_empty, skip_null)
    
    # For complex objects, process directly without caching
    return _process_value(value, cast_to_string, include_empty, skip_null)
```

### Phase 2: Add Configuration Options

1. Add cache configuration to `TransmogConfig`
2. Expose maxsize and cache enable/disable options
3. Connect configuration to cache implementation

```python
class CacheConfig:
    def __init__(self, enabled=True, maxsize=10000):
        self.enabled = enabled
        self.maxsize = maxsize

# Add to TransmogConfig
def with_caching(self, enabled=True, maxsize=10000):
    """Configure caching behavior."""
    self.cache_config = CacheConfig(enabled=enabled, maxsize=maxsize)
    return self
```

### Phase 3: Implement Cache Clearing API

1. Add cache clearing functions to processor
2. Connect cache clearing to processing boundaries
3. Add explicit cache clearing option in API

```python
def clear_caches():
    """Clear all processing caches."""
    _process_value_cached.cache_clear()

# Add to Processor class
def clear_cache(self):
    """Clear the processor's caches."""
    clear_caches()
    return self

# Add automatic clearing to batch processing
def process_batch(self, batch_data, entity_name, extract_time=None):
    result = self._process_batch_internal(batch_data, entity_name, extract_time)
    if self.config.cache_config.clear_after_batch:
        self.clear_cache()
    return result
```

### Phase 4: Optimize for Common Cases

1. Add specialized fast paths for common data patterns
2. Implement cache statistics (hits/misses) for performance tuning
3. Add documentation on cache behavior and configuration

## Config Options Reference

```python
# Default configuration
processor = Processor()

# Disable caching completely
processor = Processor(
    TransmogConfig.default().with_caching(enabled=False)
)

# Configure cache size
processor = Processor(
    TransmogConfig.default().with_caching(maxsize=50000)
)

# Clear cache after batch processing
processor = Processor(
    TransmogConfig.default().with_caching(clear_after_batch=True)
)

# Manually clear cache
processor.clear_cache()
```
