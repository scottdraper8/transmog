"""Memory management utilities for performance optimization.

This module provides memory monitoring, adaptive batch sizing, and
garbage collection management for memory-efficient processing.
"""

import gc
import logging
from typing import Any, Optional, Union

logger = logging.getLogger(__name__)

# Try to import psutil for memory monitoring
try:
    import psutil

    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    logger.debug("psutil not available - using fallback memory management")


class MemoryMonitor:
    """Monitor memory usage and pressure for adaptive processing."""

    def __init__(self, max_memory_percent: float = 0.8):
        """Initialize memory monitor with maximum memory percentage threshold."""
        self.max_memory_percent = max_memory_percent
        self._initial_memory = self.get_current_memory_mb()

    def get_current_memory_mb(self) -> float:
        """Get current memory usage in MB."""
        if PSUTIL_AVAILABLE:
            try:
                process = psutil.Process()
                return float(process.memory_info().rss) / (1024 * 1024)
            except Exception:
                # Log the exception for debugging but continue with fallback
                logger.debug("Failed to get memory info via psutil")

        # Fallback - estimate based on object count
        return gc.get_count()[0] * 0.001  # Very rough estimate

    def get_system_memory_info(self) -> dict[str, float]:
        """Get system memory information."""
        if PSUTIL_AVAILABLE:
            try:
                memory = psutil.virtual_memory()
                return {
                    "total_mb": memory.total / (1024 * 1024),
                    "available_mb": memory.available / (1024 * 1024),
                    "used_percent": memory.percent,
                }
            except Exception:
                # Log the exception for debugging but continue with fallback
                logger.debug("Failed to get system memory info via psutil")

        # Fallback
        return {
            "total_mb": 8192.0,  # Assume 8GB
            "available_mb": 4096.0,  # Assume 4GB available
            "used_percent": 50.0,
        }

    def get_memory_pressure(self) -> float:
        """Get memory pressure as percentage (0.0-1.0)."""
        if PSUTIL_AVAILABLE:
            try:
                memory = psutil.virtual_memory()
                return float(memory.percent) / 100.0
            except Exception:
                # Log the exception for debugging but continue with fallback
                logger.debug("Failed to get memory pressure via psutil")

        # Fallback - estimate based on current vs initial memory
        current = self.get_current_memory_mb()
        if current > self._initial_memory * 2:
            return 0.8  # High pressure
        elif current > self._initial_memory * 1.5:
            return 0.6  # Medium pressure
        else:
            return 0.3  # Low pressure

    def should_reduce_usage(self) -> bool:
        """Check if memory usage should be reduced."""
        return self.get_memory_pressure() > self.max_memory_percent

    def get_memory_used_mb(self) -> float:
        """Get memory used since initialization."""
        return self.get_current_memory_mb() - self._initial_memory


class AdaptiveBatchSizer:
    """Dynamically adjust batch sizes based on memory pressure."""

    def __init__(
        self, initial_size: int = 1000, min_size: int = 50, max_size: int = 10000
    ):
        """Initialize batch size adjuster with size limits."""
        self.initial_size = initial_size
        self.min_size = min_size
        self.max_size = max_size
        self.current_size = initial_size
        self.monitor = MemoryMonitor()

    def get_batch_size(self) -> int:
        """Get adaptive batch size based on current memory pressure."""
        pressure = self.monitor.get_memory_pressure()

        if pressure > 0.9:
            # Critical memory pressure - use minimum batch size
            self.current_size = self.min_size
        elif pressure > 0.8:
            # High memory pressure - reduce batch size significantly
            self.current_size = max(self.min_size, self.current_size // 4)
        elif pressure > 0.6:
            # Medium memory pressure - reduce batch size moderately
            self.current_size = max(self.min_size, self.current_size // 2)
        elif pressure < 0.3:
            # Low memory pressure - can increase batch size
            self.current_size = min(self.max_size, int(self.current_size * 1.5))

        return self.current_size

    def update_performance_feedback(
        self, processing_time: float, memory_used: float
    ) -> None:
        """Update batch size based on performance feedback."""
        # If processing is slow and memory usage is high, reduce batch size
        if processing_time > 10.0 and memory_used > 100.0:  # 10s and 100MB
            self.current_size = max(self.min_size, int(self.current_size * 0.8))
        # If processing is fast and memory usage is low, increase batch size
        elif processing_time < 2.0 and memory_used < 50.0:  # 2s and 50MB
            self.current_size = min(self.max_size, int(self.current_size * 1.2))


class GCManager:
    """Strategic garbage collection management."""

    def __init__(self, gc_frequency: int = 1000):
        """Initialize garbage collection manager with processing frequency."""
        self.gc_frequency = gc_frequency
        self.processed_count = 0
        self.total_collected = 0

    def maybe_collect(self, force: bool = False) -> int:
        """Conditionally trigger garbage collection."""
        self.processed_count += 1

        if force or self.processed_count % self.gc_frequency == 0:
            collected = gc.collect()
            self.total_collected += collected

            if collected > 0:
                logger.debug(
                    f"GC collected {collected} objects (total: {self.total_collected})"
                )

            return collected

        return 0

    def collect_after_batch(self) -> int:
        """Force collection after processing a batch."""
        return self.maybe_collect(force=True)

    def get_gc_stats(self) -> dict[str, int]:
        """Get garbage collection statistics."""
        counts = gc.get_count()
        return {
            "generation_0": counts[0],
            "generation_1": counts[1],
            "generation_2": counts[2],
            "total_collected": self.total_collected,
        }


def estimate_object_memory_mb(obj: Any) -> float:
    """Estimate memory usage of an object in MB."""
    try:
        import sys

        size_bytes = sys.getsizeof(obj)

        # For containers, estimate content size
        if isinstance(obj, dict):
            for key, value in obj.items():
                size_bytes += sys.getsizeof(key) + sys.getsizeof(value)
        elif isinstance(obj, (list, tuple)):
            for item in obj:
                size_bytes += sys.getsizeof(item)

        return size_bytes / (1024 * 1024)
    except Exception:
        # Fallback estimation
        if isinstance(obj, dict):
            return len(obj) * 0.001  # ~1KB per dict entry
        elif isinstance(obj, (list, tuple)):
            return len(obj) * 0.0005  # ~0.5KB per list item
        else:
            return 0.001  # ~1KB for other objects


def get_memory_efficient_config(
    available_memory_mb: Optional[float] = None,
) -> dict[str, Union[int, float]]:
    """Get memory-efficient configuration based on available memory."""
    if available_memory_mb is None:
        monitor = MemoryMonitor()
        memory_info = monitor.get_system_memory_info()
        available_memory_mb = memory_info["available_mb"]

    # Conservative memory usage - use 10% of available memory for processing
    target_memory_mb = available_memory_mb * 0.1

    # Estimate configuration values
    batch_size = max(50, min(5000, int(target_memory_mb * 10)))  # ~100KB per record
    cache_size = max(
        100, min(10000, int(target_memory_mb * 200))
    )  # ~5KB per cache entry

    return {
        "batch_size": batch_size,
        "cache_size": cache_size,
        "gc_frequency": max(100, batch_size // 10),
        "target_memory_mb": target_memory_mb,
    }


# Global instances for reuse
_global_monitor = MemoryMonitor()
_global_batch_sizer = AdaptiveBatchSizer()
_global_gc_manager = GCManager()


def get_global_memory_monitor() -> MemoryMonitor:
    """Get the global memory monitor instance."""
    return _global_monitor


def get_global_batch_sizer() -> AdaptiveBatchSizer:
    """Get the global adaptive batch sizer instance."""
    return _global_batch_sizer


def get_global_gc_manager() -> GCManager:
    """Get the global garbage collection manager instance."""
    return _global_gc_manager
