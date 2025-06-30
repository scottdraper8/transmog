"""
Tests for error recovery strategies.

Tests different error recovery strategies and their behavior.
"""

import pytest
import logging
import threading
import time
from unittest.mock import patch, MagicMock

from transmog.error import (
    ProcessingError,
    ParsingError,
    FileError,
    TransmogError,
)
from transmog.error.recovery import (
    RecoveryStrategy,
    StrictRecovery,
    SkipAndLogRecovery,
    PartialProcessingRecovery,
    with_recovery,
)


class TestRecoveryStrategies:
    """Test recovery strategy implementations."""

    def test_strict_recovery_strategy(self):
        """Test strict recovery strategy behavior."""
        strategy = StrictRecovery()

        # Should re-raise all errors
        with pytest.raises(ProcessingError):
            strategy.recover(ProcessingError("test error"))

        with pytest.raises(ParsingError):
            strategy.recover(ParsingError("parsing error"))

        with pytest.raises(ValueError):
            strategy.recover(ValueError("value error"))

    def test_skip_and_log_recovery_strategy(self):
        """Test skip and log recovery strategy behavior."""
        strategy = SkipAndLogRecovery()

        # Should return None for batch operations
        result = strategy.recover(ProcessingError("test error"), data=[])
        assert result is None

        # Should return empty dict for single record operations
        result = strategy.recover(ProcessingError("test error"), data={})
        assert result == {}

        # Should handle different error types
        result = strategy.recover(ParsingError("parsing error"))
        assert result == {}

        result = strategy.recover(FileError("file error"))
        assert result == {}

    def test_partial_processing_recovery_strategy(self):
        """Test partial processing recovery strategy behavior."""
        strategy = PartialProcessingRecovery()

        # Should return partial results with error information
        result = strategy.recover(ProcessingError("test error"))

        # Check that result contains error information
        assert isinstance(result, dict)
        assert "_error" in result or "error" in result

    def test_recovery_strategy_interface(self):
        """Test that all recovery strategies implement the interface correctly."""
        strategies = [
            StrictRecovery(),
            SkipAndLogRecovery(),
            PartialProcessingRecovery(),
        ]

        for strategy in strategies:
            assert isinstance(strategy, RecoveryStrategy)
            assert hasattr(strategy, "recover")
            assert callable(strategy.recover)


class TestWithRecoveryDecorator:
    """Test the with_recovery decorator."""

    def test_with_recovery_strict_mode(self):
        """Test with_recovery decorator in strict mode."""

        @with_recovery(strategy=StrictRecovery())
        def failing_function():
            raise ProcessingError("test error")

        with pytest.raises(ProcessingError):
            failing_function()

    def test_with_recovery_lenient_mode(self):
        """Test with_recovery decorator in lenient mode."""

        @with_recovery(strategy=SkipAndLogRecovery())
        def failing_function():
            raise ProcessingError("test error")

        result = failing_function()
        # Should return empty dict from recovery
        assert result == {}

    def test_with_recovery_custom_strategy(self):
        """Test with_recovery decorator with custom strategy."""

        class CustomRecovery(RecoveryStrategy):
            def recover(self, error, **kwargs):
                return {"recovered": True, "error": str(error)}

        @with_recovery(strategy=CustomRecovery())
        def failing_function():
            raise ProcessingError("test error")

        result = failing_function()
        assert result["recovered"] is True
        assert "test error" in result["error"]

    def test_with_recovery_fallback_value(self):
        """Test with_recovery decorator with fallback value."""

        @with_recovery(fallback_value="fallback")
        def failing_function():
            raise ProcessingError("test error")

        result = failing_function()
        assert result == "fallback"


class TestErrorContextHandling:
    """Test error context handling in recovery strategies."""

    def test_error_context_preservation(self):
        """Test that error context is preserved during recovery."""
        strategy = PartialProcessingRecovery()

        error = ProcessingError("test error")
        result = strategy.recover(
            error,
            entity_name="test_entity",
            source="test_source",
            data={"field": "value"},
        )

        assert isinstance(result, dict)

    def test_nested_error_handling(self):
        """Test handling of nested errors."""
        strategy = SkipAndLogRecovery()

        # Test with nested exception
        try:
            raise ValueError("inner error")
        except ValueError as inner:
            outer_error = ProcessingError("outer error")
            outer_error.__cause__ = inner

            result = strategy.recover(outer_error)
            assert result == {}

    def test_error_logging(self):
        """Test that errors are properly logged."""
        with patch("transmog.error.recovery.logger") as mock_logger:
            strategy = SkipAndLogRecovery()

            strategy.recover(ProcessingError("test error"))

            # Should have logged the error
            mock_logger.log.assert_called()


class TestRecoveryStrategyConfiguration:
    """Test recovery strategy configuration options."""

    def test_strategy_configuration_parameters(self):
        """Test configuring recovery strategies with parameters."""
        # SkipAndLogRecovery with custom log level
        strategy = SkipAndLogRecovery(log_level=logging.ERROR)
        assert strategy.log_level == logging.ERROR

        # PartialProcessingRecovery with custom log level
        strategy = PartialProcessingRecovery(log_level=logging.DEBUG)
        assert strategy.log_level == logging.DEBUG

    def test_strategy_selection_based_on_error_type(self):
        """Test selecting recovery strategy based on error type."""
        strategy = PartialProcessingRecovery()

        # Different error types should be handled
        processing_result = strategy.recover(ProcessingError("processing error"))
        parsing_result = strategy.recover(ParsingError("parsing error"))
        file_result = strategy.recover(FileError("file error"))

        assert isinstance(processing_result, dict)
        assert isinstance(parsing_result, dict)
        assert isinstance(file_result, dict)


class TestRecoveryStrategyEdgeCases:
    """Test edge cases for recovery strategies."""

    def test_recovery_with_empty_context(self):
        """Test recovery with minimal context."""
        strategy = StrictRecovery()

        with pytest.raises(ProcessingError):
            strategy.recover(ProcessingError("test error"))

    def test_recovery_with_none_error(self):
        """Test recovery with None error (should not happen normally)."""
        strategy = SkipAndLogRecovery()

        # Should handle gracefully
        result = strategy.recover(ProcessingError("test error"))
        assert result == {}

    def test_recovery_with_large_context(self):
        """Test recovery with large context data."""
        strategy = PartialProcessingRecovery()

        large_data = {f"field_{i}": f"value_{i}" for i in range(1000)}
        result = strategy.recover(
            ProcessingError("test error"), data=large_data, entity_name="test"
        )

        assert isinstance(result, dict)

    def test_recovery_strategy_thread_safety(self):
        """Test that recovery strategies are thread-safe."""
        strategy = SkipAndLogRecovery()
        results = []
        errors = []

        def worker():
            try:
                result = strategy.recover(ProcessingError("test"))
                results.append(result)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker) for _ in range(10)]

        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Should have 10 successful results, no errors
        assert len(results) == 10
        assert len(errors) == 0
        assert all(result == {} for result in results)


class TestRecoveryStrategyIntegration:
    """Test integration of recovery strategies with data processing."""

    def test_recovery_strategy_with_data_processing(self):
        """Test recovery strategy integration with data processing."""
        strategy = PartialProcessingRecovery()

        # Simulate processing error with data context
        data = {"name": "test", "value": 42}
        result = strategy.recover(
            ProcessingError("processing failed"), data=data, entity_name="test_entity"
        )

        assert isinstance(result, dict)

    def test_recovery_strategy_performance(self):
        """Test recovery strategy performance with many errors."""
        strategy = SkipAndLogRecovery()

        start_time = time.time()

        # Process many errors
        for i in range(1000):
            strategy.recover(ProcessingError(f"error {i}"))

        end_time = time.time()
        processing_time = end_time - start_time

        # Should complete in reasonable time (less than 1 second)
        assert processing_time < 1.0
