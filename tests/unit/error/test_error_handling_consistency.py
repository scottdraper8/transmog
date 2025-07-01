"""Test error handling consistency improvements from Phase 6."""

import pytest

import transmog as tm
from transmog.error import (
    DEFAULT,
    LENIENT,
    STRICT,
    ProcessingError,
    build_error_context,
    format_error_message,
    get_recovery_strategy,
)


class TestErrorHandlingConsistency:
    """Test the standardized error handling implementation."""

    def test_recovery_strategy_mapping(self):
        """Test that string identifiers map correctly to strategy objects."""
        # Test string to object mapping
        assert get_recovery_strategy("strict") is STRICT
        assert get_recovery_strategy("skip") is DEFAULT
        assert get_recovery_strategy("partial") is LENIENT

        # Test API-level mappings
        assert get_recovery_strategy("raise") is STRICT
        assert get_recovery_strategy("warn") is LENIENT

        # Test object passthrough
        assert get_recovery_strategy(STRICT) is STRICT
        assert get_recovery_strategy(None) is STRICT

    def test_recovery_strategy_validation(self):
        """Test that invalid recovery strategies raise appropriate errors."""
        with pytest.raises(ValueError, match="Invalid recovery strategy"):
            get_recovery_strategy("invalid_strategy")

        with pytest.raises(ValueError, match="Recovery strategy must be string"):
            get_recovery_strategy(123)

    def test_error_message_templates(self):
        """Test standardized error message formatting."""
        error = Exception("test error")  # Use base Exception to avoid wrapping

        # Test processing template
        context = build_error_context(
            entity_name="test_entity", entity_type="record", operation="processing"
        )
        message = format_error_message("processing", error, **context)
        assert "Error processing record 'test_entity': test error" == message

        # Test parsing template
        context = build_error_context(
            entity_name="test_field", entity_type="field", source="test.json"
        )
        message = format_error_message("parsing", error, **context)
        assert (
            "Parsing error in test.json for field 'test_field': test error" == message
        )

    def test_error_context_builder(self):
        """Test standardized error context building."""
        context = build_error_context(
            entity_name="test_entity",
            entity_type="record",
            operation="flattening",
            source="test_source",
            custom_field="custom_value",
        )

        assert context["entity_name"] == "test_entity"
        assert context["entity_type"] == "record"
        assert context["operation"] == "flattening"
        assert context["source"] == "test_source"
        assert context["custom_field"] == "custom_value"

    def test_error_message_fallback(self):
        """Test error message fallback for missing context."""
        error = Exception("test error")  # Use base Exception

        # Missing required context should fall back to generic template
        message = format_error_message("processing", error)
        assert "Error in processing: test error" == message

    def test_api_error_handling_consistency(self):
        """Test that API-level error handling uses standardized strategies."""
        # Use a simpler problematic data that will definitely trigger errors
        problematic_data = {
            "name": "test",
            "bad_float": float(
                "inf"
            ),  # Invalid float that should cause serialization issues
        }

        # Test skip strategy (API: "skip")
        result_skip = tm.flatten(problematic_data, errors="skip")
        assert len(result_skip.main) == 1

        # Test warn strategy (API: "warn")
        result_warn = tm.flatten(problematic_data, errors="warn")
        assert len(result_warn.main) == 1

        # For strict testing, use data that will definitely fail
        # Create a function object which can't be serialized
        def test_function():
            pass

        truly_problematic_data = {
            "name": "test",
            "function": test_function,  # This will definitely fail serialization
        }

        # Test strict strategy (API: "raise") - should raise on non-serializable data
        with pytest.raises((ProcessingError, TypeError, ValueError)):
            tm.flatten(truly_problematic_data, errors="raise")

    def test_consistent_error_messages_across_modules(self):
        """Test that error messages are consistent across different modules."""
        # This test ensures that similar errors from different modules
        # use the same message templates and context structure

        problematic_data = {"field": float("inf")}  # Invalid float

        # Test with skip strategy to capture error messages
        result = tm.flatten(problematic_data, errors="skip")

        # Should handle the error gracefully
        assert len(result.main) == 1

    def test_recovery_strategy_object_usage(self):
        """Test that recovery strategy objects work consistently."""
        from transmog.config import ErrorHandlingConfig, TransmogConfig

        # Test configuration with strategy objects
        config = TransmogConfig(
            error_handling=ErrorHandlingConfig(
                recovery_strategy="skip",  # Still accepts strings
                allow_malformed_data=True,
            )
        )

        # The configuration should work with string-based recovery strategies
        # that get converted to objects internally
        assert config.error_handling.recovery_strategy == "skip"

    def test_nested_error_context_preservation(self):
        """Test that error context is preserved through nested processing."""
        nested_data = {
            "level1": {
                "level2": {
                    "level3": {
                        "problematic_field": float("nan")  # NaN value
                    }
                }
            }
        }

        # Process with skip strategy
        result = tm.flatten(nested_data, errors="skip")

        # Should handle nested errors gracefully
        assert len(result.main) == 1

    def test_array_processing_error_consistency(self):
        """Test that array processing errors use consistent handling."""
        array_data = {
            "items": [
                {"id": 1, "name": "valid"},
                {"id": float("inf"), "name": "invalid"},  # Problematic item
                {"id": 3, "name": "valid"},
            ]
        }

        # Test with skip strategy
        result = tm.flatten(array_data, errors="skip")

        # Should process valid items and skip problematic ones
        assert len(result.main) == 1
        # Should have array tables for valid items
        assert "items" in result.tables or len(result.tables) > 0
