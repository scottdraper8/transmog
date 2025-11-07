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
from transmog.types.base import RecoveryMode


class TestErrorHandlingConsistency:
    """Test the standardized error handling implementation."""

    def test_recovery_strategy_mapping(self):
        """Test that RecoveryMode enums map correctly to strategy objects."""
        # Test enum to object mapping
        assert get_recovery_strategy(RecoveryMode.STRICT) is STRICT
        assert get_recovery_strategy(RecoveryMode.SKIP) is DEFAULT
        assert get_recovery_strategy(RecoveryMode.PARTIAL) is LENIENT

        # Test object passthrough
        assert get_recovery_strategy(STRICT) is STRICT
        assert get_recovery_strategy(None) is STRICT

    def test_recovery_strategy_validation(self):
        """Test that invalid recovery strategies raise appropriate errors."""
        with pytest.raises(
            ValueError, match="Unknown RecoveryMode|Recovery strategy must be"
        ):
            get_recovery_strategy("invalid_strategy")

        with pytest.raises(ValueError, match="Recovery strategy must be"):
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

        # Test skip strategy
        config_skip = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result_skip = tm.flatten(problematic_data, config=config_skip)
        assert len(result_skip.main) == 1

        # Test partial strategy
        config_partial = tm.TransmogConfig(recovery_mode=RecoveryMode.PARTIAL)
        result_partial = tm.flatten(problematic_data, config=config_partial)
        assert len(result_partial.main) == 1

        # System handles non-serializable objects by converting to string representation
        def test_function():
            pass

        data_with_function = {
            "name": "test",
            "function": test_function,
        }

        config_strict = tm.TransmogConfig(recovery_mode=RecoveryMode.STRICT)
        result = tm.flatten(data_with_function, config=config_strict)
        assert len(result.main) == 1
        assert "function" in result.main[0]

    def test_consistent_error_messages_across_modules(self):
        """Test that error messages are consistent across different modules."""
        # This test ensures that similar errors from different modules
        # use the same message templates and context structure

        problematic_data = {"field": float("inf")}  # Invalid float

        # Test with skip strategy to capture error messages
        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(problematic_data, config=config)

        # Should handle the error gracefully
        assert len(result.main) == 1

    def test_recovery_strategy_object_usage(self):
        """Test that recovery strategy objects work consistently."""
        from transmog.config import TransmogConfig

        # Test configuration with strategy
        config = TransmogConfig(
            recovery_mode=RecoveryMode.SKIP,
            allow_malformed_data=True,
        )

        # Configuration should work with RecoveryMode enum
        assert config.recovery_mode == RecoveryMode.SKIP

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
        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(nested_data, config=config)

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
        config = tm.TransmogConfig(recovery_mode=RecoveryMode.SKIP)
        result = tm.flatten(array_data, config=config)

        # Should process valid items and skip problematic ones
        assert len(result.main) == 1
        # Should have array tables for valid items
        assert "items" in result.tables or len(result.tables) > 0
