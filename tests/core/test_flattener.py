"""
Tests for the flattener implementation.

This module tests the core flattener functionality using the interface-based approach.
"""

import pytest
from typing import Dict, List, Any, Optional

from transmog.core.flattener import flatten_json
from transmog.error import CircularReferenceError, ProcessingError
from transmog.config import TransmogConfig

# Import and inherit from the interface
from tests.interfaces.test_flattener_interface import AbstractFlattenerTest


class TestFlattener(AbstractFlattenerTest):
    """
    Tests for the flattener module.

    Inherits from AbstractFlattenerTest to ensure it follows the interface-based testing pattern.
    """

    def test_flattener_with_config(self, simple_data):
        """Test flattening with a TransmogConfig object."""
        # Create processor with explicit configuration
        proc_config = (
            TransmogConfig.default()
            .with_naming(separator="_", abbreviate_field_names=False)
            .with_processing(cast_to_string=False)
        )

        # Use the TransmogConfig to get the parameters
        flattened = flatten_json(
            simple_data,
            separator=proc_config.naming.separator,
            cast_to_string=proc_config.processing.cast_to_string,
            abbreviate_field_names=proc_config.naming.abbreviate_field_names,
        )

        # Check basic fields are preserved
        assert flattened["id"] == 1
        assert flattened["name"] == "Test"

        # Check nested fields are flattened
        assert "address_street" in flattened
        assert "address_city" in flattened
        assert "address_state" in flattened

    def test_sanitize_field_names(self):
        """Test the sanitize_field_names option."""
        # Create data with special characters in field names
        data = {
            "field with spaces": "value1",
            "field-with-hyphens": "value2",
            "field.with.dots": "value3",
            "nested": {
                "field+with+plus+signs": "nested value",
            },
        }

        # Sanitization happens by default
        flattened_sanitized = flatten_json(data)

        # Check sanitized field names - spaces and hyphens should be replaced with underscores
        assert "field_with_spaces" in flattened_sanitized
        assert "field_with_hyphens" in flattened_sanitized
        assert "field_with_dots" in flattened_sanitized
        assert "nested_fiel_with_plus_signs" in flattened_sanitized

        # Values should be preserved
        assert flattened_sanitized["field_with_spaces"] == "value1"
        assert flattened_sanitized["field_with_hyphens"] == "value2"
        assert flattened_sanitized["field_with_dots"] == "value3"
        assert flattened_sanitized["nested_fiel_with_plus_signs"] == "nested value"

    def test_in_place_option(self, simple_data):
        """Test the in_place option."""
        # Make a copy of the data for testing
        data_copy = dict(simple_data)

        # Flatten with in_place=True
        flattened = flatten_json(data_copy, in_place=True)

        # The flattened result should be the same object as the input
        assert flattened is data_copy

        # Check flattened fields
        assert "address_street" in flattened
        assert "address_city" in flattened
        assert "address_state" in flattened

        # The nested structure should be removed
        assert "address" not in flattened

    def test_error_handling(self):
        """Test error handling with non-serializable objects."""

        # Create a non-JSON-serializable object
        class BadObject:
            def __eq__(self, other):
                return False

            def __repr__(self):
                return "<BadObject>"

        # Create data with the bad object
        data = {"bad": BadObject()}

        # Test with lenient error handling
        with pytest.raises(ProcessingError):
            flatten_json(data, error_handling="lenient")

        # Test with strict error handling
        with pytest.raises(ProcessingError):
            flatten_json(data, error_handling="strict")
