"""Tests for naming conventions and abbreviation system.

This module contains tests for the naming conventions and abbreviation system
in Transmog, ensuring that table and field names are generated correctly.
"""

import pytest

from transmog.naming.abbreviator import abbreviate_component, abbreviate_table_name
from transmog.naming.conventions import get_table_name, sanitize_name


class TestNamingConventions:
    """Tests for naming conventions in Transmog."""

    def test_get_table_name_first_level(self):
        """Test table naming for first level arrays."""
        # First level array should follow <entity>_<arrayname> pattern
        result = get_table_name("orders", "customer")
        assert result == "customer_orders"

    def test_get_table_name_nested(self):
        """Test table naming for nested arrays."""
        # Nested array should follow <entity>_<path>_<arrayname> pattern
        result = get_table_name("items", "customer", parent_path="orders")
        assert result == "customer_orders_items"

        # Deeply nested array
        result = get_table_name("details", "customer", parent_path="orders/items")
        assert result == "customer_orders_items_details"

    def test_abbreviate_table_name(self):
        """Test abbreviate_table_name function."""
        # First level array
        result = abbreviate_table_name("items", "customer")
        assert result == "customer_items"

        # Second level with abbreviation directly using the underscore notation
        result = abbreviate_table_name(
            "orders_shipments", "customer", max_component_length=4
        )
        # With our new implementation in the extractor, abbreviation is done directly
        # This test is for backwards compatibility with existing code
        assert result == "customer_orders_shipments"

    def test_abbreviate_component(self):
        """Test component abbreviation."""
        # Default abbreviation (4 chars)
        result = abbreviate_component("information", max_length=4)
        assert result == "info"

        # Custom abbreviation length
        result = abbreviate_component("information", max_length=5)
        assert result == "infor"

        # Custom abbreviation dictionary
        custom_abbrevs = {"information": "INFO"}
        result = abbreviate_component(
            "information", max_length=4, abbreviation_dict=custom_abbrevs
        )
        assert result == "INFO"

    def test_sanitize_name(self):
        """Test name sanitization."""
        # Replace spaces
        result = sanitize_name("user name")
        assert result == "user_name"

        # Replace special characters
        result = sanitize_name("user@email.com")
        assert result == "user_email_com"

        # Test with SQL special character handling
        result = sanitize_name("user$name")
        assert result == "user_name"
