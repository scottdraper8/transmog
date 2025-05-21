"""Tests for naming conventions.

This module contains tests for the naming conventions in Transmog,
ensuring that table and field names are generated correctly.
"""

from transmog.naming.conventions import (
    get_table_name,
    handle_deeply_nested_path,
    sanitize_name,
)


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

    def test_deeply_nested_path_handling(self):
        """Test handling of deeply nested paths."""
        # Path with depth less than threshold should remain unchanged
        result = get_table_name(
            "details", "customer", parent_path="orders_items", deeply_nested_threshold=4
        )
        assert result == "customer_orders_items_details"

        # Path with depth greater than threshold should be simplified
        result = get_table_name(
            "values",
            "customer",
            parent_path="orders_items_details_specifications",
            deeply_nested_threshold=4,
        )
        assert "nested" in result
        assert result == "customer_orders_nested_values"

        # Test with custom deeply nested threshold
        result = get_table_name(
            "data",
            "customer",
            parent_path="orders_items_details",
            deeply_nested_threshold=3,
        )
        assert "nested" in result
        assert result == "customer_orders_nested_data"

    def test_handle_deeply_nested_path(self):
        """Test the deeply nested path utility directly."""
        # Path with fewer components than threshold should be unchanged
        path = "level1_level2_level3"
        result = handle_deeply_nested_path(path, deeply_nested_threshold=4)
        assert result == path

        # Path with more components than threshold should be simplified
        path = "level1_level2_level3_level4_level5"
        result = handle_deeply_nested_path(path, deeply_nested_threshold=4)
        assert result == "level1_nested_level5"

        # Test with custom separator
        path = "level1.level2.level3.level4.level5"
        result = handle_deeply_nested_path(
            path, separator=".", deeply_nested_threshold=4
        )
        assert result == "level1.nested.level5"

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

        # Test with numeric prefix
        result = sanitize_name("123_column")
        assert result == "col_123_column"

        # Test preservation of separators
        result = sanitize_name("user_email_com", preserve_separator=True)
        assert result == "user_email_com"
