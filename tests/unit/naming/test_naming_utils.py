"""Tests for naming utilities module."""

import pytest

from transmog.naming.utils import get_table_name_for_array


class TestGetTableNameForArray:
    """Test table name generation for arrays."""

    def test_first_level_array(self):
        """Table name generated correctly for first-level arrays."""
        result = get_table_name_for_array(
            entity_name="user", array_name="addresses", parent_path="", separator="_"
        )
        assert result == "user_addresses"

    def test_nested_array(self):
        """Table name generated correctly for nested arrays."""
        result = get_table_name_for_array(
            entity_name="company",
            array_name="employees",
            parent_path="departments",
            separator="_",
        )
        assert result == "company_departments_employees"

    def test_deeply_nested_array_default_threshold(self):
        """Table name simplified for deeply nested arrays with default threshold."""
        result = get_table_name_for_array(
            entity_name="org",
            array_name="items",
            parent_path="level1_level2_level3",
            separator="_",
        )
        # With default threshold of 4, this should be simplified
        assert result == "org_level1_nested_items"

    def test_deeply_nested_array_custom_threshold(self):
        """Table name simplified for deeply nested arrays with custom threshold."""
        result = get_table_name_for_array(
            entity_name="org",
            array_name="items",
            parent_path="level1_level2",
            separator="_",
            deeply_nested_threshold=3,
        )
        # With threshold of 3, this should be simplified
        assert result == "org_level1_nested_items"

    def test_custom_separator(self):
        """Table name generated correctly with custom separator."""
        result = get_table_name_for_array(
            entity_name="user",
            array_name="addresses",
            parent_path="contact",
            separator=".",
        )
        assert result == "user.contact.addresses"

    def test_empty_entity_name(self):
        """Table name generated correctly with empty entity name."""
        result = get_table_name_for_array(
            entity_name="", array_name="items", parent_path="", separator="_"
        )
        assert result == "_items"

    def test_empty_array_name(self):
        """Table name generated correctly with empty array name."""
        result = get_table_name_for_array(
            entity_name="user", array_name="", parent_path="", separator="_"
        )
        assert result == "user_"

    def test_unicode_names(self):
        """Table name generated correctly with unicode characters."""
        result = get_table_name_for_array(
            entity_name="用户", array_name="地址", parent_path="联系", separator="_"
        )
        assert result == "用户_联系_地址"

    def test_special_characters_in_names(self):
        """Table name generated correctly with special characters."""
        result = get_table_name_for_array(
            entity_name="user-data",
            array_name="home-addresses",
            parent_path="contact-info",
            separator="_",
        )
        assert result == "user-data_contact-info_home-addresses"

    def test_very_long_path(self):
        """Table name simplified correctly for very long paths."""
        long_path = "_".join([f"level{i}" for i in range(1, 10)])
        result = get_table_name_for_array(
            entity_name="root", array_name="items", parent_path=long_path, separator="_"
        )
        # Should be simplified to root_level1_nested_items
        assert result == "root_level1_nested_items"

    def test_threshold_edge_case(self):
        """Table name handling at threshold boundary."""
        # Exactly at threshold - should be simplified
        result = get_table_name_for_array(
            entity_name="org",
            array_name="items",
            parent_path="a_b_c",  # 3 components + entity + array = 5 total
            separator="_",
            deeply_nested_threshold=4,
        )
        assert result == "org_a_nested_items"

        # Just under threshold - should not be simplified
        result = get_table_name_for_array(
            entity_name="org",
            array_name="items",
            parent_path="a_b",  # 2 components + entity + array = 4 total
            separator="_",
            deeply_nested_threshold=4,
        )
        assert result == "org_a_b_items"
