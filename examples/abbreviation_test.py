"""
Test script for abbreviation settings with deep paths.

This script demonstrates how the abbreviation system works
with the updated 4-character limit for intermediate components.
"""

import sys
import os
from pprint import pprint

# Add parent directory to path to import transmog
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import from transmog
from src.transmog.naming.abbreviator import (
    abbreviate_table_name,
    abbreviate_field_name,
    get_common_abbreviations,
)


def test_deep_path_abbreviation():
    """Test abbreviation with deep paths."""

    # Deep path example
    entity_name = "customers"
    path = "shipping_information_address_street"

    # Get default abbreviations
    abbreviations = get_common_abbreviations()

    # Test with default settings (4-char limit, preserve leaf)
    abbreviated_table = abbreviate_table_name(
        path,
        entity_name,
        separator="_",
        abbreviate_enabled=True,
        max_component_length=4,
        preserve_leaf=True,
        abbreviation_dict=abbreviations,
    )

    abbreviated_field = abbreviate_field_name(
        path,
        separator="_",
        abbreviate_enabled=True,
        max_component_length=4,
        preserve_leaf=True,
        abbreviation_dict=abbreviations,
    )

    print(f"Original path: {entity_name}_{path}")
    print(f"Abbreviated table name: {abbreviated_table}")
    print(f"Abbreviated field name: {abbreviated_field}")

    # Test with different component lengths
    for length in [2, 3, 4, 5, 10]:
        result = abbreviate_table_name(
            path,
            entity_name,
            max_component_length=length,
            preserve_leaf=True,
            abbreviation_dict=abbreviations,
        )
        print(f"Table with {length}-char limit: {result}")

    # Test with different paths
    paths = [
        "customers_shipping_information_address_street",
        "orders_line_items_product_details_manufacturer_information",
        "users_profile_preferences_notification_settings_email_frequency",
    ]

    print("\nMore examples with 4-char limit and leaf preservation:")
    for p in paths:
        abbreviated = abbreviate_field_name(
            p,
            max_component_length=4,
            preserve_leaf=True,
            abbreviation_dict=abbreviations,
        )
        print(f"Original: {p}")
        print(f"Abbreviated: {abbreviated}")
        print()


if __name__ == "__main__":
    test_deep_path_abbreviation()
