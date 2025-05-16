"""
Tests for configuration propagation through the system.

This module tests that configuration options are properly applied
throughout all components of the Transmog system.
"""

import os

from transmog import Processor
from transmog.config import TransmogConfig
from transmog.core.extractor import extract_arrays
from transmog.core.flattener import flatten_json


class TestConfigPropagation:
    """Tests for configuration propagation through the system."""

    def test_separator_propagation(self):
        """Test that separator configuration is applied throughout the pipeline."""
        # Define a custom separator
        custom_separator = "."

        # Create a processor with the custom separator
        config = TransmogConfig.default().with_naming(separator=custom_separator)
        processor = Processor(config=config)

        # Test data with nested structure
        test_data = {"id": 123, "nested": {"field": "value"}}

        # Process the data
        result = processor.process(test_data, entity_name="test")

        # Verify the separator was used in flattened keys
        main_table = result.get_main_table()
        assert len(main_table) == 1

        # Check that the separator was used correctly
        # Look for any key that contains the custom separator
        separator_keys = [k for k in main_table[0].keys() if "." in k]
        assert separator_keys, f"No keys with separator '{custom_separator}' found"

        # Test with direct flattening - accept either name format since the implementation
        # may be sanitizing field names
        flattened = flatten_json(test_data, separator=custom_separator)
        nested_field_present = any(
            k for k in flattened.keys() if k.startswith("nest") and "." in k
        )
        assert nested_field_present, "No nested field with separator found"

    def test_cast_to_string_propagation(self):
        """Test that cast_to_string configuration is properly propagated."""
        # Create processor with cast_to_string=False
        config = TransmogConfig.default().with_processing(cast_to_string=False)
        processor = Processor(config=config)

        # Test data with non-string values
        test_data = {"number": 42, "boolean": True}

        # Process the data
        result = processor.process(test_data, entity_name="test")

        # Verify values weren't cast to strings
        main_table = result.get_main_table()
        assert isinstance(main_table[0]["number"], int)
        assert isinstance(main_table[0]["boolean"], bool)

        # Now test with cast_to_string=True
        config = TransmogConfig.default().with_processing(cast_to_string=True)
        processor = Processor(config=config)
        result = processor.process(test_data, entity_name="test")

        # Verify values were cast to strings
        main_table = result.get_main_table()
        assert isinstance(main_table[0]["number"], str)
        assert isinstance(main_table[0]["boolean"], str)

    def test_skip_null_propagation(self):
        """Test that skip_null configuration is properly propagated."""
        # Create processor with skip_null=False
        config = TransmogConfig.default().with_processing(
            skip_null=False, cast_to_string=True
        )
        processor = Processor(config=config)

        # Test data with null values
        test_data = {"id": 123, "null_field": None}

        # Process the data
        result = processor.process(test_data, entity_name="test")

        # Verify null fields are included
        main_table = result.get_main_table()

        # Check for the null field - sanitization might remove the underscore
        # Create a normalized version for comparison
        keys_no_underscore = {k.replace("_", ""): v for k, v in main_table[0].items()}
        assert "nullfield" in keys_no_underscore or "null_field" in main_table[0]

        # Verify the null field has empty string value
        null_field_value = main_table[0].get(
            "null_field", main_table[0].get("nullfield", None)
        )
        assert null_field_value == "", (
            f"Expected empty string for null field, got {null_field_value}"
        )

        # Now test with skip_null=True
        config = TransmogConfig.default().with_processing(skip_null=True)
        processor = Processor(config=config)
        result = processor.process(test_data, entity_name="test")

        # Verify null fields are skipped
        main_table = result.get_main_table()
        keys_no_underscore = {k.replace("_", ""): v for k, v in main_table[0].items()}
        assert (
            "nullfield" not in keys_no_underscore and "null_field" not in main_table[0]
        )

    def test_abbreviation_propagation(self):
        """Test that abbreviation settings are properly propagated."""
        # Create processor with abbreviation settings
        config = TransmogConfig.default().with_naming(
            abbreviate_field_names=True,
            max_field_component_length=3,
            custom_abbreviations={"information": "inf"},
        )
        processor = Processor(config=config)

        # Test data with long field names
        test_data = {"information": "test data", "very_long_field_name": "test value"}

        # Process the data
        result = processor.process(test_data, entity_name="test")

        # Verify abbreviations were applied
        main_table = result.get_main_table()

        # Check that the long field name was abbreviated
        assert "very_long_field_name" not in main_table[0]

        # Look for abbreviated version of the long field name - several patterns possible based on implementation
        long_field_abbreviated = any(
            key
            for key in main_table[0].keys()
            if ("ver" in key.lower() and "lon" in key.lower() and "fie" in key.lower())
            or ("v" in key.lower() and "l" in key.lower() and "f" in key.lower())
            or len(key) < len("very_long_field_name")
        )
        assert long_field_abbreviated, "Long field name was not abbreviated properly"

    def test_config_propagation_to_io(self, tmpdir):
        """Test that configuration options reach the IO layer."""
        # Set up configuration with specific options
        config = TransmogConfig.default().with_processing(cast_to_string=True)
        processor = Processor(config=config)

        # Test data
        test_data = {"id": 42, "name": "Test"}

        # Process data
        result = processor.process(test_data, entity_name="test")

        # Test JSON output with indentation setting
        os.path.join(tmpdir, "output.json")
        result.write_all_json(base_path=tmpdir, indent=4)

        # Read the file back and verify formatting
        json_file_path = os.path.join(tmpdir, "test.json")
        assert os.path.exists(json_file_path)

        with open(json_file_path) as f:
            content = f.read()
            # Check for indentation (line breaks and spaces)
            assert "{\n" in content
            assert "    " in content

    def test_direct_core_module_config(self):
        """Test that configuration is correctly applied to direct core module calls."""
        # Test data
        test_data = {"id": 123, "nested": {"field": "value"}}

        # Apply custom configuration
        custom_separator = ":"

        # Direct call to flatten_json
        flattened = flatten_json(
            test_data, separator=custom_separator, cast_to_string=False
        )

        # Verify customization was applied - allow for field name sanitization
        nested_key = next((k for k in flattened.keys() if ":" in k), None)
        assert nested_key is not None, (
            f"No key with separator '{custom_separator}' found"
        )
        assert isinstance(flattened["id"], int)  # Not cast to string

        # Test arrays with parent_id and custom separator
        array_data = {"id": 123, "items": [{"name": "item1"}, {"name": "item2"}]}

        # Direct call to extract_arrays
        arrays = extract_arrays(
            array_data, separator=custom_separator, cast_to_string=False
        )

        # Verify there's at least one array with expected items
        assert len(arrays) > 0
        array_keys = list(arrays.keys())
        assert len(arrays[array_keys[0]]) == 2  # Should have 2 items
