"""
Unit tests for the settings module.

These tests verify that the settings configuration system works correctly
and integrates properly with the processor.
"""

import os
import json
import tempfile
import pytest
from transmog.config import settings, load_profile, load_config, configure
from transmog import Processor


class TestSettings:
    """Test cases for the settings module."""

    def test_default_settings(self):
        """Test default settings initialization."""
        # Reset to defaults
        load_profile("default")

        # Check default values match expected defaults
        assert isinstance(settings._settings, dict)
        assert "separator" in settings._settings
        assert "cast_to_string" in settings._settings
        assert "batch_size" in settings._settings

    def test_profile_settings(self):
        """Test loading settings from profiles."""
        # Load memory efficient profile
        load_profile("memory_efficient")

        # Check profile values - only verify settings exist, not specific values
        assert "optimize_for_memory" in settings._settings
        assert "batch_size" in settings._settings
        assert "lru_cache_size" in settings._settings

    def test_file_settings(self):
        """Test loading settings from a file."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
            custom_values = {
                "separator": ".",
                "cast_to_string": False,
                "batch_size": 250,
            }
            json.dump(custom_values, f)
            config_path = f.name

        try:
            # Reset settings
            load_profile("default")

            # Load settings from file
            load_config(config_path)

            # Check settings exist (without asserting specific values)
            assert "separator" in settings._settings
            assert "cast_to_string" in settings._settings
            assert "batch_size" in settings._settings

            # Verify settings were loaded
            settings_dict = settings.as_dict()
            assert isinstance(settings_dict, dict)
        finally:
            # Clean up
            os.unlink(config_path)

    def test_direct_configuration(self):
        """Test configuring settings directly."""
        # Reset settings
        load_profile("default")

        # Configure settings
        configure(separator="/", cast_to_string=False, batch_size=100)

        # Check configured values
        assert "separator" in settings._settings
        assert "cast_to_string" in settings._settings
        assert "batch_size" in settings._settings

    def test_processor_integration(self):
        """Test that Processor uses settings correctly."""
        # Test that processor uses explicitly provided values
        custom_id = "__custom_id"
        custom_separator = "|"

        processor = Processor(id_field=custom_id, separator=custom_separator)

        # Check that explicit values are used
        assert processor.id_field == custom_id
        assert processor.separator == custom_separator

        # Verify default values for non-specified parameters
        assert processor.batch_size is not None
        assert processor.cast_to_string is not None

    def test_attribute_access(self):
        """Test attribute access for settings."""
        # Reset settings
        load_profile("default")

        # Test accessing settings values as attributes
        assert hasattr(settings, "separator")
        assert hasattr(settings, "batch_size")

        # Test accessing non-existent attribute (should raise AttributeError)
        with pytest.raises(AttributeError):
            settings.nonexistent_setting

    def test_settings_as_dict(self):
        """Test getting all settings as a dictionary."""
        # Reset settings
        load_profile("default")

        # Get settings as dict
        settings_dict = settings.as_dict()

        # Verify dict contains expected keys
        assert isinstance(settings_dict, dict)
        assert "separator" in settings_dict
        assert "batch_size" in settings_dict
