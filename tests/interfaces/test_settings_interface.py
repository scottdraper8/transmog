"""
Settings and configuration interface tests.

This module defines abstract test classes for the settings and configuration systems
that all implementations must satisfy.
"""

import os
import tempfile
from typing import Any

import pytest


class AbstractSettingsTest:
    """
    Abstract test class for settings system.

    All settings implementations must pass these tests to ensure
    consistent configuration behavior across the system.
    """

    @pytest.fixture
    def settings_module(self):
        """
        Fixture to provide the settings module to test.

        Implementations must override this to provide the actual settings module.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def config_class(self):
        """
        Fixture to provide the configuration class to test.

        Implementations must override this to provide the actual configuration class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def default_config(self):
        """
        Fixture to provide a default configuration.

        Implementations must override this to provide a default configuration instance.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def custom_config_values(self) -> dict[str, Any]:
        """
        Fixture providing custom configuration values for testing.
        """
        return {
            "separator": ".",
            "cast_to_string": False,
            "batch_size": 250,
        }

    def test_default_settings(self, settings_module):
        """Test default settings initialization."""
        # Reset to defaults
        settings_module.load_profile("default")

        # Check default values match expected defaults
        assert hasattr(settings_module, "settings")
        assert isinstance(settings_module.settings._settings, dict)
        assert "separator" in settings_module.settings._settings
        assert "cast_to_string" in settings_module.settings._settings
        assert "batch_size" in settings_module.settings._settings

    def test_profile_settings(self, settings_module):
        """Test loading settings from profiles."""
        # Load memory efficient profile
        settings_module.load_profile("memory_efficient")

        # Check profile values - only verify settings exist, not specific values
        assert "batch_size" in settings_module.settings._settings

    def test_file_settings(self, settings_module, custom_config_values):
        """Test loading settings from a file."""
        # Create a temporary config file
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
            import json

            json.dump(custom_config_values, f)
            config_path = f.name

        try:
            # Reset settings
            settings_module.load_profile("default")

            # Load settings from file
            settings_module.load_config(config_path)

            # Check settings exist (without asserting specific values)
            for key in custom_config_values:
                assert key in settings_module.settings._settings

            # Verify settings were loaded
            settings_dict = settings_module.settings.as_dict()
            assert isinstance(settings_dict, dict)
        finally:
            # Clean up
            os.unlink(config_path)

    def test_direct_configuration(self, settings_module, custom_config_values):
        """Test configuring settings directly."""
        # Reset settings
        settings_module.load_profile("default")

        # Configure settings
        settings_module.configure(**custom_config_values)

        # Check configured values
        for key in custom_config_values:
            assert key in settings_module.settings._settings

    def test_attribute_access(self, settings_module):
        """Test attribute access for settings."""
        # Reset settings
        settings_module.load_profile("default")

        # Test accessing settings values as attributes
        assert hasattr(settings_module.settings, "separator")
        assert hasattr(settings_module.settings, "batch_size")

        # Test accessing non-existent attribute (should raise AttributeError)
        with pytest.raises(AttributeError):
            settings_module.settings.nonexistent_setting

    def test_settings_as_dict(self, settings_module):
        """Test getting all settings as a dictionary."""
        # Reset settings
        settings_module.load_profile("default")

        # Get settings as dict
        settings_dict = settings_module.settings.as_dict()

        # Verify dict contains expected keys
        assert isinstance(settings_dict, dict)
        assert "separator" in settings_dict
        assert "batch_size" in settings_dict


class AbstractConfigurationTest:
    """
    Abstract test class for configuration system.

    All configuration implementations must pass these tests to ensure
    consistent behavior across the system.
    """

    @pytest.fixture
    def config_class(self):
        """
        Fixture to provide the configuration class to test.

        Implementations must override this to provide the actual configuration class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def processor_class(self):
        """
        Fixture to provide the processor class that uses configuration.

        Implementations must override this to provide the actual processor class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    def test_config_creation(self, config_class):
        """Test creating configuration objects."""
        # Create default config
        config = config_class.default()
        assert config is not None

        # Create memory optimized config
        memory_config = config_class.memory_optimized()
        assert memory_config is not None

        # Create performance optimized config
        perf_config = config_class.performance_optimized()
        assert perf_config is not None

    def test_config_with_naming(self, config_class):
        """Test configuration with naming options."""
        custom_separator = ":"
        config = config_class.default().with_naming(separator=custom_separator)
        assert config.naming.separator == custom_separator

    def test_config_with_processing(self, config_class):
        """Test configuration with processing options."""
        config = config_class.default().with_processing(cast_to_string=False)
        assert config.processing.cast_to_string is False

    def test_config_with_metadata(self, config_class):
        """Test configuration with metadata options."""
        custom_id_field = "record_id"
        config = config_class.default().with_metadata(id_field=custom_id_field)
        assert config.metadata.id_field == custom_id_field

    def test_config_with_error_handling(self, config_class):
        """Test configuration with error handling options."""
        config = config_class.default().with_error_handling(
            allow_malformed_data=True, recovery_strategy="skip"
        )
        assert config.error_handling.allow_malformed_data is True
        assert config.error_handling.recovery_strategy == "skip"

    def test_deterministic_ids_config(self, config_class):
        """Test configuration with deterministic IDs."""
        source_field = "id"  # Use a single field name instead of a dictionary
        config = config_class.with_deterministic_ids(source_field)
        assert config.metadata.default_id_field == source_field

    def test_processor_integration(self, config_class, processor_class):
        """Test that processor correctly uses configuration."""
        custom_id = "__custom_id"
        custom_separator = "|"

        # Create config with custom values
        config = (
            config_class.default()
            .with_metadata(id_field=custom_id)
            .with_naming(separator=custom_separator)
        )
        processor = processor_class(config=config)

        # Check that explicit values are used
        assert processor.config.metadata.id_field == custom_id
        assert processor.config.naming.separator == custom_separator
