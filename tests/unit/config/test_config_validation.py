"""
Tests for configuration validation in Transmog v1.1.0.

Tests validation of configuration parameters, type checking, and error handling.
"""

import pytest

from transmog.config import (
    MetadataConfig,
    NamingConfig,
    ProcessingConfig,
    TransmogConfig,
)
from transmog.config.naming import NamingOptions
from transmog.config.process import ProcessingConfig as ProcessingConfigClass
from transmog.error import ConfigurationError, ValidationError


class TestTransmogConfigValidation:
    """Test TransmogConfig validation."""

    def test_default_config_creation(self):
        """Test creating default configuration."""
        config = TransmogConfig()

        # Check that all components exist
        assert hasattr(config, "naming")
        assert hasattr(config, "processing")
        assert hasattr(config, "metadata")
        assert hasattr(config, "error_handling")
        assert hasattr(config, "cache_config")

    def test_with_naming_validation(self):
        """Test naming configuration validation."""
        config = TransmogConfig()

        # Valid separator
        updated_config = config.with_naming(separator="_")
        assert updated_config.naming.separator == "_"

        # Invalid separator should raise error
        with pytest.raises((ValidationError, ValueError, ConfigurationError)):
            config.with_naming(separator="")

    def test_with_processing_validation(self):
        """Test processing configuration validation."""
        config = TransmogConfig()

        # Valid batch size
        updated_config = config.with_processing(batch_size=1000)
        assert updated_config.processing.batch_size == 1000

        # Invalid batch size should raise error
        with pytest.raises((ValidationError, ValueError, ConfigurationError)):
            config.with_processing(batch_size=-1)

    def test_factory_methods(self):
        """Test configuration factory methods."""
        # Memory optimized
        memory_config = TransmogConfig.memory_optimized()
        assert memory_config.processing.batch_size < 1000

        # Performance optimized
        perf_config = TransmogConfig.performance_optimized()
        assert perf_config.processing.batch_size > 1000

        # CSV optimized
        csv_config = TransmogConfig.csv_optimized()
        assert csv_config.processing.cast_to_string is True


class TestNamingOptionsValidation:
    """Test NamingOptions validation."""

    def test_valid_separator_validation(self):
        """Test valid separator values."""
        valid_separators = ["_", "-", ".", "|"]

        for sep in valid_separators:
            config = NamingOptions(separator=sep)
            assert config.separator == sep

    def test_invalid_separator_validation(self):
        """Test invalid separator values."""
        with pytest.raises(ValueError):
            NamingOptions(separator="")

    def test_threshold_validation(self):
        """Test deeply_nested_threshold validation."""
        # Valid threshold
        config = NamingOptions(deeply_nested_threshold=5)
        assert config.deeply_nested_threshold == 5

        # Invalid threshold
        with pytest.raises(ValueError):
            NamingOptions(deeply_nested_threshold=1)


class TestProcessingConfigValidation:
    """Test ProcessingConfig validation."""

    def test_valid_batch_size(self):
        """Test valid batch size values."""
        valid_sizes = [1, 100, 1000, 10000]

        for size in valid_sizes:
            config = ProcessingConfigClass(batch_size=size)
            assert config.batch_size == size

    def test_boolean_parameters(self):
        """Test boolean parameter validation."""
        config = ProcessingConfigClass(
            cast_to_string=True, include_empty=False, skip_null=True
        )

        assert config.cast_to_string is True
        assert config.include_empty is False
        assert config.skip_null is True


class TestConfigurationComposition:
    """Test configuration composition and chaining."""

    def test_method_chaining(self):
        """Test that configuration methods can be chained."""
        config = TransmogConfig(separator=".", batch_size=500, cache_maxsize=1000)

        assert config.naming.separator == "."
        assert config.processing.batch_size == 500
        assert config.cache_config.maxsize == 1000

    def test_configuration_immutability(self):
        """Test that configurations are immutable."""
        original = TransmogConfig()
        updated = original.with_naming(separator=".")

        # Original should be unchanged
        assert original.naming.separator == "_"
        # Updated should have modified value
        assert updated.naming.separator == "."

    def test_factory_method_chaining(self):
        """Test chaining with factory methods."""
        config = (
            TransmogConfig.memory_optimized()
            .with_naming(separator="|")
            .use_string_format()
        )

        assert config.naming.separator == "|"
        assert config.processing.cast_to_string is True


class TestConfigurationDefaults:
    """Test configuration default values."""

    def test_default_values(self):
        """Test that default configuration has reasonable values."""
        config = TransmogConfig()

        # Naming defaults
        assert config.naming.separator == "_"
        assert config.naming.deeply_nested_threshold == 4

        # Processing defaults
        assert config.processing.cast_to_string is True
        assert config.processing.batch_size == 1000

        # Metadata defaults
        assert config.metadata.id_field == "__transmog_id"

        # Cache defaults
        assert config.cache_config.enabled is True

    def test_naming_options_defaults(self):
        """Test NamingOptions default values."""
        config = NamingOptions()

        assert config.separator == "_"
        assert config.deeply_nested_threshold == 4

    def test_processing_config_defaults(self):
        """Test ProcessingConfig default values."""
        config = ProcessingConfigClass()

        assert config.cast_to_string is True
        assert config.batch_size == 1000


class TestConfigurationEdgeCases:
    """Test edge cases in configuration validation."""

    def test_convenience_methods(self):
        """Test convenience configuration methods."""
        config = TransmogConfig()

        # Dot notation
        dot_config = config.use_dot_notation()
        assert dot_config.naming.separator == "."

        # String format
        string_config = config.use_string_format()
        assert string_config.processing.cast_to_string is True

    def test_specialized_configs(self):
        """Test specialized configuration methods."""
        # Error tolerant
        error_config = TransmogConfig.error_tolerant()
        assert error_config.error_handling.allow_malformed_data is True

        # Simple mode
        simple_config = TransmogConfig.simple_mode()
        assert simple_config.metadata.id_field == "id"

    def test_validation_edge_cases(self):
        """Test validation edge cases."""
        config = TransmogConfig()

        # Test that validation catches edge cases
        with pytest.raises((ValidationError, ValueError, ConfigurationError)):
            config.with_processing(batch_size=0)

        # Test valid edge case
        edge_config = config.with_processing(batch_size=1)
        assert edge_config.processing.batch_size == 1
