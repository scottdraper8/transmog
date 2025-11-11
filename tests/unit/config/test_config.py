"""Tests for TransmogConfig configuration management."""

import pytest

from transmog.config import TransmogConfig
from transmog.error import ConfigurationError
from transmog.types import ArrayMode, NullHandling, RecoveryMode


class TestConfigCreation:
    """Test TransmogConfig creation and initialization."""

    def test_default_config(self):
        """Test default configuration values."""
        config = TransmogConfig()

        assert isinstance(config, TransmogConfig)
        assert config.separator == "_"
        assert config.null_handling == NullHandling.SKIP
        assert config.cast_to_string is False
        assert config.batch_size == 1000
        assert config.id_field == "_id"
        assert config.parent_field == "_parent_id"

    def test_config_with_custom_params(self):
        """Test configuration with custom parameters."""
        config = TransmogConfig(
            separator=".",
            batch_size=500,
            cast_to_string=False,
        )

        assert config.separator == "."
        assert config.batch_size == 500
        assert config.cast_to_string is False


class TestConfigValidation:
    """Test configuration validation rules."""

    def test_empty_separator_rejected(self):
        """Test that empty separator is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(separator="")

    def test_zero_batch_size_rejected(self):
        """Test that zero batch size is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(batch_size=0)

    def test_negative_batch_size_rejected(self):
        """Test that negative batch size is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(batch_size=-1)

    def test_zero_max_depth_rejected(self):
        """Test that zero max depth is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(max_depth=0)

    def test_duplicate_field_names_rejected(self):
        """Test that duplicate field names are rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(id_field="test", parent_field="test")

        with pytest.raises(ConfigurationError):
            TransmogConfig(id_field="test", time_field="test")


class TestConfigFactories:
    """Test configuration factory methods."""

    def test_for_memory_factory(self):
        """Test memory-optimized configuration factory."""
        config = TransmogConfig.for_memory()

        assert isinstance(config, TransmogConfig)
        assert config.batch_size == 100

    def test_for_csv_factory(self):
        """Test CSV-optimized configuration factory."""
        config = TransmogConfig.for_csv()

        assert isinstance(config, TransmogConfig)
        assert config.null_handling == NullHandling.INCLUDE
        assert config.cast_to_string is True

    def test_error_tolerant_factory(self):
        """Test error-tolerant configuration factory."""
        config = TransmogConfig.error_tolerant()

        assert isinstance(config, TransmogConfig)
        assert config.recovery_mode == RecoveryMode.SKIP

    def test_factory_returns_new_instances(self):
        """Test that factory methods return new instances."""
        config1 = TransmogConfig.for_memory()
        config2 = TransmogConfig.for_memory()

        assert config1 is not config2
        assert config1.batch_size == config2.batch_size

    def test_factory_configs_are_independent(self):
        """Test that factory configurations don't share state."""
        config1 = TransmogConfig.for_memory()
        config2 = TransmogConfig.for_csv()

        assert config1.batch_size != config2.batch_size
        assert config1.null_handling != config2.null_handling


class TestConfigArrayModes:
    """Test array mode configuration options."""

    def test_smart_array_mode(self):
        """Test SMART array mode configuration."""
        config = TransmogConfig(array_mode=ArrayMode.SMART)
        assert config.array_mode == ArrayMode.SMART

    def test_separate_array_mode(self):
        """Test SEPARATE array mode configuration."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        assert config.array_mode == ArrayMode.SEPARATE

    def test_inline_array_mode(self):
        """Test INLINE array mode configuration."""
        config = TransmogConfig(array_mode=ArrayMode.INLINE)
        assert config.array_mode == ArrayMode.INLINE

    def test_skip_array_mode(self):
        """Test SKIP array mode configuration."""
        config = TransmogConfig(array_mode=ArrayMode.SKIP)
        assert config.array_mode == ArrayMode.SKIP


class TestConfigConsistency:
    """Test configuration consistency and predictability."""

    def test_all_configs_have_sensible_defaults(self):
        """Test that all configurations have valid default values."""
        configs = [
            TransmogConfig(),
            TransmogConfig.for_memory(),
            TransmogConfig.error_tolerant(),
            TransmogConfig.for_csv(),
        ]

        for config in configs:
            assert config.separator
            assert config.batch_size > 0
            assert config.max_depth > 0
            assert config.id_field
            assert config.parent_field
