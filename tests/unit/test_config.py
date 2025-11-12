"""Tests for TransmogConfig configuration management."""

import pytest

from transmog.config import TransmogConfig
from transmog.exceptions import ConfigurationError
from transmog.types import ArrayMode


class TestConfigCreation:
    """Test TransmogConfig creation and initialization."""

    def test_default_config(self):
        """Test default configuration values."""
        config = TransmogConfig()

        assert isinstance(config, TransmogConfig)
        assert config.include_nulls is False
        assert config.batch_size == 1000
        assert config.id_field == "_id"
        assert config.parent_field == "_parent_id"
        assert config.id_generation == "random"

    def test_config_with_custom_params(self):
        """Test configuration with custom parameters."""
        config = TransmogConfig(
            batch_size=500,
            include_nulls=True,
        )

        assert config.batch_size == 500
        assert config.include_nulls is True


class TestConfigValidation:
    """Test configuration validation rules."""

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
            TransmogConfig(batch_size=100),
            TransmogConfig(include_nulls=True),
        ]

        for config in configs:
            assert config.batch_size > 0
            assert config.max_depth > 0
            assert config.id_field
            assert config.parent_field
            assert config.id_generation


class TestArrayModeHandling:
    """Test array mode validation and error handling."""

    def test_invalid_array_mode_raises_error(self):
        """Test that invalid ArrayMode raises ValueError during processing."""
        import transmog as tm

        config = TransmogConfig(array_mode=ArrayMode.SMART)

        # Monkey-patch with invalid mode to test defensive check
        class InvalidMode:
            value = "invalid"

        config.array_mode = InvalidMode()
        data = {"test": [1, 2, 3]}

        with pytest.raises(ValueError, match="Unhandled ArrayMode"):
            tm.flatten(data, config=config)
