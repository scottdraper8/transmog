"""Tests for TransmogConfig class and configuration management."""

import pytest

from transmog.config import TransmogConfig
from transmog.error import ConfigurationError
from transmog.types.base import ArrayMode, RecoveryMode


class TestTransmogConfigCreation:
    """Test TransmogConfig creation and basic functionality."""

    def test_default_config(self):
        """Test creating default configuration."""
        config = TransmogConfig()

        assert isinstance(config, TransmogConfig)
        assert config.separator == "_"
        assert config.nested_threshold == 4
        assert config.cast_to_string is False
        assert config.batch_size == 1000
        assert config.id_field == "_id"
        assert config.parent_field == "_parent_id"

    def test_config_with_custom_params(self):
        """Test creating config with custom parameters."""
        config = TransmogConfig(
            separator=".",
            nested_threshold=5,
            batch_size=500,
            cast_to_string=False,
        )

        assert config.separator == "."
        assert config.nested_threshold == 5
        assert config.batch_size == 500
        assert config.cast_to_string is False

    def test_config_validation(self):
        """Test config validation."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(separator="")

        with pytest.raises(ConfigurationError):
            TransmogConfig(nested_threshold=1)

        with pytest.raises(ConfigurationError):
            TransmogConfig(batch_size=0)


class TestConfigFactoryMethods:
    """Test configuration factory methods."""

    def test_for_memory_config(self):
        """Test memory optimized configuration."""
        config = TransmogConfig.for_memory()

        assert config.batch_size == 100
        assert config.cache_size == 1000

    def test_for_performance_config(self):
        """Test performance optimized configuration."""
        config = TransmogConfig.for_performance()

        assert config.batch_size == 10000
        assert config.cache_size == 50000

    def test_simple_config(self):
        """Test simple configuration."""
        config = TransmogConfig.simple()

        assert config.id_field == "id"
        assert config.parent_field == "parent_id"
        assert config.time_field == "timestamp"

    def test_for_csv_config(self):
        """Test CSV-optimized configuration."""
        config = TransmogConfig.for_csv()

        assert config.include_empty is True
        assert config.skip_null is False
        assert config.cast_to_string is True

    def test_for_parquet_config(self):
        """Test Parquet-optimized configuration."""
        config = TransmogConfig.for_parquet()

        assert config.batch_size == 10000
        assert config.cache_size == 50000

    def test_error_tolerant_config(self):
        """Test error-tolerant configuration."""
        config = TransmogConfig.error_tolerant()

        assert config.recovery_mode == RecoveryMode.SKIP
        assert config.allow_malformed_data is True


class TestConfigFileLoading:
    """Test configuration loading from files."""

    def test_from_file_json(self, tmp_path):
        """Test loading config from JSON file."""
        import json

        config_file = tmp_path / "config.json"
        config_data = {
            "separator": ".",
            "batch_size": 5000,
            "cast_to_string": False,
        }
        config_file.write_text(json.dumps(config_data))

        config = TransmogConfig.from_file(config_file)

        assert config.separator == "."
        assert config.batch_size == 5000
        assert config.cast_to_string is False

    def test_from_file_not_found(self):
        """Test loading from non-existent file."""
        with pytest.raises(ConfigurationError):
            TransmogConfig.from_file("nonexistent.json")


class TestConfigEnvironmentVariables:
    """Test configuration from environment variables."""

    def test_from_env(self, monkeypatch):
        """Test loading config from environment variables."""
        monkeypatch.setenv("TRANSMOG_SEPARATOR", ".")
        monkeypatch.setenv("TRANSMOG_BATCH_SIZE", "5000")

        config = TransmogConfig.from_env()

        assert config.separator == "."
        assert config.batch_size == 5000


class TestConfigValidation:
    """Test configuration validation."""

    def test_duplicate_field_names(self):
        """Test that duplicate field names are rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(id_field="test", parent_field="test")

        with pytest.raises(ConfigurationError):
            TransmogConfig(id_field="test", time_field="test")

    def test_invalid_separator(self):
        """Test that empty separator is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(separator="")

    def test_invalid_nested_threshold(self):
        """Test that invalid nested threshold is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(nested_threshold=1)

    def test_invalid_batch_size(self):
        """Test that invalid batch size is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(batch_size=0)

        with pytest.raises(ConfigurationError):
            TransmogConfig(batch_size=-1)

    def test_invalid_max_depth(self):
        """Test that invalid max depth is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(max_depth=0)

    def test_invalid_cache_size(self):
        """Test that invalid cache size is rejected."""
        with pytest.raises(ConfigurationError):
            TransmogConfig(cache_size=-1)


class TestConfigArrayModes:
    """Test array mode configuration."""

    def test_smart_array_mode(self):
        """Test SMART array mode."""
        config = TransmogConfig(array_mode=ArrayMode.SMART)
        assert config.array_mode == ArrayMode.SMART

    def test_separate_array_mode(self):
        """Test SEPARATE array mode."""
        config = TransmogConfig(array_mode=ArrayMode.SEPARATE)
        assert config.array_mode == ArrayMode.SEPARATE

    def test_inline_array_mode(self):
        """Test INLINE array mode."""
        config = TransmogConfig(array_mode=ArrayMode.INLINE)
        assert config.array_mode == ArrayMode.INLINE

    def test_skip_array_mode(self):
        """Test SKIP array mode."""
        config = TransmogConfig(array_mode=ArrayMode.SKIP)
        assert config.array_mode == ArrayMode.SKIP
