"""Tests for predefined configurations and profiles."""

import pytest

from transmog.config import TransmogConfig
from transmog.types.base import RecoveryMode


class TestPredefinedConfigurations:
    """Test predefined configuration factory methods."""

    def test_default_configuration(self):
        """Test the default configuration."""
        config = TransmogConfig()

        assert isinstance(config, TransmogConfig)
        assert config.separator == "_"
        assert config.cast_to_string is False
        assert config.batch_size == 1000
        assert config.id_field == "_id"
        assert config.parent_field == "_parent_id"
        assert config.cache_size == 10000

    def test_for_memory_configuration(self):
        """Test memory-optimized configuration."""
        config = TransmogConfig.for_memory()

        assert isinstance(config, TransmogConfig)
        assert config.batch_size == 100
        assert config.cache_size == 1000

    def test_for_performance_configuration(self):
        """Test performance-optimized configuration."""
        config = TransmogConfig.for_performance()

        assert isinstance(config, TransmogConfig)
        assert config.batch_size == 10000
        assert config.cache_size == 50000

    def test_simple_configuration(self):
        """Test simple mode configuration."""
        config = TransmogConfig.simple()

        assert isinstance(config, TransmogConfig)
        assert config.id_field == "id"
        assert config.parent_field == "parent_id"
        assert config.time_field == "timestamp"

    def test_error_tolerant_configuration(self):
        """Test error-tolerant configuration."""
        config = TransmogConfig.error_tolerant()

        assert isinstance(config, TransmogConfig)
        assert config.recovery_mode == RecoveryMode.SKIP
        assert config.allow_malformed_data is True

    def test_for_csv_configuration(self):
        """Test CSV-optimized configuration."""
        config = TransmogConfig.for_csv()

        assert isinstance(config, TransmogConfig)
        assert config.include_empty is True
        assert config.skip_null is False
        assert config.cast_to_string is True

    def test_for_parquet_configuration(self):
        """Test Parquet-optimized configuration."""
        config = TransmogConfig.for_parquet()

        assert isinstance(config, TransmogConfig)
        assert config.batch_size == 10000
        assert config.cache_size == 50000


class TestConfigurationConsistency:
    """Test that configurations are consistent and predictable."""

    def test_factory_methods_return_new_instances(self):
        """Test that factory methods return new instances."""
        config1 = TransmogConfig.for_performance()
        config2 = TransmogConfig.for_performance()

        assert config1 is not config2

    def test_configurations_have_expected_defaults(self):
        """Test that all configurations have sensible defaults."""
        configs = [
            TransmogConfig(),
            TransmogConfig.for_memory(),
            TransmogConfig.for_performance(),
            TransmogConfig.simple(),
            TransmogConfig.error_tolerant(),
            TransmogConfig.for_csv(),
            TransmogConfig.for_parquet(),
        ]

        for config in configs:
            assert config.separator
            assert config.batch_size > 0
            assert config.max_depth > 0
            assert config.nested_threshold >= 2
