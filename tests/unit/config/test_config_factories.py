"""Tests for configuration factory methods."""

import pytest

from transmog.config import TransmogConfig
from transmog.types.base import RecoveryMode


class TestConfigFactoryMethods:
    """Test configuration factory methods."""

    def test_for_memory_config(self):
        """Test memory optimized configuration factory."""
        config = TransmogConfig.for_memory()

        assert isinstance(config, TransmogConfig)
        assert config.batch_size == 100
        assert config.cache_size == 1000

    def test_for_parquet_config(self):
        """Test performance optimized configuration factory."""
        config = TransmogConfig.for_parquet()

        assert isinstance(config, TransmogConfig)
        assert config.batch_size == 10000
        assert config.cache_size == 50000

    def test_simple_config(self):
        """Test simple mode configuration factory."""
        config = TransmogConfig.simple()

        assert isinstance(config, TransmogConfig)
        assert config.id_field == "id"
        assert config.parent_field == "parent_id"

    def test_error_tolerant_config(self):
        """Test error tolerant configuration factory."""
        config = TransmogConfig.error_tolerant()

        assert isinstance(config, TransmogConfig)
        assert config.recovery_mode == RecoveryMode.SKIP
        assert config.allow_malformed_data is True

    def test_for_csv_config(self):
        """Test CSV-optimized configuration factory."""
        config = TransmogConfig.for_csv()

        assert isinstance(config, TransmogConfig)
        assert config.include_empty is True
        assert config.skip_null is False
        assert config.cast_to_string is True


class TestFactoryMethodsReturnNewInstances:
    """Test that factory methods return new instances."""

    def test_factory_returns_new_instance(self):
        """Test that each factory call returns a new instance."""
        config1 = TransmogConfig.for_parquet()
        config2 = TransmogConfig.for_parquet()

        assert config1 is not config2
        assert config1.batch_size == config2.batch_size

    def test_factory_configs_are_independent(self):
        """Test that factory configs don't share state."""
        config1 = TransmogConfig.for_memory()
        config2 = TransmogConfig.for_parquet()

        assert config1.batch_size != config2.batch_size
        assert config1.cache_size != config2.cache_size
