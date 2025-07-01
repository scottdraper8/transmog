"""
Tests for TransmogConfig class and configuration management.

Tests configuration creation, factory methods, and configuration composition.
"""

import pytest

from transmog.config import (
    CacheConfig,
    ErrorHandlingConfig,
    MetadataConfig,
    NamingConfig,
    ProcessingConfig,
    ProcessingMode,
    TransmogConfig,
)
from transmog.error import ConfigurationError


class TestTransmogConfigCreation:
    """Test TransmogConfig creation and basic functionality."""

    def test_default_config(self):
        """Test creating default configuration."""
        config = TransmogConfig.default()

        assert isinstance(config, TransmogConfig)
        assert isinstance(config.naming, NamingConfig)
        assert isinstance(config.processing, ProcessingConfig)
        assert isinstance(config.metadata, MetadataConfig)
        assert isinstance(config.error_handling, ErrorHandlingConfig)
        assert isinstance(config.cache_config, CacheConfig)

    def test_config_with_custom_components(self):
        """Test creating config with custom components."""
        naming = NamingConfig(separator=".", nested_threshold=5)
        processing = ProcessingConfig(batch_size=500)

        config = TransmogConfig(naming=naming, processing=processing)

        assert config.naming.separator == "."
        assert config.naming.nested_threshold == 5
        assert config.processing.batch_size == 500

    def test_config_immutability(self):
        """Test that config modifications create new instances."""
        config1 = TransmogConfig.default()
        config2 = config1.with_naming(separator=".")

        # Original should be unchanged
        assert config1.naming.separator == "_"
        # Modified config should have changes
        assert config2.naming.separator == "."
        # Should be different instances
        assert config1 is not config2


class TestConfigFactoryMethods:
    """Test configuration factory methods."""

    def test_memory_optimized_config(self):
        """Test memory optimized configuration."""
        config = TransmogConfig.memory_optimized()

        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert config.processing.batch_size == 100
        assert config.cache_config.clear_after_batch is True
        assert config.cache_config.maxsize == 1000

    def test_performance_optimized_config(self):
        """Test performance optimized configuration."""
        config = TransmogConfig.performance_optimized()

        assert config.processing.processing_mode == ProcessingMode.HIGH_PERFORMANCE
        assert config.processing.batch_size == 10000
        assert config.cache_config.clear_after_batch is False
        assert config.cache_config.maxsize == 50000

    def test_simple_mode_config(self):
        """Test simple mode configuration."""
        config = TransmogConfig.simple_mode()

        assert config.metadata.id_field == "id"
        assert config.metadata.parent_field == "parent_id"
        assert config.processing.cast_to_string is False

    def test_streaming_optimized_config(self):
        """Test streaming optimized configuration."""
        config = TransmogConfig.streaming_optimized()

        assert config.processing.batch_size == 500
        assert config.processing.cast_to_string is True
        assert config.cache_config.clear_after_batch is True

    def test_error_tolerant_config(self):
        """Test error tolerant configuration."""
        config = TransmogConfig.error_tolerant()

        assert config.error_handling.recovery_strategy == "skip"
        assert config.error_handling.allow_malformed_data is True
        assert config.processing.cast_to_string is True

    def test_csv_optimized_config(self):
        """Test CSV optimized configuration."""
        config = TransmogConfig.csv_optimized()

        assert config.processing.cast_to_string is True
        assert config.processing.include_empty is True
        assert config.processing.skip_null is False
        assert config.naming.separator == "_"


class TestConfigWithMethods:
    """Test configuration modification methods."""

    def test_with_naming(self):
        """Test with_naming method."""
        config = TransmogConfig.default()

        new_config = config.with_naming(separator=".", nested_threshold=10)

        assert new_config.naming.separator == "."
        assert new_config.naming.nested_threshold == 10
        # Original unchanged
        assert config.naming.separator == "_"

    def test_with_processing(self):
        """Test with_processing method."""
        config = TransmogConfig.default()

        new_config = config.with_processing(
            batch_size=2000, cast_to_string=False, skip_null=False
        )

        assert new_config.processing.batch_size == 2000
        assert new_config.processing.cast_to_string is False
        assert new_config.processing.skip_null is False

    def test_with_metadata(self):
        """Test with_metadata method."""
        config = TransmogConfig.default()

        new_config = config.with_metadata(
            id_field="custom_id", parent_field="custom_parent", time_field="custom_time"
        )

        assert new_config.metadata.id_field == "custom_id"
        assert new_config.metadata.parent_field == "custom_parent"
        assert new_config.metadata.time_field == "custom_time"

    def test_with_error_handling(self):
        """Test with_error_handling method."""
        config = TransmogConfig.default()

        new_config = config.with_error_handling(
            recovery_strategy="partial", allow_malformed_data=True, max_retries=5
        )

        assert new_config.error_handling.recovery_strategy == "partial"
        assert new_config.error_handling.allow_malformed_data is True
        assert new_config.error_handling.max_retries == 5

    def test_with_caching(self):
        """Test with_caching method."""
        config = TransmogConfig.default()

        new_config = config.with_caching(
            enabled=False, maxsize=5000, clear_after_batch=True
        )

        assert new_config.cache_config.enabled is False
        assert new_config.cache_config.maxsize == 5000
        assert new_config.cache_config.clear_after_batch is True


class TestConfigSpecializedMethods:
    """Test specialized configuration methods."""

    def test_with_deterministic_ids_string(self):
        """Test with_deterministic_ids with string field."""
        config = TransmogConfig.with_deterministic_ids("user_id")

        assert config.metadata.default_id_field == "user_id"

    def test_with_deterministic_ids_dict(self):
        """Test with_deterministic_ids with field mapping."""
        id_mapping = {"users": "user_id", "orders": "order_id"}
        config = TransmogConfig.with_deterministic_ids(id_mapping)

        assert config.metadata.default_id_field == id_mapping

    def test_with_custom_id_generation(self):
        """Test with_custom_id_generation method."""

        def custom_strategy(data):
            return f"custom_{data.get('name', 'unknown')}"

        config = TransmogConfig.with_custom_id_generation(custom_strategy)

        assert config.metadata.id_generation_strategy == custom_strategy

    def test_with_natural_ids(self):
        """Test with_natural_ids method."""
        patterns = ["id", "uuid", "key"]
        mapping = {"users": "user_id"}

        config = TransmogConfig.with_natural_ids(
            id_field_patterns=patterns, id_field_mapping=mapping
        )

        assert config.metadata.id_field_patterns == patterns
        assert config.metadata.id_field_mapping == mapping

    def test_use_dot_notation(self):
        """Test use_dot_notation convenience method."""
        config = TransmogConfig.default().use_dot_notation()

        assert config.naming.separator == "."

    def test_disable_arrays(self):
        """Test disable_arrays convenience method."""
        config = TransmogConfig.default().disable_arrays()

        assert config.processing.visit_arrays is False

    def test_keep_arrays(self):
        """Test keep_arrays convenience method."""
        config = TransmogConfig.default().keep_arrays()

        assert config.processing.keep_arrays is True

    def test_use_string_format(self):
        """Test use_string_format convenience method."""
        config = TransmogConfig.default().use_string_format()

        assert config.processing.cast_to_string is True


class TestConfigValidation:
    """Test configuration validation."""

    def test_invalid_separator_validation(self):
        """Test validation of invalid separator."""
        config = TransmogConfig.default()

        with pytest.raises(Exception):  # May be ConfigurationError or ValueError
            config.with_naming(separator="")

    def test_invalid_batch_size_validation(self):
        """Test validation of invalid batch size."""
        config = TransmogConfig.default()

        with pytest.raises(Exception):
            config.with_processing(batch_size=0)

        with pytest.raises(Exception):
            config.with_processing(batch_size=-1)

    def test_invalid_max_depth_validation(self):
        """Test validation of invalid max depth."""
        config = TransmogConfig.default()

        with pytest.raises(Exception):
            config.with_processing(max_depth=0)

    def test_duplicate_metadata_fields(self):
        """Test validation of duplicate metadata field names."""
        config = TransmogConfig.default()

        with pytest.raises(ConfigurationError):
            config.with_metadata(id_field="same_field", parent_field="same_field")

    def test_invalid_recovery_strategy(self):
        """Test validation of invalid recovery strategy."""
        config = TransmogConfig.default()

        with pytest.raises(Exception):
            config.with_error_handling(recovery_strategy="invalid_strategy")

    def test_invalid_cache_size(self):
        """Test validation of invalid cache size."""
        config = TransmogConfig.default()

        with pytest.raises(Exception):
            config.with_caching(maxsize=-1)


class TestConfigChaining:
    """Test configuration method chaining."""

    def test_method_chaining(self):
        """Test chaining multiple configuration methods."""
        config = TransmogConfig(
            separator=".", batch_size=500, cache_maxsize=10000, cast_to_string=True
        )

        assert config.naming.separator == "."
        assert config.processing.batch_size == 500
        assert config.cache_config.maxsize == 10000
        assert config.processing.cast_to_string is True

    def test_complex_chaining(self):
        """Test complex configuration chaining."""
        config = (
            TransmogConfig.memory_optimized()
            .use_dot_notation()
            .with_metadata(id_field="custom_id")
            .with_error_handling(recovery_strategy="skip")
            .disable_arrays()
        )

        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert config.naming.separator == "."
        assert config.metadata.id_field == "custom_id"
        assert config.error_handling.recovery_strategy == "skip"
        assert config.processing.visit_arrays is False


class TestConfigComponents:
    """Test individual configuration components."""

    def test_naming_config(self):
        """Test NamingConfig."""
        naming = NamingConfig(separator=":", nested_threshold=3)

        assert naming.separator == ":"
        assert naming.nested_threshold == 3

    def test_processing_config(self):
        """Test ProcessingConfig."""
        processing = ProcessingConfig(
            cast_to_string=False,
            batch_size=100,
            processing_mode=ProcessingMode.HIGH_PERFORMANCE,
        )

        assert processing.cast_to_string is False
        assert processing.batch_size == 100
        assert processing.processing_mode == ProcessingMode.HIGH_PERFORMANCE

    def test_metadata_config(self):
        """Test MetadataConfig."""
        metadata = MetadataConfig(
            id_field="custom_id",
            parent_field="custom_parent",
            default_id_field="natural_id",
        )

        assert metadata.id_field == "custom_id"
        assert metadata.parent_field == "custom_parent"
        assert metadata.default_id_field == "natural_id"

    def test_error_handling_config(self):
        """Test ErrorHandlingConfig."""
        error_config = ErrorHandlingConfig(
            recovery_strategy="partial", allow_malformed_data=True, max_retries=10
        )

        assert error_config.recovery_strategy == "partial"
        assert error_config.allow_malformed_data is True
        assert error_config.max_retries == 10

    def test_cache_config(self):
        """Test CacheConfig."""
        cache_config = CacheConfig(enabled=False, maxsize=5000, clear_after_batch=True)

        assert cache_config.enabled is False
        assert cache_config.maxsize == 5000
        assert cache_config.clear_after_batch is True


class TestProcessingMode:
    """Test ProcessingMode enum."""

    def test_processing_mode_values(self):
        """Test ProcessingMode enum values."""
        assert ProcessingMode.STANDARD
        assert ProcessingMode.LOW_MEMORY
        assert ProcessingMode.HIGH_PERFORMANCE

        # Should be different values
        assert ProcessingMode.STANDARD != ProcessingMode.LOW_MEMORY
        assert ProcessingMode.LOW_MEMORY != ProcessingMode.HIGH_PERFORMANCE
