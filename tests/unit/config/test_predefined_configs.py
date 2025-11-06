"""
Tests for predefined configurations and profiles.

Tests the various pre-built configuration profiles and factory methods
available in Transmog.
"""

import pytest

from transmog.config import ProcessingMode, TransmogConfig
from transmog.error import ConfigurationError


class TestPredefinedConfigurations:
    """Test predefined configuration factory methods."""

    def test_default_configuration(self):
        """Test the default configuration."""
        config = TransmogConfig.default()

        assert isinstance(config, TransmogConfig)
        assert config.naming.separator == "_"
        assert config.processing.cast_to_string is True
        assert config.processing.batch_size == 1000
        assert config.metadata.id_field == "__transmog_id"
        assert config.metadata.parent_field == "__parent_transmog_id"
        assert config.cache_config.enabled is True

    def test_memory_optimized_configuration(self):
        """Test memory-optimized configuration."""
        config = TransmogConfig.memory_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert (
            config.processing.batch_size <= 500
        )  # Smaller batches for memory efficiency
        assert config.cache_config.maxsize <= 1000  # Smaller cache

    def test_performance_optimized_configuration(self):
        """Test performance-optimized configuration."""
        config = TransmogConfig.performance_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.processing_mode == ProcessingMode.HIGH_PERFORMANCE
        assert config.processing.batch_size >= 1000  # Larger batches for performance
        assert config.cache_config.maxsize >= 10000  # Larger cache

    def test_simple_mode_configuration(self):
        """Test simple mode configuration."""
        config = TransmogConfig.simple_mode()

        assert isinstance(config, TransmogConfig)
        # Simple mode should have user-friendly field names
        assert config.metadata.id_field == "id"
        assert config.metadata.parent_field == "parent_id"
        assert config.metadata.time_field == "transmog_time"
        assert config.processing.cast_to_string is False

    def test_streaming_optimized_configuration(self):
        """Test streaming-optimized configuration."""
        config = TransmogConfig.streaming_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert config.processing.batch_size <= 500  # Small batches for streaming
        assert config.cache_config.clear_after_batch is True

    def test_error_tolerant_configuration(self):
        """Test error-tolerant configuration."""
        config = TransmogConfig.error_tolerant()

        assert isinstance(config, TransmogConfig)
        assert config.error_handling.recovery_strategy == "skip"
        assert config.error_handling.allow_malformed_data is True
        assert config.error_handling.max_retries == 5

    def test_csv_optimized_configuration(self):
        """Test CSV-optimized configuration."""
        config = TransmogConfig.csv_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.cast_to_string is True  # CSV needs strings
        assert config.processing.include_empty is True  # Include empty CSV cells
        assert config.processing.skip_null is False  # Don't skip nulls in CSV
        assert config.naming.separator == "_"


class TestConfigurationProfiles:
    """Test configuration profiles with specific use cases."""

    def test_natural_id_configuration(self):
        """Test configuration with natural ID discovery."""
        config = TransmogConfig.with_natural_ids()

        assert isinstance(config, TransmogConfig)
        assert config.metadata.force_transmog_id is False  # Allow natural IDs

    def test_deterministic_id_configuration(self):
        """Test configuration with deterministic ID generation."""
        config = TransmogConfig.with_deterministic_ids("user_id")

        assert isinstance(config, TransmogConfig)
        assert config.metadata.default_id_field == "user_id"

    def test_custom_id_generation_configuration(self):
        """Test configuration with custom ID generation."""

        def custom_id_gen(record):
            return f"custom_{record.get('name', 'unknown')}"

        config = TransmogConfig.with_custom_id_generation(custom_id_gen)

        assert isinstance(config, TransmogConfig)
        assert config.metadata.id_generation_strategy == custom_id_gen


class TestConfigurationChaining:
    """Test chaining configuration methods."""

    def test_method_chaining(self):
        """Test that configuration methods can be chained."""
        config = TransmogConfig(separator=".", batch_size=500, id_field="custom_id")

        assert config.naming.separator == "."
        assert config.processing.batch_size == 500
        assert config.metadata.id_field == "custom_id"

    def test_chaining_with_profiles(self):
        """Test chaining profile methods."""
        from transmog.types.base import ArrayMode

        config = TransmogConfig.memory_optimized().use_dot_notation().disable_arrays()

        assert config.naming.separator == "."
        assert config.processing.array_mode == ArrayMode.SKIP
        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY

    def test_profile_override_chaining(self):
        """Test that later methods override earlier ones."""
        config = (
            TransmogConfig.performance_optimized().with_processing(  # Sets large batch size
                batch_size=100
            )  # Override with smaller batch
        )

        assert config.processing.batch_size == 100  # Should use the override value

    def test_convenience_method_chaining(self):
        """Test chaining convenience methods."""
        from transmog.types.base import ArrayMode

        config = (
            TransmogConfig.default()
            .use_dot_notation()
            .use_string_format()
            .keep_arrays_inline()
        )

        assert config.naming.separator == "."
        assert config.processing.cast_to_string is True
        assert config.processing.array_mode == ArrayMode.INLINE


class TestConfigurationValidation:
    """Test validation of predefined configurations."""

    def test_all_profiles_are_valid(self):
        """Test that all predefined profiles produce valid configurations."""
        profile_methods = [
            "default",
            "memory_optimized",
            "performance_optimized",
            "simple_mode",
            "streaming_optimized",
            "error_tolerant",
            "csv_optimized",
        ]

        for method_name in profile_methods:
            if hasattr(TransmogConfig, method_name):
                config = getattr(TransmogConfig, method_name)()
                assert isinstance(config, TransmogConfig)

                # Basic validation checks
                assert config.naming.separator is not None
                assert len(config.naming.separator) > 0
                assert config.processing.batch_size > 0
                assert config.metadata.id_field is not None
                assert config.metadata.parent_field is not None

    def test_profile_consistency(self):
        """Test that profiles are internally consistent."""
        # Memory optimized should have smaller resource usage
        memory_config = TransmogConfig.memory_optimized()
        performance_config = TransmogConfig.performance_optimized()

        # Memory config should use less memory
        assert (
            memory_config.processing.batch_size
            <= performance_config.processing.batch_size
        )
        assert (
            memory_config.cache_config.maxsize
            <= performance_config.cache_config.maxsize
        )

    def test_streaming_profile_consistency(self):
        """Test that streaming profile is consistent with low memory usage."""
        streaming_config = TransmogConfig.streaming_optimized()

        # Streaming should be memory efficient
        assert streaming_config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert streaming_config.processing.batch_size <= 500
        assert streaming_config.cache_config.clear_after_batch is True


class TestConfigurationDocumentation:
    """Test that configuration methods are properly documented."""

    def test_profile_methods_exist(self):
        """Test that all documented profile methods exist."""
        expected_methods = [
            "default",
            "memory_optimized",
            "performance_optimized",
            "simple_mode",
            "streaming_optimized",
            "error_tolerant",
            "csv_optimized",
            "with_deterministic_ids",
            "with_custom_id_generation",
            "with_natural_ids",
        ]

        for method_name in expected_methods:
            assert hasattr(TransmogConfig, method_name), (
                f"Method {method_name} not found"
            )
            method = getattr(TransmogConfig, method_name)
            assert callable(method), f"Method {method_name} is not callable"

    def test_convenience_methods_exist(self):
        """Test that all convenience methods exist."""
        expected_methods = [
            "use_dot_notation",
            "disable_arrays",
            "keep_arrays_inline",
            "use_string_format",
        ]

        config = TransmogConfig.default()
        for method_name in expected_methods:
            assert hasattr(config, method_name), f"Method {method_name} not found"
            method = getattr(config, method_name)
            assert callable(method), f"Method {method_name} is not callable"


class TestConfigurationEdgeCases:
    """Test edge cases in configuration creation."""

    def test_empty_configuration_handling(self):
        """Test handling of configurations with minimal settings."""
        # Create config with minimal settings
        config = TransmogConfig()

        # Should still have valid defaults
        assert config.naming.separator is not None
        assert config.processing.batch_size > 0
        assert config.metadata.id_field is not None

    def test_conflicting_settings_detection(self):
        """Test detection of conflicting configuration settings."""
        # Test that conflicting metadata field names are detected
        with pytest.raises(ConfigurationError):
            TransmogConfig().with_metadata(
                id_field="same_field",
                parent_field="same_field",  # Same as id_field
            )

    def test_configuration_method_combinations(self):
        """Test various combinations of configuration methods."""
        # Test that all these combinations work without errors
        configs = [
            TransmogConfig.memory_optimized().use_dot_notation(),
            TransmogConfig.performance_optimized().disable_arrays(),
            TransmogConfig.csv_optimized().with_naming(separator="."),
            TransmogConfig.error_tolerant().with_processing(batch_size=50),
        ]

        for config in configs:
            assert isinstance(config, TransmogConfig)
            # Basic validation
            assert config.naming.separator is not None
            assert config.processing.batch_size > 0
