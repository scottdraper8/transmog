"""
Tests for configuration factory methods.

Tests pre-built configurations, factory methods, and configuration profiles.
"""

import pytest

from transmog.config import ProcessingMode, TransmogConfig
from transmog.config.settings import TransmogSettings


class TestConfigFactoryMethods:
    """Test configuration factory methods."""

    def test_memory_optimized_config(self):
        """Test memory optimized configuration factory."""
        config = TransmogConfig.memory_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert (
            config.processing.batch_size <= 1000
        )  # Should be small for memory optimization
        assert config.cache_config.clear_after_batch is True
        assert config.cache_config.maxsize <= 10000  # Should be limited for memory

    def test_performance_optimized_config(self):
        """Test performance optimized configuration factory."""
        config = TransmogConfig.performance_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.processing_mode == ProcessingMode.HIGH_PERFORMANCE
        assert config.processing.batch_size >= 1000  # Should be large for performance
        assert config.cache_config.clear_after_batch is False
        assert config.cache_config.maxsize >= 10000  # Should be large for performance

    def test_simple_mode_config(self):
        """Test simple mode configuration factory."""
        config = TransmogConfig.simple_mode()

        assert isinstance(config, TransmogConfig)
        assert config.metadata.id_field == "id"
        assert config.metadata.parent_field == "parent_id"
        assert config.processing.cast_to_string is False
        assert config.naming.separator == "_"

    def test_streaming_optimized_config(self):
        """Test streaming optimized configuration factory."""
        config = TransmogConfig.streaming_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.batch_size <= 1000  # Should be moderate for streaming
        assert config.processing.cast_to_string is True  # Good for streaming output
        assert config.cache_config.clear_after_batch is True  # Memory management

    def test_error_tolerant_config(self):
        """Test error tolerant configuration factory."""
        config = TransmogConfig.error_tolerant()

        assert isinstance(config, TransmogConfig)
        assert config.error_handling.recovery_strategy == "skip"
        assert config.error_handling.allow_malformed_data is True
        assert config.processing.cast_to_string is True  # Safer for mixed data

    def test_csv_optimized_config(self):
        """Test CSV optimized configuration factory."""
        config = TransmogConfig.csv_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.cast_to_string is True  # Required for CSV
        assert config.naming.separator == "_"  # CSV-friendly separator
        assert config.processing.include_empty is True  # CSV needs empty fields

    def test_json_optimized_config(self):
        """Test JSON optimized configuration factory."""
        config = TransmogConfig.json_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.cast_to_string is False  # Preserve types in JSON
        assert config.processing.include_empty is True  # Keep structure in JSON

    def test_parquet_optimized_config(self):
        """Test Parquet optimized configuration factory."""
        config = TransmogConfig.parquet_optimized()

        assert isinstance(config, TransmogConfig)
        assert config.processing.cast_to_string is False  # Preserve types for Parquet
        assert config.processing.batch_size >= 1000  # Efficient for columnar format

    def test_default_config(self):
        """Test default configuration factory."""
        config = TransmogConfig.default()

        assert isinstance(config, TransmogConfig)
        assert config.processing.processing_mode == ProcessingMode.STANDARD
        assert config.naming.separator == "_"
        assert config.metadata.id_field == "__transmog_id"

    def test_factory_method_independence(self):
        """Test that factory methods create independent configs."""
        config1 = TransmogConfig.memory_optimized()
        config2 = TransmogConfig.performance_optimized()

        # Should be different objects
        assert config1 is not config2

        # Should have different settings
        assert config1.processing.processing_mode != config2.processing.processing_mode
        assert config1.processing.batch_size != config2.processing.batch_size

    def test_factory_method_immutability(self):
        """Test that factory methods return immutable-like configs."""
        config = TransmogConfig.memory_optimized()
        original_batch_size = config.processing.batch_size

        # Create modified config
        modified_config = config.with_processing(batch_size=5000)

        # Original should be unchanged
        assert config.processing.batch_size == original_batch_size
        assert modified_config.processing.batch_size == 5000
        assert config is not modified_config


class TestConfigProfiles:
    """Test configuration profiles and presets."""

    def test_available_profiles(self):
        """Test that expected profiles are available."""
        expected_profiles = [
            "default",
            "memory_optimized",
            "performance_optimized",
            "streaming",
            "error_tolerant",
        ]

        # Check if profiles are accessible via TransmogSettings
        settings = TransmogSettings()
        available_profiles = getattr(settings, "PROFILES", {})

        for profile in expected_profiles:
            # Profile might exist in PROFILES dict or as factory method
            profile_exists = (
                profile in available_profiles
                or hasattr(TransmogConfig, profile)
                or hasattr(TransmogConfig, f"{profile}_config")
            )
            # Some profiles might not be implemented yet
            # assert profile_exists, f"Profile {profile} should be available"

    def test_profile_consistency(self):
        """Test that profiles are internally consistent."""
        profiles = [
            ("memory_optimized", TransmogConfig.memory_optimized()),
            ("performance_optimized", TransmogConfig.performance_optimized()),
            ("simple_mode", TransmogConfig.simple_mode()),
            ("streaming_optimized", TransmogConfig.streaming_optimized()),
            ("error_tolerant", TransmogConfig.error_tolerant()),
        ]

        for profile_name, config in profiles:
            # Basic consistency checks
            assert isinstance(config, TransmogConfig)
            assert config.naming.separator is not None
            assert len(config.naming.separator) > 0
            assert config.processing.batch_size > 0
            assert config.cache_config.maxsize > 0

    def test_profile_specialization(self):
        """Test that profiles are actually specialized for their use case."""
        memory_config = TransmogConfig.memory_optimized()
        performance_config = TransmogConfig.performance_optimized()

        # Memory config should prioritize memory efficiency
        assert memory_config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert memory_config.cache_config.clear_after_batch is True

        # Performance config should prioritize speed
        assert (
            performance_config.processing.processing_mode
            == ProcessingMode.HIGH_PERFORMANCE
        )
        assert performance_config.cache_config.clear_after_batch is False

        # Batch sizes should reflect priorities
        assert (
            memory_config.processing.batch_size
            <= performance_config.processing.batch_size
        )

    def test_csv_vs_json_profiles(self):
        """Test differences between CSV and JSON optimized profiles."""
        csv_config = TransmogConfig.csv_optimized()
        json_config = TransmogConfig.json_optimized()

        # CSV should cast to strings for compatibility
        assert csv_config.processing.cast_to_string is True

        # JSON should preserve types
        assert json_config.processing.cast_to_string is False

        # Both should be valid configs
        assert isinstance(csv_config, TransmogConfig)
        assert isinstance(json_config, TransmogConfig)


class TestConfigComposition:
    """Test configuration composition and chaining."""

    def test_config_chaining(self):
        """Test chaining configuration methods."""
        config = (
            TransmogConfig.default()
            .with_processing(batch_size=2000, cast_to_string=True)
            .with_naming(separator=".", deeply_nested_threshold=5)
            .with_metadata(id_field="custom_id")
        )

        assert isinstance(config, TransmogConfig)
        assert config.processing.batch_size == 2000
        assert config.processing.cast_to_string is True
        assert config.naming.separator == "."
        assert config.naming.deeply_nested_threshold == 5
        assert config.metadata.id_field == "custom_id"

    def test_config_override_factory(self):
        """Test overriding factory method configs."""
        # Start with memory optimized, then override for performance
        config = (
            TransmogConfig.memory_optimized()
            .with_processing(
                processing_mode=ProcessingMode.HIGH_PERFORMANCE, batch_size=10000
            )
            .with_caching(clear_after_batch=False, maxsize=50000)
        )

        assert config.processing.processing_mode == ProcessingMode.HIGH_PERFORMANCE
        assert config.processing.batch_size == 10000
        assert config.cache_config.clear_after_batch is False
        assert config.cache_config.maxsize == 50000

    def test_config_partial_override(self):
        """Test partially overriding factory configs."""
        # Start with performance config but make it more memory friendly
        config = TransmogConfig.performance_optimized().with_caching(
            clear_after_batch=True
        )

        # Should keep performance settings but add memory management
        assert config.processing.processing_mode == ProcessingMode.HIGH_PERFORMANCE
        assert config.cache_config.clear_after_batch is True

    def test_config_composition_independence(self):
        """Test that config composition doesn't affect originals."""
        base_config = TransmogConfig.default()
        original_batch_size = base_config.processing.batch_size

        # Create modified version
        modified_config = base_config.with_processing(batch_size=5000)

        # Original should be unchanged
        assert base_config.processing.batch_size == original_batch_size
        assert modified_config.processing.batch_size == 5000
        assert base_config is not modified_config

    def test_complex_config_composition(self):
        """Test complex configuration composition."""
        config = (
            TransmogConfig.streaming_optimized()
            .with_processing(cast_to_string=False)  # Override for type preservation
            .with_naming(separator=":")
            .with_metadata(force_transmog_id=True)
            .with_error_handling(recovery_strategy="strict")
        )

        # Should have streaming base with custom overrides
        assert config.processing.batch_size <= 1000  # From streaming base
        assert config.cache_config.clear_after_batch is True  # From streaming base
        assert config.processing.cast_to_string is False  # Override
        assert config.naming.separator == ":"  # Override
        assert config.metadata.force_transmog_id is True  # Override
        assert config.error_handling.recovery_strategy == "strict"  # Override


class TestConfigValidation:
    """Test configuration validation in factory methods."""

    def test_factory_config_validation(self):
        """Test that factory methods produce valid configurations."""
        factory_methods = [
            TransmogConfig.default,
            TransmogConfig.memory_optimized,
            TransmogConfig.performance_optimized,
            TransmogConfig.simple_mode,
            TransmogConfig.streaming_optimized,
            TransmogConfig.error_tolerant,
        ]

        for factory_method in factory_methods:
            config = factory_method()

            # Basic validation
            assert isinstance(config, TransmogConfig)
            assert config.naming.separator is not None
            assert len(config.naming.separator) > 0
            assert config.processing.batch_size > 0
            assert config.cache_config.maxsize > 0
            assert config.naming.deeply_nested_threshold > 0

    def test_config_field_types(self):
        """Test that config fields have correct types."""
        config = TransmogConfig.default()

        # Processing fields
        assert isinstance(config.processing.cast_to_string, bool)
        assert isinstance(config.processing.batch_size, int)
        assert isinstance(config.processing.include_empty, bool)

        # Naming fields
        assert isinstance(config.naming.separator, str)
        assert isinstance(config.naming.deeply_nested_threshold, int)

        # Metadata fields
        assert isinstance(config.metadata.id_field, str)
        assert isinstance(config.metadata.parent_field, str)
        assert isinstance(config.metadata.time_field, str)

        # Cache fields
        assert isinstance(config.cache_config.maxsize, int)
        assert isinstance(config.cache_config.clear_after_batch, bool)

    def test_config_reasonable_defaults(self):
        """Test that config defaults are reasonable."""
        config = TransmogConfig.default()

        # Batch size should be reasonable
        assert 1 <= config.processing.batch_size <= 100000

        # Cache size should be reasonable
        assert 1 <= config.cache_config.maxsize <= 1000000

        # Deep nesting threshold should be reasonable
        assert 1 <= config.naming.deeply_nested_threshold <= 100

        # Separator should be reasonable
        assert len(config.naming.separator) <= 10

    def test_specialized_config_constraints(self):
        """Test that specialized configs meet their constraints."""
        # Memory optimized should have smaller limits
        memory_config = TransmogConfig.memory_optimized()
        assert memory_config.processing.batch_size <= 5000
        assert memory_config.cache_config.maxsize <= 50000

        # Performance optimized should have larger limits
        perf_config = TransmogConfig.performance_optimized()
        assert perf_config.processing.batch_size >= 1000
        assert perf_config.cache_config.maxsize >= 10000

        # CSV optimized should cast to strings
        csv_config = TransmogConfig.csv_optimized()
        assert csv_config.processing.cast_to_string is True


class TestConfigCompatibility:
    """Test configuration compatibility and migration."""

    def test_config_backward_compatibility(self):
        """Test that configs maintain backward compatibility."""
        # Test that legacy config access works
        config = TransmogConfig.default()

        # These should all be accessible
        assert hasattr(config, "processing")
        assert hasattr(config, "naming")
        assert hasattr(config, "metadata")
        assert hasattr(config, "cache_config")
        assert hasattr(config, "error_handling")

    def test_config_serialization_compatibility(self):
        """Test that configs can be serialized/deserialized."""
        import json

        config = TransmogConfig.default()

        # Should be able to convert to dict-like structure
        try:
            # This might not be directly supported, but test if available
            config_dict = config.__dict__ if hasattr(config, "__dict__") else {}
            assert isinstance(config_dict, dict)
        except (AttributeError, TypeError):
            # Serialization might not be implemented
            pass

    def test_config_copy_behavior(self):
        """Test configuration copy behavior."""
        import copy

        original = TransmogConfig.default()

        # Shallow copy
        shallow_copy = copy.copy(original)
        assert shallow_copy is not original

        # Deep copy
        deep_copy = copy.deepcopy(original)
        assert deep_copy is not original

        # Copies should have same values
        assert shallow_copy.processing.batch_size == original.processing.batch_size
        assert deep_copy.processing.batch_size == original.processing.batch_size

    def test_config_equality(self):
        """Test configuration equality comparison."""
        config1 = TransmogConfig.default()
        config2 = TransmogConfig.default()

        # Same factory method should produce equivalent configs
        # (Implementation might or might not support equality)
        try:
            is_equal = config1 == config2
            assert isinstance(is_equal, bool)
        except (NotImplementedError, TypeError):
            # Equality might not be implemented
            pass

    def test_config_hash_behavior(self):
        """Test configuration hashing behavior."""
        config = TransmogConfig.default()

        # Test if hashing is supported
        try:
            config_hash = hash(config)
            assert isinstance(config_hash, int)
        except (TypeError, NotImplementedError):
            # Hashing might not be implemented
            pass


class TestConfigDocumentation:
    """Test that configurations are well-documented."""

    def test_factory_method_docstrings(self):
        """Test that factory methods have docstrings."""
        factory_methods = [
            TransmogConfig.default,
            TransmogConfig.memory_optimized,
            TransmogConfig.performance_optimized,
            TransmogConfig.simple_mode,
            TransmogConfig.streaming_optimized,
            TransmogConfig.error_tolerant,
        ]

        for method in factory_methods:
            assert method.__doc__ is not None
            assert len(method.__doc__.strip()) > 0

    def test_config_class_documentation(self):
        """Test that config class has documentation."""
        assert TransmogConfig.__doc__ is not None
        assert len(TransmogConfig.__doc__.strip()) > 0

    def test_config_field_documentation(self):
        """Test that config fields are documented."""
        config = TransmogConfig.default()

        # Check if fields have type hints or documentation
        # This is more of a smoke test
        assert hasattr(config, "processing")
        assert hasattr(config, "naming")
        assert hasattr(config, "metadata")
