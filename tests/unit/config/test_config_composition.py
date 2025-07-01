"""
Tests for configuration composition and merging.

Tests how different configuration objects can be combined, merged,
and composed to create complex configurations.
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


class TestConfigurationComposition:
    """Test composition of configuration objects."""

    def test_config_from_components(self):
        """Test creating configuration from individual components."""
        naming = NamingConfig(separator=".", nested_threshold=3)
        processing = ProcessingConfig(batch_size=500, cast_to_string=False)
        metadata = MetadataConfig(id_field="custom_id", parent_field="custom_parent")

        config = TransmogConfig(naming=naming, processing=processing, metadata=metadata)

        assert config.naming.separator == "."
        assert config.naming.nested_threshold == 3
        assert config.processing.batch_size == 500
        assert config.processing.cast_to_string is False
        assert config.metadata.id_field == "custom_id"
        assert config.metadata.parent_field == "custom_parent"

    def test_config_component_independence(self):
        """Test that configuration components are independent."""
        naming1 = NamingConfig(separator="_")
        naming2 = NamingConfig(separator=".")

        config1 = TransmogConfig(naming=naming1)
        config2 = TransmogConfig(naming=naming2)

        # Configurations should be independent
        assert config1.naming.separator == "_"
        assert config2.naming.separator == "."

        # Modifying one configuration shouldn't affect the other
        config1_modified = config1.with_naming(separator=":")
        assert config1.naming.separator == "_"  # Original unchanged
        assert config1_modified.naming.separator == ":"  # New config modified

    def test_partial_configuration_composition(self):
        """Test composing configurations with only some components specified."""
        # Create config with only naming specified
        custom_naming = NamingConfig(separator="__", nested_threshold=2)
        config = TransmogConfig(naming=custom_naming)

        # Other components should use defaults
        assert config.naming.separator == "__"
        assert config.naming.nested_threshold == 2
        assert config.processing.batch_size == 1000  # Default
        assert config.metadata.id_field == "__transmog_id"  # Default


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
        config = TransmogConfig.memory_optimized().use_dot_notation().disable_arrays()

        assert config.naming.separator == "."
        assert config.processing.visit_arrays is False
        assert config.processing.processing_mode == ProcessingMode.LOW_MEMORY

    def test_profile_override_chaining(self):
        """Test that later methods override earlier ones."""
        config = (
            TransmogConfig.performance_optimized().with_processing(  # Sets large batch size
                batch_size=100
            )  # Override with smaller batch
        )

        assert config.processing.batch_size == 100  # Should use the override value


class TestConfigurationInheritance:
    """Test configuration inheritance patterns."""

    def test_config_inheritance_from_base(self):
        """Test inheriting from a base configuration."""
        base_config = TransmogConfig.memory_optimized()

        # Create specialized config based on memory optimized
        specialized_config = base_config.with_naming(separator=".")

        # Should inherit memory optimization settings
        assert (
            specialized_config.processing.processing_mode == ProcessingMode.LOW_MEMORY
        )
        assert specialized_config.processing.batch_size <= 500

        # Should have the modified naming setting
        assert specialized_config.naming.separator == "."

    def test_config_inheritance_chain(self):
        """Test a chain of configuration inheritance."""
        base = TransmogConfig.default()
        step1 = base.with_naming(separator=".")
        step2 = step1.with_processing(batch_size=250)
        step3 = step2.with_metadata(id_field="custom_id")

        # Final config should have all modifications
        assert step3.naming.separator == "."
        assert step3.processing.batch_size == 250
        assert step3.metadata.id_field == "custom_id"

        # Earlier configs should be unchanged
        assert base.naming.separator == "_"
        assert step1.processing.batch_size == 1000
        assert step2.metadata.id_field == "__transmog_id"

    def test_config_inheritance_immutability(self):
        """Test that configuration inheritance preserves immutability."""
        original = TransmogConfig.default()
        modified = original.with_naming(separator=".")

        # Original should be unchanged
        assert original.naming.separator == "_"
        assert modified.naming.separator == "."

        # They should be different objects
        assert original is not modified


class TestConfigurationTemplates:
    """Test configuration templates and patterns."""

    def test_create_template_configuration(self):
        """Test creating a template configuration for reuse."""
        template = TransmogConfig(
            naming=NamingConfig(separator=".", nested_threshold=2),
            processing=ProcessingConfig(cast_to_string=False, batch_size=100),
            metadata=MetadataConfig(force_transmog_id=True),
        )

        # Use template as base for specific configurations
        csv_config = template.with_processing(cast_to_string=True)
        json_config = template.with_processing(cast_to_string=False)

        # Both should inherit template settings
        assert csv_config.naming.separator == "."
        assert json_config.naming.separator == "."
        assert csv_config.processing.batch_size == 100
        assert json_config.processing.batch_size == 100

        # But should have different processing settings
        assert csv_config.processing.cast_to_string is True
        assert json_config.processing.cast_to_string is False

    def test_configuration_factory_pattern(self):
        """Test using factory pattern for configuration creation."""

        def create_processing_config(data_type: str) -> TransmogConfig:
            """Factory function to create configurations for different data types."""
            base = TransmogConfig.default()

            if data_type == "csv":
                return base.csv_optimized()
            elif data_type == "json":
                return base.with_processing(cast_to_string=False)
            elif data_type == "streaming":
                return base.streaming_optimized()
            else:
                return base

        csv_config = create_processing_config("csv")
        json_config = create_processing_config("json")
        streaming_config = create_processing_config("streaming")

        # Each should have appropriate settings
        assert csv_config.processing.cast_to_string is True
        assert json_config.processing.cast_to_string is False
        assert streaming_config.processing.processing_mode == ProcessingMode.LOW_MEMORY


class TestConfigurationValidation:
    """Test validation in configuration composition."""

    def test_composed_config_validation(self):
        """Test that composed configurations are validated."""
        # This should work fine
        config = TransmogConfig(
            processing=ProcessingConfig(batch_size=100),
            metadata=MetadataConfig(id_field="custom_id"),
        )

        assert config.processing.batch_size == 100
        assert config.metadata.id_field == "custom_id"

    def test_component_validation_in_composition(self):
        """Test that individual components are validated during composition."""
        # Test that invalid configurations raise errors
        with pytest.raises(ConfigurationError):
            TransmogConfig().with_metadata(
                id_field="test_id",
                parent_field="test_id",  # Same as id_field - should fail
            )


class TestConfigurationSerialization:
    """Test serialization of composed configurations."""

    def test_config_serialization_components(self):
        """Test that composed configurations can be serialized."""
        config = TransmogConfig(
            naming=NamingConfig(separator=".", nested_threshold=2),
            processing=ProcessingConfig(batch_size=500, cast_to_string=False),
        )

        # Test that all components are accessible
        assert config.naming.separator == "."
        assert config.processing.batch_size == 500
        assert config.metadata.id_field == "__transmog_id"  # Default

    def test_config_immutability_in_composition(self):
        """Test that configuration composition preserves immutability."""
        original = TransmogConfig.default()

        # Create config with modifications
        modified = original.with_naming(separator=".").with_processing(batch_size=250)

        # Original should be unchanged
        assert original.naming.separator == "_"
        assert original.processing.batch_size == 1000

        # Modified should have changed values
        assert modified.naming.separator == "."
        assert modified.processing.batch_size == 250

    def test_round_trip_configuration_creation(self):
        """Test creating configurations and modifying them in round trips."""
        # Start with a base configuration
        base = TransmogConfig.memory_optimized()

        # Apply a series of modifications
        step1 = base.with_naming(separator=".")
        step2 = step1.with_processing(batch_size=50)
        step3 = step2.with_metadata(id_field="custom_id")

        # Verify all modifications are preserved
        assert step3.naming.separator == "."
        assert step3.processing.batch_size == 50
        assert step3.metadata.id_field == "custom_id"

        # Verify base configuration inheritance
        assert step3.processing.processing_mode == ProcessingMode.LOW_MEMORY
        assert step3.cache_config.clear_after_batch is True
