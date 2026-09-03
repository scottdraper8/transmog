"""Tests for TransmogConfig configuration management."""

import warnings

import pytest

import transmog as tm
from transmog.config import TransmogConfig
from transmog.exceptions import ConfigurationError
from transmog.types import ArrayMode


class TestConfigDefaults:
    """Test default configuration values."""

    def test_default_values(self):
        """Defaults match the public contract."""
        config = TransmogConfig()

        assert config.batch_size == 5000
        assert config.max_depth == 100
        assert config.array_mode is ArrayMode.SMART
        assert config.include_nulls is False
        assert config.stringify_values is False
        assert config.id_generation == "random"
        assert config.id_field == "_id"
        assert config.parent_field == "_parent_id"
        assert config.time_field == "_timestamp"

    def test_default_batch_size_does_not_warn(self):
        """The default batch size is inside the warning-free range."""
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            TransmogConfig()


class TestConfigCreation:
    """Test TransmogConfig creation and initialization."""

    def test_config_with_custom_params(self):
        """Custom parameters are stored."""
        config = TransmogConfig(
            batch_size=500,
            include_nulls=True,
        )

        assert config.batch_size == 500
        assert config.include_nulls is True

    def test_stringify_values_enabled(self):
        """stringify_values can be enabled."""
        config = TransmogConfig(stringify_values=True)
        assert config.stringify_values is True


class TestConfigValidation:
    """Test configuration validation rules."""

    def test_zero_batch_size_rejected(self):
        """Zero batch size is rejected."""
        with pytest.raises(ConfigurationError, match="Batch size must be at least 1"):
            TransmogConfig(batch_size=0)

    def test_negative_batch_size_rejected(self):
        """Negative batch size is rejected."""
        with pytest.raises(ConfigurationError, match="Batch size must be at least 1"):
            TransmogConfig(batch_size=-1)

    def test_zero_max_depth_rejected(self):
        """Zero max depth is rejected."""
        with pytest.raises(ConfigurationError, match="Max depth must be at least 1"):
            TransmogConfig(max_depth=0)

    def test_duplicate_field_names_rejected(self):
        """Duplicate metadata field names are rejected."""
        with pytest.raises(ConfigurationError, match="must be unique"):
            TransmogConfig(id_field="test", parent_field="test")

        with pytest.raises(ConfigurationError, match="must be unique"):
            TransmogConfig(id_field="test", time_field="test")

    def test_stringify_values_must_be_boolean(self):
        """stringify_values must be a boolean."""
        with pytest.raises(ConfigurationError, match="must be a boolean"):
            TransmogConfig(stringify_values="true")

        with pytest.raises(ConfigurationError, match="must be a boolean"):
            TransmogConfig(stringify_values=1)

    def test_id_generation_empty_list_rejected(self):
        """Empty composite key list is rejected."""
        with pytest.raises(
            ConfigurationError, match="id_generation list cannot be empty"
        ):
            TransmogConfig(id_generation=[])

    def test_id_generation_non_string_list_rejected(self):
        """Non-string composite key items are rejected."""
        with pytest.raises(
            ConfigurationError, match="id_generation list must contain only strings"
        ):
            TransmogConfig(id_generation=[123, "field"])

    def test_id_generation_invalid_type_rejected(self):
        """Non-string, non-list id_generation is rejected."""
        with pytest.raises(
            ConfigurationError, match="id_generation must be a string or list"
        ):
            TransmogConfig(id_generation=123)

    def test_id_generation_invalid_string_rejected(self):
        """Unknown string strategy is rejected."""
        with pytest.raises(ConfigurationError, match="id_generation must be one of"):
            TransmogConfig(id_generation="invalid")


class TestBatchSizeWarnings:
    """Test batch_size range warnings."""

    def test_small_batch_size_warns(self):
        """batch_size below 500 warns about part-file churn."""
        with pytest.warns(UserWarning, match="Small batch_size"):
            TransmogConfig(batch_size=100)

    def test_large_batch_size_warns(self):
        """batch_size above 100000 warns about memory."""
        with pytest.warns(UserWarning, match="Large batch_size"):
            TransmogConfig(batch_size=100_001)

    def test_boundary_batch_sizes_do_not_warn(self):
        """500 and 100000 are inside the warning-free range."""
        with warnings.catch_warnings():
            warnings.simplefilter("error", UserWarning)
            TransmogConfig(batch_size=500)
            TransmogConfig(batch_size=100_000)


class TestMaxDepthBehavior:
    """Test that max_depth actually truncates output."""

    def test_max_depth_truncates_deep_nesting(self):
        """Shallower max_depth produces fewer flattened fields."""
        data = {"a": {"b": {"c": {"d": {"e": "deep_value"}}}}}

        result_shallow = tm.flatten(
            data, name="test", config=TransmogConfig(max_depth=2)
        )
        shallow_keys = {k for k in result_shallow.main[0] if not k.startswith("_")}

        result_deep = tm.flatten(
            data, name="test", config=TransmogConfig(max_depth=100)
        )
        deep_keys = {k for k in result_deep.main[0] if not k.startswith("_")}

        assert len(shallow_keys) < len(deep_keys)

    def test_max_depth_1_only_top_level(self):
        """max_depth=1 only includes top-level scalar fields."""
        data = {"top": "value", "nested": {"inner": "hidden"}}

        result = tm.flatten(data, name="test", config=TransmogConfig(max_depth=1))

        main = result.main[0]
        non_meta = {k: v for k, v in main.items() if not k.startswith("_")}
        assert "top" in non_meta
        assert not any("inner" in k for k in non_meta)
