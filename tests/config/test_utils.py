"""
Tests for config/utils.py module.
"""

from transmog.config import TransmogConfig
from transmog.config.utils import get_common_config_params


def test_get_common_config_params():
    """Test that get_common_config_params returns expected parameters."""
    # Create a config with non-default settings
    config = (
        TransmogConfig.default()
        .with_naming(
            separator="|",
            abbreviate_field_names=False,
            abbreviate_table_names=False,
            max_field_component_length=10,
            max_table_component_length=10,
            preserve_root_component=False,
            preserve_leaf_component=False,
            custom_abbreviations={"test": "t"},
        )
        .with_processing(
            cast_to_string=False,
            include_empty=True,
            skip_null=False,
            visit_arrays=False,
            max_nesting_depth=5,
        )
    )

    # Get common params
    params = get_common_config_params(config)

    # Verify all expected parameters are present
    assert params["separator"] == "|"
    assert params["abbreviate_field_names"] is False
    assert params["abbreviate_table_names"] is False
    assert params["max_field_component_length"] == 10
    assert params["max_table_component_length"] == 10
    assert params["preserve_root_component"] is False
    assert params["preserve_leaf_component"] is False
    assert params["custom_abbreviations"] == {"test": "t"}
    assert params["cast_to_string"] is False
    assert params["include_empty"] is True
    assert params["skip_null"] is False
    assert params["visit_arrays"] is False
    assert params["max_depth"] == 5


def test_get_common_config_params_default():
    """Test that get_common_config_params works with default config."""
    # Use default config
    config = TransmogConfig.default()

    # Get common params
    params = get_common_config_params(config)

    # Verify default values are present
    assert params["separator"] == "_"
    assert params["abbreviate_field_names"] is True
    assert params["abbreviate_table_names"] is True
    assert params["max_field_component_length"] is None  # Default is None in the class
    assert params["max_table_component_length"] is None  # Default is None in the class
    assert params["preserve_root_component"] is True
    assert params["preserve_leaf_component"] is True
    assert params["custom_abbreviations"] == {}
    assert params["cast_to_string"] is True
    assert params["include_empty"] is False
    assert params["skip_null"] is True
    assert params["visit_arrays"] is True
    assert params["max_depth"] is None
