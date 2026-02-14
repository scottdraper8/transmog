"""Tests for base writer utility functions."""

import math

import pytest

from transmog.writers.base import _normalize_special_floats, _sanitize_filename


class TestSanitizeFilename:
    """Test _sanitize_filename function."""

    def test_alphanumeric_unchanged(self):
        """Test that alphanumeric strings pass through unchanged."""
        assert _sanitize_filename("simple_name") == "simple_name"

    def test_special_characters_replaced(self):
        """Test that special characters are replaced with underscores."""
        assert _sanitize_filename("my file!@#") == "my_file"

    def test_consecutive_underscores_collapsed(self):
        """Test that consecutive underscores are collapsed."""
        assert _sanitize_filename("a___b") == "a_b"

    def test_leading_trailing_underscores_stripped(self):
        """Test that leading/trailing underscores are stripped."""
        result = _sanitize_filename("__test__")
        assert not result.startswith("_")
        assert not result.endswith("_")
        assert result == "test"

    def test_dots_and_hyphens_preserved(self):
        """Test that dots and hyphens are preserved."""
        assert _sanitize_filename("file-name.csv") == "file-name.csv"

    def test_spaces_replaced(self):
        """Test that spaces are replaced."""
        result = _sanitize_filename("my table name")
        assert " " not in result
        assert result == "my_table_name"

    def test_slash_replaced(self):
        """Test that path separators are replaced."""
        result = _sanitize_filename("path/to/file")
        assert "/" not in result


class TestNormalizeSpecialFloats:
    """Test _normalize_special_floats function."""

    def test_nan_returns_none(self):
        """Test that NaN is normalized to None."""
        assert _normalize_special_floats(float("nan")) is None

    def test_inf_returns_none(self):
        """Test that Infinity is normalized to None."""
        assert _normalize_special_floats(float("inf")) is None

    def test_negative_inf_returns_none(self):
        """Test that -Infinity is normalized to None."""
        assert _normalize_special_floats(float("-inf")) is None

    def test_custom_null_replacement(self):
        """Test custom null_replacement value."""
        assert _normalize_special_floats(float("nan"), null_replacement=0) == 0
        assert (
            _normalize_special_floats(float("inf"), null_replacement="NULL") == "NULL"
        )

    def test_regular_float_unchanged(self):
        """Test that regular floats pass through unchanged."""
        assert _normalize_special_floats(3.14) == 3.14
        assert _normalize_special_floats(0.0) == 0.0
        assert _normalize_special_floats(-1.5) == -1.5

    def test_non_float_unchanged(self):
        """Test that non-float values pass through unchanged."""
        assert _normalize_special_floats("string") == "string"
        assert _normalize_special_floats(42) == 42
        assert _normalize_special_floats(None) is None
        assert _normalize_special_floats(True) is True
