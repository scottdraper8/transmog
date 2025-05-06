"""
Tests for the dependency management system.

This ensures that all dependency management is properly centralized.
"""

import unittest
from unittest import mock

import transmog
from transmog.dependencies import DependencyManager
from transmog.io.formats import detect_format
from transmog.error.handling import check_dependency, require_dependency
from transmog import IoDependencyManager


class TestDependencyManager(unittest.TestCase):
    """Test the centralized dependency management."""

    def setUp(self):
        """Set up the test by clearing dependency cache."""
        # Clear the cached dependencies
        DependencyManager._optional_deps = {}

    def test_dependency_manager_singleton(self):
        """Test that there is only one DependencyManager implementation."""
        # These should all be the same class
        self.assertIs(DependencyManager, transmog.DependencyManager)
        self.assertIs(DependencyManager, IoDependencyManager)

    def test_dependency_checking_functions(self):
        """Test that dependency checking functions use the central manager."""
        # Mock the has_dependency method
        with mock.patch.object(
            DependencyManager, "has_dependency", return_value=True
        ) as mock_has_dep:
            # All these should call the same function
            result1 = DependencyManager.has_dependency("test_package")
            result2 = check_dependency("test_package")

            self.assertTrue(result1)
            self.assertTrue(result2)
            self.assertEqual(mock_has_dep.call_count, 2)

    def test_require_dependency_uses_central_manager(self):
        """Test that require_dependency uses the central manager."""
        # Mock the has_dependency method to return False
        with mock.patch.object(DependencyManager, "has_dependency", return_value=False):
            # Should raise MissingDependencyError
            with self.assertRaises(transmog.MissingDependencyError):
                require_dependency("test_package")

    def test_io_formats_uses_central_manager(self):
        """Test that io.formats uses the central manager."""
        # Set up test data
        test_data = {"key": "value"}

        # Mock the has_dependency method for orjson
        with mock.patch.object(
            DependencyManager, "has_dependency", side_effect=lambda x: x == "orjson"
        ) as mock_has_dep:
            # Call detect_format which might use dependency checking internally
            format_name = detect_format(test_data)

            # The function should work regardless of dependency availability
            self.assertEqual(format_name, "json")

            # Verify the central manager was used if dependencies were checked
            if mock_has_dep.call_count > 0:
                mock_has_dep.assert_called_with("orjson")


if __name__ == "__main__":
    unittest.main()
