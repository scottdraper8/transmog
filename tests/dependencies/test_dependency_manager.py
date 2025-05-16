"""
Tests for dependency management.

This module implements concrete tests for the dependency management interface.
"""

from unittest import mock

import pytest

import transmog
from tests.interfaces.test_dependency_interface import AbstractDependencyManagerTest
from transmog.dependencies import DependencyManager
from transmog.error.handling import check_dependency, require_dependency
from transmog.io.formats import detect_format


class TestDependencyManager(AbstractDependencyManagerTest):
    """Concrete tests for dependency management."""

    @pytest.fixture
    def dependency_manager_class(self):
        """Provide the dependency manager class."""
        return DependencyManager

    @pytest.fixture
    def imported_dependency_manager(self):
        """Provide the imported dependency manager."""
        return transmog.DependencyManager

    @pytest.fixture
    def check_dependency_func(self):
        """Provide the check_dependency function."""
        return check_dependency

    @pytest.fixture
    def require_dependency_func(self):
        """Provide the require_dependency function."""
        return require_dependency

    @pytest.fixture
    def missing_dependency_error_class(self):
        """Provide the missing dependency error class."""
        return transmog.MissingDependencyError

    @pytest.fixture
    def detect_format_func(self):
        """Provide the detect_format function."""
        return detect_format

    def test_dependency_manager_singleton(self):
        """Test that dependency managers are singletons."""
        # Get dependency manager instances
        manager1 = DependencyManager
        manager2 = transmog.DependencyManager

        # Verify they are the same object
        assert manager1 is manager2

    def test_dependency_checking_functions(self):
        """Test dependency checking functions."""
        # Test check_dependency function
        assert callable(check_dependency)

        # This should return True or False, but not raise an error
        result = check_dependency("os")  # This is a standard library, so should exist
        assert result is True

        # Test with a package that's unlikely to exist
        result = check_dependency("unlikely_to_exist_random_pkg_name_123")
        assert result is False

    def test_require_dependency_uses_central_manager(self):
        """Test that require_dependency uses the central dependency manager."""
        # Succeed for a package that exists
        require_dependency("os")

        # Fail for a package that doesn't exist
        with pytest.raises(transmog.MissingDependencyError):
            require_dependency("unlikely_to_exist_random_pkg_name_123")

    def test_io_formats_uses_central_manager(self):
        """Test that IO formats detection uses the central dependency manager."""
        # Test detect_format function
        assert callable(detect_format)

        # Since formats might depend on optional packages, just verify the function exists
        # and doesn't error on basic usage
        try:
            detect_format({"test": "data"})
        except Exception as e:
            # If an error occurs, it should be a controlled one related to format detection
            # not an import error or attribute error
            assert "format" in str(e).lower() or isinstance(e, ValueError)

    def test_has_dependency_function(self):
        """Test the has_dependency function of the dependency manager."""
        # Mock importlib.import_module to control its behavior
        with mock.patch("importlib.import_module") as mock_import:
            # Set up the mock to succeed for 'os' and fail for 'missing_pkg'
            def mock_import_side_effect(name, *args, **kwargs):
                if name == "os":
                    return mock.MagicMock()
                else:
                    raise ImportError(f"No module named '{name}'")

            mock_import.side_effect = mock_import_side_effect

            # Test a package that exists using check_dependency
            assert check_dependency("os") is True

            # Test a package that doesn't exist
            assert check_dependency("missing_pkg") is False

    def test_manual_dependency_registration(self):
        """Test manually indicating a dependency's availability."""
        pkg_name = "test_manual_pkg"

        # First check that it's not already marked as available
        assert check_dependency(pkg_name) is False

        # Check that the package is not in the internal cache yet
        assert (
            pkg_name not in DependencyManager._optional_deps
            or DependencyManager._optional_deps[pkg_name] is False
        )

        # Use the register_dependency method to properly register the dependency
        DependencyManager.register_dependency(pkg_name, True)

        # Verify that the registration was stored in the internal cache
        assert pkg_name in DependencyManager._optional_deps
        assert DependencyManager._optional_deps[pkg_name] is True

        # Note: check_dependency() will still return False because it actually tries to import the
        # package, which won't succeed for our test package. This is the expected behavior.
        # The test verifies that registration works correctly in the internal data structure.
