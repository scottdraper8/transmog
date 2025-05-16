"""
Dependency management interface tests.

This module defines abstract test classes for dependency management
that all implementations must satisfy.
"""

from unittest import mock

import pytest


class AbstractDependencyManagerTest:
    """
    Abstract test class for dependency management.

    All dependency manager implementations must pass these tests to ensure
    consistent behavior across the system.
    """

    @pytest.fixture
    def dependency_manager_class(self):
        """
        Fixture to provide the dependency manager class.

        Implementations must override this to provide the actual class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def imported_dependency_manager(self):
        """
        Fixture to provide the imported dependency manager.

        Implementations must override this to provide the imported manager.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def check_dependency_func(self):
        """
        Fixture to provide the check_dependency function.

        Implementations must override this to provide the actual function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def require_dependency_func(self):
        """
        Fixture to provide the require_dependency function.

        Implementations must override this to provide the actual function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def missing_dependency_error_class(self):
        """
        Fixture to provide the missing dependency error class.

        Implementations must override this to provide the actual class.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    @pytest.fixture
    def detect_format_func(self):
        """
        Fixture to provide the detect_format function.

        Implementations must override this to provide the actual function.
        """
        raise NotImplementedError("Concrete test classes must implement this fixture")

    def test_dependency_manager_singleton(
        self, dependency_manager_class, imported_dependency_manager
    ):
        """Test that there is only one DependencyManager implementation."""
        # Reset the dependency manager's state
        dependency_manager_class._optional_deps = {}

        # These should all be the same class
        assert dependency_manager_class is imported_dependency_manager

    def test_dependency_checking_functions(
        self, dependency_manager_class, check_dependency_func
    ):
        """Test that dependency checking functions use the central manager."""
        # Reset the dependency manager's state
        dependency_manager_class._optional_deps = {}

        # Mock the has_dependency method
        with mock.patch.object(
            dependency_manager_class, "has_dependency", return_value=True
        ) as mock_has_dep:
            # All these should call the same function
            result1 = dependency_manager_class.has_dependency("test_package")
            result2 = check_dependency_func("test_package")

            assert result1 is True
            assert result2 is True
            assert mock_has_dep.call_count == 2

    def test_require_dependency_uses_central_manager(
        self,
        dependency_manager_class,
        require_dependency_func,
        missing_dependency_error_class,
    ):
        """Test that require_dependency uses the central manager."""
        # Reset the dependency manager's state
        dependency_manager_class._optional_deps = {}

        # Mock the has_dependency method to return False
        with mock.patch.object(
            dependency_manager_class, "has_dependency", return_value=False
        ):
            # Should raise MissingDependencyError
            with pytest.raises(missing_dependency_error_class):
                require_dependency_func("test_package")

    def test_io_formats_uses_central_manager(
        self, dependency_manager_class, detect_format_func
    ):
        """Test that io.formats uses the central manager."""
        # Reset the dependency manager's state
        dependency_manager_class._optional_deps = {}

        # Set up test data
        test_data = {"key": "value"}

        # Mock the has_dependency method for orjson
        with mock.patch.object(
            dependency_manager_class,
            "has_dependency",
            side_effect=lambda x: x == "orjson",
        ) as mock_has_dep:
            # Call detect_format which might use dependency checking internally
            format_name = detect_format_func(test_data)

            # The function should work regardless of dependency availability
            assert format_name == "json"

            # Verify the central manager was used if dependencies were checked
            if mock_has_dep.call_count > 0:
                mock_has_dep.assert_called_with("orjson")

    def test_has_dependency_caching(self, dependency_manager_class):
        """Test that has_dependency caches results."""
        # Reset the dependency manager's state
        dependency_manager_class._optional_deps = {}

        # First call should attempt import
        with mock.patch("importlib.import_module") as mock_import:
            # Set up the mock to succeed for 'existing_pkg' and fail for 'missing_pkg'
            def mock_import_side_effect(name, *args, **kwargs):
                if name == "existing_pkg":
                    return mock.MagicMock()
                else:
                    raise ImportError(f"No module named '{name}'")

            mock_import.side_effect = mock_import_side_effect

            # Test package that exists
            assert dependency_manager_class.has_dependency("existing_pkg") is True
            # Should have called import
            assert mock_import.call_count == 1

            # Test package that doesn't exist
            assert dependency_manager_class.has_dependency("missing_pkg") is False
            # Should have called import again
            assert mock_import.call_count == 2

            # Reset call count
            mock_import.reset_mock()

            # Call has_dependency again for both packages - should use cached results
            assert dependency_manager_class.has_dependency("existing_pkg") is True
            assert dependency_manager_class.has_dependency("missing_pkg") is False
            # Import should not be called again
            assert mock_import.call_count == 0

    def test_register_dependency(self, dependency_manager_class):
        """Test registering a dependency manually."""
        # Reset the dependency manager's state
        dependency_manager_class._optional_deps = {}

        # Register a dependency manually
        dependency_manager_class.register_dependency("custom_pkg", True)

        # Check that it's available
        assert dependency_manager_class.has_dependency("custom_pkg") is True

        # Should not attempt to import the package
        with mock.patch("importlib.import_module") as mock_import:
            assert dependency_manager_class.has_dependency("custom_pkg") is True
            assert mock_import.call_count == 0
