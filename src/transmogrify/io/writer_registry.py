"""
Writer registry module for managing output format writers.

This module provides a registry for different writer implementations,
allowing for format-specific writers to be registered and used.
"""

import importlib
from typing import Any, Dict, List, Optional, Type, Callable

from src.transmogrify.exceptions import ConfigurationError

# Import this in a way that avoids circular imports
# The class will be properly typed at runtime
if False:  # for type checking only
    from src.transmogrify.io.writer_interface import DataWriter


class WriterRegistry:
    """
    Registry for writer implementations.

    This class maintains a registry of writer classes for different formats,
    allowing the appropriate writer to be created based on the format name.
    """

    # Dictionary of registered writer classes, keyed by format name
    _writers: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def register(cls, writer_class: Type[Any]) -> None:
        """
        Register a writer class for a specific format.

        Args:
            writer_class: The writer class to register
        """
        format_name = getattr(writer_class, "format_name", None)

        if not format_name:
            return

        cls._writers[format_name] = {
            "class": writer_class,
            "module": writer_class.__module__,
            "loaded": True,
        }

    @classmethod
    def register_format(
        cls, format_name: str, module_path: str, class_name: str
    ) -> None:
        """
        Register a format without immediately loading the implementation.

        This method allows formats to be registered without importing the
        actual implementation, avoiding potential circular imports.

        Args:
            format_name: The name of the format
            module_path: The path to the module containing the implementation
            class_name: The name of the writer class
        """
        cls._writers[format_name] = {
            "class": None,
            "module": module_path,
            "class_name": class_name,
            "loaded": False,
        }

    @classmethod
    def _load_writer(cls, format_name: str) -> Optional[Type[Any]]:
        """
        Load a writer class if it hasn't been loaded yet.

        Args:
            format_name: The format name to load

        Returns:
            The loaded writer class or None if not available
        """
        if format_name not in cls._writers:
            return None

        writer_info = cls._writers[format_name]

        # If already loaded, return the class
        if writer_info.get("loaded", False) and writer_info.get("class") is not None:
            return writer_info["class"]

        # Otherwise, try to load it
        try:
            module = importlib.import_module(writer_info["module"])
            writer_class = getattr(module, writer_info["class_name"])

            # Update the registry with the loaded class
            writer_info["class"] = writer_class
            writer_info["loaded"] = True

            return writer_class
        except (ImportError, AttributeError):
            # Mark as failed to avoid repeated import attempts
            writer_info["loaded"] = False
            return None

    @classmethod
    def is_format_available(cls, format_name: str) -> bool:
        """
        Check if a writer is available for the given format.

        Args:
            format_name: The format name to check

        Returns:
            Whether a writer is available for this format
        """
        # If not registered at all, it's not available
        if format_name not in cls._writers:
            return False

        writer_info = cls._writers[format_name]

        # If already loaded successfully, it's available
        if writer_info.get("loaded", False) and writer_info.get("class") is not None:
            return True

        # Try to load it now to check availability
        writer_class = cls._load_writer(format_name)
        return writer_class is not None

    @classmethod
    def create_writer(cls, format_name: str) -> Any:
        """
        Create a writer instance for the specified format.

        Args:
            format_name: The format to create a writer for

        Returns:
            An instance of the writer

        Raises:
            ValueError: If no writer is available for the format
        """
        writer_class = cls._load_writer(format_name)

        if writer_class is None:
            raise ValueError(f"No writer available for format: {format_name}")

        return writer_class()

    @classmethod
    def list_available_formats(cls) -> List[str]:
        """
        List all available format names.

        Returns:
            List of available format names
        """
        available = []

        for format_name in cls._writers:
            if cls.is_format_available(format_name):
                available.append(format_name)

        return available

    @classmethod
    def get_writer(cls, format_name: str) -> "DataWriter":
        """
        Get a writer instance by format name.

        Args:
            format_name: The format name to get a writer for

        Returns:
            An instance of the appropriate writer

        Raises:
            ConfigurationError: If no writer is found for the format
        """
        if format_name.lower() not in cls._writers:
            available = ", ".join(cls.list_available_formats())
            if not available:
                available = "(none)"
            raise ConfigurationError(
                f"No writer found for format '{format_name}'. "
                f"Available formats: {available}"
            )

        writer_class = cls._load_writer(format_name.lower())
        if writer_class is None:
            available = ", ".join(cls.list_available_formats())
            raise ConfigurationError(
                f"Failed to load writer for format '{format_name}'. "
                f"Available formats: {available}"
            )
        return writer_class()

    @classmethod
    def list_registered_formats(cls) -> List[str]:
        """List all registered format names."""
        return list(cls._writers.keys())
