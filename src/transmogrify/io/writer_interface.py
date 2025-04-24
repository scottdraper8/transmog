"""
Writer interface for Transmogrify output formats.

This module defines the interface that all format writers must implement.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class DataWriter(ABC):
    """Interface for data writers that output ProcessingResult data to various formats."""

    @classmethod
    @abstractmethod
    def format_name(cls) -> str:
        """Return the name of the format this writer handles."""
        pass

    @classmethod
    @abstractmethod
    def is_available(cls) -> bool:
        """Check if this writer's dependencies are available."""
        pass

    @abstractmethod
    def write_table(
        self, table_data: List[Dict[str, Any]], output_path: str, **kwargs
    ) -> str:
        """Write a single table to the output format.

        Args:
            table_data: List of records to write
            output_path: Path to write the output file
            **kwargs: Format-specific options

        Returns:
            Path to the written file
        """
        pass

    @abstractmethod
    def write_all_tables(
        self,
        main_table: List[Dict[str, Any]],
        child_tables: Dict[str, List[Dict[str, Any]]],
        base_path: str,
        entity_name: str,
        **kwargs,
    ) -> Dict[str, str]:
        """Write main and child tables to the output format.

        Args:
            main_table: Main table data
            child_tables: Dict of child table name to table data
            base_path: Base directory for output
            entity_name: Name of the main entity
            **kwargs: Format-specific options

        Returns:
            Dict mapping table names to output file paths
        """
        pass
