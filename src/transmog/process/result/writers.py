"""File writers for ProcessingResult.

Contains methods for writing result data to various file formats
and handling file I/O operations.
"""

import json
import os
from typing import TYPE_CHECKING, Any, Optional

from transmog.error.exceptions import OutputError
from transmog.io.writer_factory import create_writer, is_format_available

from .converters import ResultConverters

if TYPE_CHECKING:
    from .core import ProcessingResult


class ResultWriters:
    """Handles writing ProcessingResult data to files."""

    def __init__(self, result: "ProcessingResult"):
        """Initialize with a ProcessingResult instance.

        Args:
            result: ProcessingResult instance to write
        """
        self.result = result
        self.converters = ResultConverters(result)

    def write_all_json(
        self, base_path: str, indent: Optional[int] = 2, **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to JSON files.

        Args:
            base_path: Base path for output files
            indent: Indentation level for JSON formatting
            **kwargs: Additional JSON writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            OutputError: If writing fails
        """
        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Convert to dictionary structure with tables
        tables = self.result.to_json_objects()

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            # Process each table
            for table_name, records in tables.items():
                # Skip empty tables
                if not records:
                    continue

                # Create the formatted table name
                formatted_name = self.result.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.json")

                # Write to JSON file
                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(records, f, indent=indent, **kwargs)

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write JSON files: {e}",
                output_format="json",
                path=base_path,
            ) from e

    def write_all_csv(
        self, base_path: str, include_header: bool = True, **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to CSV files.

        Args:
            base_path: Base path for output files
            include_header: Whether to include headers in CSV files
            **kwargs: Additional CSV writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            OutputError: If writing fails
        """
        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Get CSV bytes for each table
        csv_bytes = self.converters.to_csv_bytes(
            include_header=include_header, **kwargs
        )

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            # Process each table
            for table_name, data in csv_bytes.items():
                # Skip empty tables
                if not data:
                    continue

                # Create the formatted table name
                formatted_name = self.result.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.csv")

                # Write to CSV file
                with open(file_path, "wb") as f:
                    f.write(data)

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write CSV files: {e}",
                output_format="csv",
                path=base_path,
            ) from e

    def write_all_parquet(
        self, base_path: str, compression: str = "snappy", **kwargs: Any
    ) -> dict[str, str]:
        """Write all tables to Parquet files.

        Args:
            base_path: Base path for output files
            compression: Compression algorithm to use
            **kwargs: Additional Parquet writer options

        Returns:
            Dictionary of table names to file paths

        Raises:
            OutputError: If writing fails
        """
        # Create the base directory if it doesn't exist
        os.makedirs(base_path, exist_ok=True)

        # Get all tables as PyArrow tables
        tables = {"main": self.result.main_table, **self.result.child_tables}

        # Keep track of the paths
        file_paths: dict[str, str] = {}

        try:
            import pyarrow.parquet as pq

            # Process each table
            for table_name, records in tables.items():
                # Skip empty tables
                if not records:
                    continue

                # Create the formatted table name
                formatted_name = self.result.get_formatted_table_name(table_name)
                file_path = os.path.join(base_path, f"{formatted_name}.parquet")

                # Convert records to PyArrow table with config-driven type handling
                arrow_table = self.result._dict_list_to_pyarrow(records)

                # Write to Parquet file
                pq.write_table(
                    arrow_table, file_path, compression=compression, **kwargs
                )

                # Record the file path
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write Parquet files: {e}",
                output_format="parquet",
                path=base_path,
            ) from e

    def write(
        self, format_name: str, base_path: str, **format_options: Any
    ) -> dict[str, str]:
        """Write all tables to files of the specified format.

        Args:
            format_name: Format to write (e.g., 'csv', 'json', 'parquet')
            base_path: Base path for output files
            **format_options: Format-specific options

        Returns:
            Dictionary mapping table names to output file paths

        Raises:
            OutputError: If the output format is not supported
        """
        # Ensure the output directory exists
        os.makedirs(base_path, exist_ok=True)

        # Check if format is supported
        if not is_format_available(format_name):
            raise OutputError(f"Output format {format_name} is not available")

        # Get writer for this format
        writer = create_writer(format_name)

        # Write each table to a file
        output_files = {}

        # Write main table
        main_table_name = self.result.entity_name
        main_filename = os.path.join(base_path, f"{main_table_name}.{format_name}")
        writer.write(self.result.main_table, main_filename, **format_options)
        output_files[main_table_name] = main_filename

        # Write child tables
        for table_name, data in self.result.child_tables.items():
            # Get a safe filename for the table
            safe_table_name = self.result.get_formatted_table_name(table_name)
            output_filename = os.path.join(
                base_path, f"{safe_table_name}.{format_name}"
            )
            writer.write(data, output_filename, **format_options)
            output_files[table_name] = output_filename

        return output_files

    def write_to_file(
        self,
        format_name: str,
        output_directory: str,
        **options: Any,
    ) -> dict[str, str]:
        """Write results to files using the specified writer.

        Args:
            format_name: Format to write in
            output_directory: Directory to write files to
            **options: Writer-specific options

        Returns:
            Dictionary mapping table names to output file paths

        Raises:
            OutputError: If the writer is not available or writing fails
        """
        if not is_format_available(format_name):
            raise OutputError(
                f"Format '{format_name}' is not available",
                output_format=format_name,
            )

        writer = create_writer(format_name)
        os.makedirs(output_directory, exist_ok=True)
        file_paths: dict[str, str] = {}

        try:
            # Write main table
            main_path = os.path.join(
                output_directory, f"{self.result.entity_name}.{format_name}"
            )
            with open(main_path, "wb") as f:
                writer.write(self.result.main_table, f, **options)
            file_paths["main"] = main_path

            # Write child tables
            for table_name, table_data in self.result.child_tables.items():
                # Skip empty tables
                if not table_data:
                    continue

                # Format the table name
                formatted_name = self.result.get_formatted_table_name(table_name)
                file_path = os.path.join(
                    output_directory, f"{formatted_name}.{format_name}"
                )

                # Write the data
                with open(file_path, "wb") as f:
                    writer.write(table_data, f, **options)
                file_paths[table_name] = file_path

            return file_paths
        except Exception as e:
            raise OutputError(
                f"Failed to write {format_name} files: {e}",
                output_format=format_name,
                path=output_directory,
            ) from e
