"""Base classes for data writers."""

import json
import logging
import math
import os
import re
import warnings
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, BinaryIO, Literal, TextIO

from transmog.exceptions import ConfigurationError

logger = logging.getLogger(__name__)


def _normalize_special_floats(value: Any, null_replacement: Any = None) -> Any:
    """Normalize special float values (NaN, Inf) for output.

    Converts NaN and Infinity to the specified null_replacement value
    for consistent null representation across different formats.

    Args:
        value: Value to normalize
        null_replacement: Value to use for NaN/Inf (default: None)

    Returns:
        Normalized value (null_replacement for NaN/Inf, original value otherwise)
    """
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return null_replacement
    return value


def _collect_field_names(data: list[dict[str, Any]]) -> list[str]:
    """Collect all unique field names from data, preserving insertion order.

    Args:
        data: List of records

    Returns:
        List of unique field names in first-seen order
    """
    if not data:
        return []

    seen: dict[str, None] = {}
    for record in data:
        for key in record:
            if key not in seen:
                seen[key] = None

    return list(seen)


def sanitize_filename(name: str) -> str:
    """Sanitize a string for use as a filename.

    Args:
        name: String to sanitize

    Returns:
        Sanitized filename string
    """
    sanitized = re.sub(r"[^\w\-_.]", "_", name)
    sanitized = re.sub(r"_{2,}", "_", sanitized)
    return sanitized.strip("_")


class DataWriter(ABC):
    """Abstract base class for data writers."""

    @abstractmethod
    def write(
        self,
        data: list[dict[str, Any]],
        destination: str | BinaryIO | TextIO,
        **options: Any,
    ) -> str | BinaryIO | TextIO:
        """Write data to the specified destination.

        Args:
            data: List of dictionaries to write
            destination: File path or file-like object
            **options: Format-specific options

        Returns:
            Path to written file or file-like object
        """
        pass


class StreamingWriter(ABC):
    """Base class for part-file streaming writers.

    Manages the part-file lifecycle: buffering records, flushing batches as
    numbered part files, tracking schema deviations across parts, and
    optionally coercing minority part files to a unified schema at close time.

    Subclasses implement format-specific methods for writing parts, inferring
    schemas, and rewriting files during coercion.
    """

    def __init__(
        self,
        destination: str | None = None,
        entity_name: str = "entity",
        batch_size: int = 5000,
        coerce_schema: bool = False,
        consolidate: bool = True,
        **options: Any,
    ):
        """Initialize the streaming writer.

        Args:
            destination: Output directory path
            entity_name: Name of the entity
            batch_size: Number of records per batch before flushing to part file.
                Typically set via TransmogConfig.batch_size and passed by the
                streaming pipeline.
            coerce_schema: Coerce minority part files to majority schema at close
            consolidate: Merge part files into a single file per table at close
            **options: Format-specific options

        Raises:
            ConfigurationError: If destination is not a string path
        """
        if destination is not None and not isinstance(destination, str):
            raise ConfigurationError(
                "Streaming writers require a directory path destination. "
                "File-like objects are not supported for part-file streaming."
            )
        self.destination = destination
        self.entity_name = entity_name
        self.batch_size = batch_size
        self.coerce_schema = coerce_schema
        self.consolidate = consolidate
        self.options = options

        self.base_dir: str | None = None
        self.buffers: dict[str, list[dict[str, Any]]] = {}
        self.part_counts: dict[str, int] = {}
        self.base_schemas: dict[str, Any] = {}
        self.schema_log: dict[str, dict] = {}
        self.all_part_paths: list[str] = []
        self._part_schemas: dict[str, list[tuple[str, Any]]] = {}

        if isinstance(destination, str):
            self.base_dir = destination
            os.makedirs(self.base_dir, exist_ok=True)

    # --- Abstract methods (subclasses must implement) ---

    @abstractmethod
    def _get_file_extension(self) -> str:
        """Return the file extension including the dot (e.g., '.parquet')."""
        ...

    @abstractmethod
    def _write_part(self, file_path: str, records: list[dict[str, Any]]) -> Any:
        """Write records to a single part file and return the inferred schema.

        The returned schema object is format-specific (e.g., pa.Schema for
        PyArrow, dict for Avro, list[str] for CSV).

        Args:
            file_path: Path for the part file
            records: Records to write

        Returns:
            The schema object used for this part file.
        """
        ...

    @abstractmethod
    def _schema_fields(self, schema: Any) -> list[tuple[str, str]]:
        """Extract (name, type_string) pairs from a format-specific schema.

        Subclasses implement this to bridge format-specific schema objects
        into the common representation used by deviation tracking.
        """
        ...

    def _schema_to_dict(self, schema: Any) -> dict:
        """Serialize a format-specific schema to a JSON-friendly dict."""
        return {
            "fields": [
                {"name": name, "type": type_str}
                for name, type_str in self._schema_fields(schema)
            ]
        }

    def _compute_deviations(self, base_schema: Any, part_schema: Any) -> dict | None:
        """Compute deviations between a base and part schema."""
        base_fields = dict(self._schema_fields(base_schema))
        part_fields = dict(self._schema_fields(part_schema))

        added = sorted(name for name in part_fields if name not in base_fields)
        removed = sorted(name for name in base_fields if name not in part_fields)
        type_changes = {
            name: {"base": base_fields[name], "part": part_fields[name]}
            for name in base_fields
            if name in part_fields and base_fields[name] != part_fields[name]
        }

        if not added and not removed and not type_changes:
            return None

        result: dict[str, Any] = {}
        if added or removed:
            result["structural"] = {"added": added, "removed": removed}
        if type_changes:
            result["type"] = type_changes
        return result

    def _schema_fingerprint(self, schema: Any) -> tuple:
        """Create a hashable fingerprint from a schema for equality comparison."""
        return tuple(self._schema_fields(schema))

    @abstractmethod
    def _build_unified_schema(self, schemas: list[Any]) -> Any:
        """Build a unified schema from a list of per-part schemas."""
        ...

    @abstractmethod
    def _rewrite_part(self, file_path: str, target_schema: Any) -> None:
        """Rewrite a part file to match the target unified schema."""
        ...

    @abstractmethod
    def _consolidate_parts(
        self, output_path: str, part_files: list[str], schema: Any
    ) -> None:
        """Merge multiple part files into a single consolidated file.

        Args:
            output_path: Destination path for the consolidated file
            part_files: Ordered list of part file paths to merge
            schema: Unified schema for the consolidated output
        """
        ...

    # --- Concrete part-file lifecycle methods ---

    def _get_part_path(self, table_name: str, part_num: int) -> str:
        """Get the file path for a numbered part file."""
        base_dir = self.base_dir
        if base_dir is None:
            raise ConfigurationError(
                "Cannot get part path without a destination directory."
            )
        extension = self._get_file_extension()
        if table_name == "main":
            filename = f"{self.entity_name}_part_{part_num:04d}{extension}"
        else:
            safe_name = sanitize_filename(table_name)
            filename = f"{safe_name}_part_{part_num:04d}{extension}"
        return os.path.join(base_dir, filename)

    def _get_consolidated_path(self, table_name: str) -> str:
        """Get the file path for a consolidated single-file output."""
        base_dir = self.base_dir
        if base_dir is None:
            raise ConfigurationError(
                "Cannot get consolidated path without a destination directory."
            )
        extension = self._get_file_extension()
        if table_name == "main":
            filename = f"{self.entity_name}{extension}"
        else:
            safe_name = sanitize_filename(table_name)
            filename = f"{safe_name}{extension}"
        return os.path.join(base_dir, filename)

    def _consolidate_all_tables(self) -> None:
        """Merge all part files into a single file per table."""
        base_dir = self.base_dir
        if base_dir is None:
            return

        consolidated_paths: list[str] = []

        for table_name, part_list in self._part_schemas.items():
            if not part_list:
                continue

            part_files = [os.path.join(base_dir, basename) for basename, _ in part_list]
            consolidated_path = self._get_consolidated_path(table_name)

            if len(part_files) == 1:
                os.rename(part_files[0], consolidated_path)
            else:
                schemas = [schema for _, schema in part_list]
                unified_schema = self._build_unified_schema(schemas)
                self._consolidate_parts(consolidated_path, part_files, unified_schema)
                for part_file in part_files:
                    os.remove(part_file)

            consolidated_paths.append(consolidated_path)

        self.all_part_paths = consolidated_paths

    def _log_schema(self, table_name: str, file_path: str, schema: Any) -> None:
        """Log schema for a part file, tracking deviations from the base."""
        basename = os.path.basename(file_path)

        if table_name not in self.schema_log:
            self.base_schemas[table_name] = schema
            self.schema_log[table_name] = {
                "base_schema": self._schema_to_dict(schema),
                "parts": [{"file": basename, "deviations": None}],
            }
        else:
            deviations = self._compute_deviations(self.base_schemas[table_name], schema)
            self.schema_log[table_name]["parts"].append(
                {"file": basename, "deviations": deviations}
            )

    def _write_schema_log(self) -> None:
        """Write the schema log to _schema_log.json in the output directory."""
        base_dir = self.base_dir
        if not base_dir or not self.schema_log:
            return
        log_path = os.path.join(base_dir, "_schema_log.json")
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump({"tables": self.schema_log}, f, indent=2)

    def _warn_deviations(self) -> None:
        """Emit warnings for schema deviations detected across part files."""
        for table_name, log_data in self.schema_log.items():
            structural_parts: list[str] = []
            type_parts: list[str] = []
            all_structural: dict[str, set[str]] = {"added": set(), "removed": set()}
            all_type_changes: dict[str, dict[str, str]] = {}

            for part in log_data["parts"]:
                devs = part["deviations"]
                if devs is None:
                    continue
                if "structural" in devs:
                    structural_parts.append(part["file"])
                    all_structural["added"].update(devs["structural"]["added"])
                    all_structural["removed"].update(devs["structural"]["removed"])
                if "type" in devs:
                    type_parts.append(part["file"])
                    all_type_changes.update(devs["type"])

            if not structural_parts and not type_parts:
                continue

            lines = [
                f"Schema deviations detected across part files "
                f"for table '{table_name}':"
            ]
            if structural_parts:
                lines.append(
                    f"  Structural ({len(structural_parts)} parts): "
                    + ", ".join(structural_parts)
                )
                if all_structural["added"]:
                    lines.append(f"    Added fields: {sorted(all_structural['added'])}")
                if all_structural["removed"]:
                    lines.append(
                        f"    Removed fields: {sorted(all_structural['removed'])}"
                    )
            if type_parts:
                lines.append(
                    f"  Type changes ({len(type_parts)} parts): "
                    + ", ".join(type_parts)
                )
                for field_name, change in sorted(all_type_changes.items()):
                    lines.append(
                        f"    '{field_name}': {change['base']} -> {change['part']}"
                    )
            lines.append("See _schema_log.json for full details.")

            warnings.warn("\n".join(lines), UserWarning, stacklevel=2)

    def _coerce_part_files(self) -> None:
        """Coerce minority part files to a unified schema.

        Builds a unified schema from all parts, identifies files that don't
        match it, and rewrites them via the subclass _rewrite_part() method.
        """
        base_dir = self.base_dir
        if base_dir is None:
            return

        for table_name, log_data in self.schema_log.items():
            parts_with_deviations = [
                p for p in log_data["parts"] if p["deviations"] is not None
            ]
            if not parts_with_deviations:
                continue

            part_schemas: dict[str, Any] = dict(self._part_schemas.get(table_name, []))

            target_schema = self._build_unified_schema(list(part_schemas.values()))
            target_fp = self._schema_fingerprint(target_schema)

            minority_files = [
                name
                for name, schema in part_schemas.items()
                if self._schema_fingerprint(schema) != target_fp
            ]

            if not minority_files:
                continue

            logger.info(
                "coercing %d part files for table '%s' to unified schema",
                len(minority_files),
                table_name,
            )

            for filename in minority_files:
                file_path = os.path.join(base_dir, filename)
                self._rewrite_part(file_path, target_schema)

                for part_entry in log_data["parts"]:
                    if part_entry["file"] == filename:
                        part_entry["coerced_to"] = self._schema_to_dict(target_schema)
                        break

    def _write_buffer(self, table_name: str) -> None:
        """Write buffered records as a new part file."""
        if table_name not in self.buffers or not self.buffers[table_name]:
            return

        if self.base_dir is None:
            self.buffers[table_name].clear()
            return

        records = self.buffers[table_name]

        part_num = self.part_counts.get(table_name, 0)
        file_path = self._get_part_path(table_name, part_num)
        self.part_counts[table_name] = part_num + 1

        schema = self._write_part(file_path, records)

        self.all_part_paths.append(file_path)
        if table_name not in self._part_schemas:
            self._part_schemas[table_name] = []
        self._part_schemas[table_name].append((os.path.basename(file_path), schema))
        self._log_schema(table_name, file_path, schema)
        self.buffers[table_name].clear()

    def write_main_records(self, records: list[dict[str, Any]]) -> None:
        """Write a batch of main records.

        Args:
            records: Main table records to write
        """
        if not records:
            return

        table_name = "main"

        if table_name not in self.buffers:
            self.buffers[table_name] = []

        self.buffers[table_name].extend(records)

        if len(self.buffers[table_name]) >= self.batch_size:
            self._write_buffer(table_name)

    def write_child_records(
        self, table_name: str, records: list[dict[str, Any]]
    ) -> None:
        """Write a batch of child records.

        Args:
            table_name: Name of the child table
            records: Child records to write
        """
        if not records:
            return

        if table_name not in self.buffers:
            self.buffers[table_name] = []

        self.buffers[table_name].extend(records)

        if len(self.buffers[table_name]) >= self.batch_size:
            self._write_buffer(table_name)

    def close(self) -> list[Path]:
        """Finalize output, flush buffered data, and clean up resources.

        Writes remaining buffered records, warns about schema deviations,
        optionally coerces minority part files, and optionally consolidates
        part files into a single file per table.

        Returns:
            List of file paths written, or empty list if no directory set.
        """
        if getattr(self, "_closed", False):
            return []

        for table_name in list(self.buffers.keys()):
            if self.buffers[table_name]:
                self._write_buffer(table_name)

        if self.base_dir and self.schema_log:
            self._warn_deviations()

            if self.coerce_schema:
                has_deviations = any(
                    any(p["deviations"] is not None for p in log["parts"])
                    for log in self.schema_log.values()
                )
                if has_deviations:
                    logger.warning(
                        "coerce_schema is enabled: minority part files will be "
                        "reread and rewritten, incurring additional I/O"
                    )
                    self._coerce_part_files()

            if self.consolidate:
                self._consolidate_all_tables()
            else:
                self._write_schema_log()

        paths = [Path(p) for p in self.all_part_paths]

        self.buffers.clear()
        self.part_counts.clear()
        self._part_schemas.clear()
        self._closed = True
        return paths

    def __enter__(self) -> "StreamingWriter":
        """Support for context manager protocol."""
        return self

    def __exit__(self, _exc_type: Any, _exc_val: Any, _exc_tb: Any) -> Literal[False]:
        """Finalize when exiting context."""
        self.close()
        return False


__all__ = ["DataWriter", "StreamingWriter", "_normalize_special_floats"]
