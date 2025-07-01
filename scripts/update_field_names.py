#!/usr/bin/env python3
"""Script to update old field names to new ones throughout the codebase."""

import os
from pathlib import Path


def update_file(file_path: Path, replacements: dict[str, str]) -> int:
    """Update field names in a single file.

    Returns the number of replacements made.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return 0

    original_content = content
    replacement_count = 0

    # Apply each replacement
    for old, new in replacements.items():
        # Count occurrences before replacement
        count_before = content.count(old)
        # Replace
        content = content.replace(old, new)
        # Count how many replacements were made
        replacement_count += count_before

    # Only write if changes were made
    if content != original_content:
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Updated {file_path}: {replacement_count} replacements")
        except Exception as e:
            print(f"Error writing {file_path}: {e}")
            return 0

    return replacement_count


def main():
    """Update all field names in the codebase."""
    # Define replacements
    replacements = {
        "__extract_id": "__transmog_id",
        "__parent_extract_id": "__parent_transmog_id",
        "__extract_datetime": "__transmog_datetime",
        "generate_extract_id": "generate_transmog_id",
        "extract_time": "transmog_time",
    }

    # Directories to process
    directories = ["tests", "docs", "examples"]

    # File extensions to process
    extensions = [".py", ".md", ".rst", ".json", ".csv"]

    total_files = 0
    total_replacements = 0

    for directory in directories:
        if not os.path.exists(directory):
            print(f"Directory {directory} not found, skipping")
            continue

        for file_path in Path(directory).rglob("*"):
            if file_path.is_file() and file_path.suffix in extensions:
                replacements_in_file = update_file(file_path, replacements)
                if replacements_in_file > 0:
                    total_files += 1
                    total_replacements += replacements_in_file

    print("\nSummary:")
    print(f"Total files updated: {total_files}")
    print(f"Total replacements made: {total_replacements}")


if __name__ == "__main__":
    main()
