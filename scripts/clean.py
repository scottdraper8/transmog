#!/usr/bin/env python
"""Clean script to remove Python cache directories and other temporary files."""

import os
import shutil
from pathlib import Path


def clean_python_dirs():
    """Remove Python cache directories and other temporary files."""
    # Directories to clean
    dirs_to_clean = [
        "__pycache__",
        ".pytest_cache",
        ".ruff_cache",
        ".mypy_cache",
        ".coverage_cache",
        ".eggs",
        "*.egg-info",
    ]

    # Get the current directory
    current_dir = Path(__file__).parent.parent.resolve()

    # Walk through all directories
    for root, dirs, _files in os.walk(current_dir):
        for dir_name in dirs:
            if dir_name in dirs_to_clean:
                try:
                    shutil.rmtree(os.path.join(root, dir_name))
                    print(f"Removed {os.path.join(root, dir_name)}")
                except OSError as e:
                    print(f"Error: {e.strerror} - {os.path.join(root, dir_name)}")


if __name__ == "__main__":
    clean_python_dirs()
    print("Clean completed!")
