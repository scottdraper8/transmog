#!/usr/bin/env python3
import os
import shutil
from pathlib import Path


def clean_python_dirs():
    # Directories to clean
    dirs_to_clean = [
        "__pycache__",
        ".egg-info",
        "build",
        "_build",
        "dist",
        ".pytest_cache",
        ".mypy_cache",
    ]

    # Get current directory
    current_dir = Path.cwd()

    # Walk through all directories
    for root, dirs, files in os.walk(current_dir):
        for dir_name in dirs:
            if dir_name in dirs_to_clean:
                dir_path = Path(root) / dir_name
                try:
                    print(f"Removing: {dir_path}")
                    shutil.rmtree(dir_path)
                except Exception as e:
                    print(f"Error removing {dir_path}: {e}")


if __name__ == "__main__":
    print("Cleaning Python cache and build directories...")
    clean_python_dirs()
    print("Done!")
