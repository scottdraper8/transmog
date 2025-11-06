#!/usr/bin/env python3
"""Script to clean artifacts, build package, and deploy to PyPI.

This script is intended to be run by GitHub Actions after merging to main.
Can also be run manually if needed.
"""

import os
import platform
import re
import shlex
import shutil
import subprocess  # nosec
from pathlib import Path

VERSION_FILES = [
    "src/transmog/__init__.py",
    "pyproject.toml",
    "docs/conf.py",
    "docs/index.md",
]

VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")
VENV_DIR = ".env"


def get_venv_python():
    """Get the path to the Python executable in the virtual environment."""
    venv_path = Path(VENV_DIR)
    if platform.system() == "Windows":
        python_path = venv_path / "Scripts" / "python.exe"
    else:
        python_path = venv_path / "bin" / "python"

    if not python_path.exists():
        raise RuntimeError(f"Python executable not found at {python_path}")

    return python_path


def run_command(command, description=None, check=False):
    """Run a command and handle errors safely.

    Note: This function uses shlex.split for string commands to avoid shell=True.
    All commands are constructed from trusted inputs in a controlled environment.
    """
    if description:
        print(f"{description}...")

    # Convert string commands to lists to avoid shell=True
    if isinstance(command, str):
        cmd = shlex.split(command)
    else:
        cmd = command

    result = subprocess.run(  # nosec # noqa: S603
        cmd,
        check=check,
        text=True,
    )

    return result


def find_current_version():
    """Return the first version string found in VERSION_FILES."""
    for file in VERSION_FILES:
        with open(file, encoding="utf-8") as f:
            for line in f:
                m = VERSION_PATTERN.search(line)
                if m:
                    return m.group(0)
    raise RuntimeError("Version string not found.")


def ensure_tools():
    """Install or upgrade build and twine packages."""
    python_path = get_venv_python()
    run_command(
        [str(python_path), "-m", "pip", "install", "--upgrade", "build", "twine"],
        description="Installing/upgrading build tools",
        check=True,
    )


def clean_artifacts():
    """Remove dist and build directories if present."""
    for d in ["dist", "build"]:
        if os.path.isdir(d):
            shutil.rmtree(d)


def build_and_upload():
    """Build the package and upload to PyPI."""
    python_path = get_venv_python()
    run_command(
        [str(python_path), "-m", "build"],
        description="Building package",
        check=True,
    )
    run_command(
        [str(python_path), "-m", "twine", "upload", "dist/*"],
        description="Uploading to PyPI",
        check=True,
    )


def create_and_push_git_tag(version):
    """Create and push a git tag for the deployed version."""
    tag_name = f"v{version}"
    run_command(
        ["git", "tag", tag_name],
        description=f"Creating git tag {tag_name}",
        check=True,
    )
    run_command(
        ["git", "push", "origin", tag_name],
        description=f"Pushing git tag {tag_name}",
        check=True,
    )


def main():
    """Run the build and deploy process."""
    version = find_current_version()
    print(f"Deploying version {version} to PyPI...")

    ensure_tools()
    clean_artifacts()
    build_and_upload()
    create_and_push_git_tag(version)

    print(f"\n✓ Successfully deployed version {version} to PyPI")


if __name__ == "__main__":
    main()
