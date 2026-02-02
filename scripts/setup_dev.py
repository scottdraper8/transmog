#!/usr/bin/env python3
"""Simplified setup script that delegates environment management to Poetry."""

from __future__ import annotations

import os
import shutil
import subprocess  # nosec B404 - Used in a controlled dev environment with appropriate safeguards
import sys
from collections.abc import Sequence
from pathlib import Path

MIN_PYTHON_VERSION = (3, 10)
PROJECT_NAME = "transmog"


def check_python_version() -> None:
    """Check if the current Python version is compatible with the project."""
    current_version = sys.version_info[:2]
    if current_version < MIN_PYTHON_VERSION:
        min_version = ".".join(map(str, MIN_PYTHON_VERSION))
        reported_version = ".".join(map(str, current_version))
        print(f"❌ Python {min_version}+ is required.")
        print(f"Interpreter reports {reported_version}.")
        sys.exit(1)
    print(f"✅ Using Python {'.'.join(map(str, current_version))}")


def run_command(
    command: Sequence[str],
    description: str | None = None,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a subprocess command and handle failures consistently."""
    if description:
        print(f"{description}...")

    resolved = list(command)
    if not resolved:
        raise ValueError("Command must contain at least one argument")

    executable = resolved[0]
    if not os.path.isabs(executable):
        located = shutil.which(executable)
        if located is None:
            print(f"❌ Command {executable} not found on PATH")
            sys.exit(1)
        resolved[0] = located

    result = subprocess.run(  # noqa: S603  # nosec B603
        resolved,
        check=check,
        text=True,
    )

    if result.returncode != 0 and not check:
        print(f"⚠️ Command {' '.join(resolved)} exited with {result.returncode}")

    return result


def ensure_poetry() -> None:
    """Verify that Poetry is available on PATH."""
    poetry_path = shutil.which("poetry")
    if poetry_path is None:
        install_url = "https://python-poetry.org/docs/#installation"
        print(f"❌ Poetry is required. Install via {install_url}")
        sys.exit(1)

    result = subprocess.run(  # noqa: S603  # nosec B603
        [poetry_path, "--version"],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        sys.exit(1)

    print(f"✅ {result.stdout.strip()}")


def install_dependencies() -> None:
    """Install project and development dependencies via Poetry.

    Poetry resolves dependencies to the latest compatible versions
    based on the version constraints specified in pyproject.toml.
    """
    run_command(
        ["poetry", "install", "--extras", "dev"],
        description="Installing dependencies with Poetry (including dev extras)",
        check=True,
    )


def install_pre_commit_hooks() -> None:
    """Install Git hooks using the Poetry-managed environment."""
    run_command(
        ["poetry", "run", "pre-commit", "install"],
        description="Installing pre-commit hook",
        check=True,
    )
    run_command(
        ["poetry", "run", "pre-commit", "install", "--hook-type", "pre-push"],
        description="Installing pre-push hook",
        check=True,
    )


def main() -> None:
    """Set up the development environment."""
    root_dir = Path(__file__).parent.parent.absolute()
    os.chdir(root_dir)

    print("=" * 80)
    print(f"Setting up {PROJECT_NAME.capitalize()} development environment")
    print("=" * 80)

    check_python_version()
    ensure_poetry()
    install_dependencies()
    install_pre_commit_hooks()

    print("\nEnvironment ready.")
    print("Activate the environment with `poetry env activate`.")
    print("Alternatively, install `poetry-plugin-shell` and use `poetry shell`.")
    print("Run individual commands without activation via `poetry run <command>`.")
    print("Pre-commit hooks have been installed and will run on future commits.")


if __name__ == "__main__":
    main()
