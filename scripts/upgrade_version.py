#!/usr/bin/env python3
"""Script to increment all version mentions, clean artifacts, and deploy to PyPI."""

import os
import re
import shlex
import shutil
import subprocess  # nosec
import sys

# Version bumping logic
VERSION_FILES = [
    "src/transmog/__init__.py",
    "pyproject.toml",
    "docs/conf.py",
    "docs/index.md",
]

VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")


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


def bump_version(version):
    """Return the next version string by incrementing the smallest component."""
    major, minor, patch = map(int, version.split("."))
    if patch < 9:
        patch += 1
    else:
        patch = 0
        if minor < 9:
            minor += 1
        else:
            minor = 0
            major += 1
    return f"{major}.{minor}.{patch}"


def update_versions(old_version, new_version):
    """Replace old_version with new_version in all VERSION_FILES."""
    for file in VERSION_FILES:
        with open(file, encoding="utf-8") as f:
            content = f.read()
        content = content.replace(old_version, new_version)
        with open(file, "w", encoding="utf-8") as f:
            f.write(content)


def ensure_tools():
    """Install or upgrade build and twine packages."""
    run_command(
        [sys.executable, "-m", "pip", "install", "--upgrade", "build", "twine"],
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
    run_command(
        [sys.executable, "-m", "build"],
        description="Building package",
        check=True,
    )
    run_command(
        [sys.executable, "-m", "twine", "upload", "dist/*"],
        description="Uploading to PyPI",
        check=True,
    )


def create_and_push_git_tag(version):
    """Create and push a git tag for the new version."""
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
    """Run the version upgrade, build, and deploy process."""
    ensure_tools()
    old_version = find_current_version()
    new_version = bump_version(old_version)
    update_versions(old_version, new_version)
    clean_artifacts()
    build_and_upload()
    create_and_push_git_tag(new_version)
    print(f"Upgraded version: {old_version} -> {new_version}")


if __name__ == "__main__":
    main()
