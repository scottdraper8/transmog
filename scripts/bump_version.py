#!/usr/bin/env python3
"""Script to increment version numbers across the codebase.

Used when creating a new version branch. Does not deploy to PyPI.
"""

import re

# Version bumping logic
VERSION_FILES = [
    "src/transmog/__init__.py",
    "pyproject.toml",
    "docs/conf.py",
    "docs/index.md",
]

VERSION_PATTERN = re.compile(r"(\d+)\.(\d+)\.(\d+)")


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
    """Return the next version string by incrementing the patch component."""
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


def main():
    """Run the version bump process."""
    old_version = find_current_version()
    new_version = bump_version(old_version)
    update_versions(old_version, new_version)
    print(f"Version bumped: {old_version} -> {new_version}")
    print("\nNext steps:")
    print("1. Review the changes: git diff")
    print(f"2. Create a new branch: git checkout -b v{new_version}")
    print(f"3. Commit the changes: git add . && git commit -m 'Bump to {new_version}'")
    print(f"4. Push the branch: git push -u origin v{new_version}")
    print("5. Make your updates/fixes on this branch")
    print("6. When ready, merge to main to trigger PyPI deployment")


if __name__ == "__main__":
    main()
