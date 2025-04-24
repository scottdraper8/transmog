"""Setup script for transmogrify package."""

import setuptools

if __name__ == "__main__":
    setuptools.setup(
        package_dir={"": "src"},
        packages=setuptools.find_packages(where="src"),
    )
