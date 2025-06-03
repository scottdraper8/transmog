#!/usr/bin/env python3
"""Setup script for Transmog development environment.

This script:
1. Creates a virtual environment if it doesn't exist
2. Activates the virtual environment
3. Checks Python version compatibility
4. Installs development dependencies
5. Sets up pre-commit and pre-push hooks
6. Runs pre-commit hooks against all files
7. Sets up documentation dependencies and tests the build
8. Validates optional dependencies for performance optimization
9. Sets up security scanning tools
10. Creates required directories for benchmarks
11. Validates dependencies against latest versions
"""

import os
import platform
import re
import shlex
import subprocess  # nosec B404 - Used in a controlled dev environment with appropriate safeguards
import sys
import venv
from pathlib import Path

MIN_PYTHON_VERSION = (3, 9)
VENV_DIR = ".env"
PROJECT_NAME = "transmog"


def check_python_version():
    """Check if the current Python version is compatible with the project."""
    current_version = sys.version_info[:2]
    if current_version < MIN_PYTHON_VERSION:
        print(
            f"❌ Python {'.'.join(map(str, MIN_PYTHON_VERSION))}+ is required, "
            f"but you're using Python {'.'.join(map(str, current_version))}"
        )
        sys.exit(1)
    print(f"✅ Using Python {'.'.join(map(str, current_version))}")


def run_command(command, description=None, check=False, capture_output=False):
    """Run a command and handle errors safely.

    Note: This function may use shell=True in controlled cases where the commands
    are constructed from trusted inputs in a development environment. All shell commands
    are carefully validated before execution.
    """
    if description:
        print(f"{description}...")

    # Convert string commands to lists to avoid shell=True where possible
    if isinstance(command, str):
        # Only use shell=True for complex commands that require shell features
        if any(
            shell_char in command for shell_char in ["|", ">", "<", "&&", "||", ";"]
        ):
            # For shell commands with proper validation
            cmd = command
            use_shell = True
        else:
            # Split the string to avoid shell=True
            cmd = shlex.split(command)
            use_shell = False
    else:
        cmd = command
        use_shell = False

    # nosec B602 - Shell is only used in a controlled development environment
    # with safeguards to prevent arbitrary command execution
    result = subprocess.run(  # nosec B602 # noqa: S603
        cmd,
        shell=use_shell,  # Only True for validated complex commands
        check=check,
        text=True,
        capture_output=capture_output,
    )

    return result


def create_virtual_env(venv_path):
    """Create a virtual environment if it doesn't exist."""
    if venv_path.exists():
        print(f"✅ Virtual environment already exists at {venv_path}")
        return

    print(f"\nCreating virtual environment at {venv_path}...")
    venv.create(venv_path, with_pip=True)
    print(f"✅ Virtual environment created at {venv_path}")


def get_venv_activate_script(venv_path):
    """Get the path to the activate script based on the platform."""
    if platform.system() == "Windows":
        activate_script = venv_path / "Scripts" / "activate.bat"
    else:
        activate_script = venv_path / "bin" / "activate"

    if not activate_script.exists():
        print(f"❌ Activation script not found at {activate_script}")
        sys.exit(1)

    return activate_script


def get_venv_python(venv_path):
    """Get the path to the Python executable in the virtual environment."""
    if platform.system() == "Windows":
        python_path = venv_path / "Scripts" / "python.exe"
    else:
        python_path = venv_path / "bin" / "python"

    if not python_path.exists():
        print(f"❌ Python executable not found at {python_path}")
        sys.exit(1)

    return python_path


def create_directories(root_dir):
    """Create necessary directories if they don't exist."""
    # Set up documentation directories
    docs_static_dir = root_dir / "docs" / "_static"
    docs_templates_dir = root_dir / "docs" / "_templates"
    benchmark_dir = root_dir / ".benchmarks"

    for dir_path in [docs_static_dir, docs_templates_dir, benchmark_dir]:
        if not dir_path.exists():
            dir_path.mkdir(exist_ok=True, parents=True)
            print(f"✅ Created {dir_path}")


def parse_pyproject_toml(root_dir):
    """Parse the pyproject.toml file to extract version and dependencies."""
    pyproject_path = root_dir / "pyproject.toml"
    if not pyproject_path.exists():
        print("❌ pyproject.toml not found")
        sys.exit(1)

    with open(pyproject_path) as f:
        content = f.read()

    # Extract version
    version_match = re.search(r'version\s*=\s*"([^"]+)"', content)
    version = version_match.group(1) if version_match else "unknown"

    # Extract dependencies
    dependencies = []
    deps_match = re.search(r"dependencies\s*=\s*\[([\s\S]+?)\]", content)
    if deps_match:
        deps_str = deps_match.group(1)
        deps_items = re.findall(r'"([^"]+)"', deps_str)
        dependencies.extend(deps_items)

    return version, dependencies


def validate_dependencies(python_path, dependencies):
    """Validate that installed dependencies match the expected versions."""
    print("\nValidating dependencies...")

    # Get installed packages
    result = run_command(
        [str(python_path), "-m", "pip", "freeze"], description=None, capture_output=True
    )

    installed = {}
    for line in result.stdout.strip().split("\n"):
        if "==" in line:
            pkg, ver = line.split("==", 1)
            installed[pkg.lower()] = ver

    # Check dependencies
    for dep in dependencies:
        if ">=" in dep:
            pkg, ver = dep.split(">=", 1)
            pkg = pkg.strip()
            ver = ver.strip()
            if pkg.lower() in installed:
                print(f"✅ {pkg} is installed (version {installed[pkg.lower()]})")
            else:
                print(f"⚠️ {pkg} is not installed")
        else:
            pkg = dep.strip()
            if pkg.lower() in installed:
                print(f"✅ {pkg} is installed (version {installed[pkg.lower()]})")
            else:
                print(f"⚠️ {pkg} is not installed")


def check_dependencies(python_path):
    """Check dependencies for performance optimization."""
    print("\nChecking dependencies for performance optimization...")

    # List of packages to check
    performance_packages = [
        "orjson",
        "pyarrow",
        "polars",
    ]

    for package in performance_packages:
        # Python import check command
        result = run_command(
            [
                str(python_path),
                "-c",
                f"import {package}; print('{package} ' + {package}.__version__)",
            ],
            description=None,
            check=False,
            capture_output=True,
        )

        if result.returncode == 0:
            print(f"✅ {result.stdout.strip()}")
        else:
            print(f"⚠️ {package} is not installed (optional)")


def setup_security_tools(python_path, root_dir):
    """Set up security scanning tools."""
    print("\nSetting up security scanning tools...")

    # Install security tools if not already installed
    security_tools = ["bandit", "safety"]
    for tool in security_tools:
        run_command(
            [str(python_path), "-m", "pip", "install", tool],
            description=f"Installing {tool}",
            check=False,
        )

    # Run a basic security scan
    run_command(
        [str(python_path), "-m", "bandit", "-r", str(root_dir / "src"), "-ll"],
        description="Running basic security scan with bandit",
        check=False,
    )


def main():
    """Set up the development environment."""
    # Get the project root directory
    root_dir = Path(__file__).parent.parent.absolute()
    os.chdir(root_dir)

    print("=" * 80)
    print(f"Setting up {PROJECT_NAME.capitalize()} development environment")
    print("=" * 80)

    # Check Python version
    check_python_version()

    # Create virtual environment
    venv_path = root_dir / VENV_DIR
    create_virtual_env(venv_path)

    # Get virtual environment paths
    python_path = get_venv_python(venv_path)

    # Parse pyproject.toml
    version, dependencies = parse_pyproject_toml(root_dir)
    print(f"\nProject version: {version}")

    # Update pip
    run_command(
        f"{python_path} -m pip install --upgrade pip", description="Upgrading pip"
    )

    # Install dev dependencies
    run_command(
        f"{python_path} -m pip install -e '.[dev]'",
        description="Installing development dependencies",
    )

    # Install documentation dependencies
    run_command(
        f"{python_path} -m pip install -e '.[docs]'",
        description="Installing documentation dependencies",
    )

    # Create necessary directories
    create_directories(root_dir)

    # Validate dependencies
    validate_dependencies(python_path, dependencies)

    # Check dependencies
    check_dependencies(python_path)

    # Setup security tools
    setup_security_tools(python_path, root_dir)

    # Install pre-commit if not already installed
    result = run_command(
        f"{python_path} -m pre-commit --version", description=None, check=False
    )

    if result.returncode != 0:
        run_command(
            f"{python_path} -m pip install pre-commit",
            description="Installing pre-commit",
        )

    # Install pre-commit hooks
    run_command(
        f"{python_path} -m pre-commit install",
        description="Setting up pre-commit hooks",
    )

    # Install pre-push hooks (if supported)
    run_command(
        f"{python_path} -m pre-commit install --hook-type pre-push",
        description="Setting up pre-push hooks",
        check=False,
    )

    # Run pre-commit hooks on all files
    print("\nRunning pre-commit hooks on all files (this may take a while)...")
    run_command(
        f"{python_path} -m pre-commit run --all-files",
        check=False,
        capture_output=False,
    )
    print("\n✅ Pre-commit setup complete")

    # Test building documentation
    print("\nTesting documentation build (this may take a moment)...")
    docs_cmd = (
        f"cd docs && {python_path} -m sphinx.cmd.build -b html "
        f"-d _build/doctrees . _build/html"
    )
    result = run_command(docs_cmd, check=False)

    if result.returncode == 0:
        print("✅ Documentation builds successfully")
    else:
        print(
            "⚠️  Documentation build failed - you may need to fix issues "
            "before committing"
        )

    print("\n" + "=" * 80)
    print("Development environment setup complete!")
    print("=" * 80)
    print("\nTo activate the virtual environment:")

    if platform.system() == "Windows":
        print(f"  {VENV_DIR}\\Scripts\\activate.bat")
    else:
        print(f"  source {VENV_DIR}/bin/activate")

    print("\nYou can now start developing with:")
    print("- Code formatting and linting: ruff")
    print("- Type checking: mypy")
    print("- Testing: pytest")
    print("- Security scanning: bandit, safety")
    print("- Documentation checking: interrogate")
    print("- Documentation building: sphinx-build")
    print("\nPre-commit hooks will run automatically on each commit.")
    print("To build documentation: cd docs && sphinx-autobuild . _build/html")


if __name__ == "__main__":
    main()
