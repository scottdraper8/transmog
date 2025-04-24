# Release Process

This document outlines the process for creating and publishing new releases of Transmogrify.

## Version Numbering

Transmogrify follows [Semantic Versioning](https://semver.org/) (SemVer):

- **MAJOR** version for incompatible API changes
- **MINOR** version for new functionality in a backward-compatible manner
- **PATCH** version for backward-compatible bug fixes

Example: `1.2.3` represents Major version 1, Minor version 2, Patch version 3.

## Release Checklist

Before releasing a new version, complete the following checklist:

1. **Run all tests:**
   ```bash
   pytest
   ```

2. **Check code quality:**
   ```bash
   black --check src tests
   isort --check src tests
   flake8 src tests
   ```

3. **Update documentation:**
   - Update relevant documentation
   - Verify example code works with the new version
   - Update version numbers in documentation

4. **Update CHANGELOG.md:**
   - Add a section for the upcoming version
   - Categorize changes under: Added, Changed, Deprecated, Removed, Fixed, Security
   - Include PR numbers and credit contributors

5. **Update version number:**
   - Update version in `src/transmogrify/__init__.py`
   - Update version in `pyproject.toml`

## Creating a Release

Once the checklist is complete, follow these steps to create a release:

### 1. Create and Push a Release Branch

```bash
git checkout -b release-X.Y.Z
git add .
git commit -m "Prepare release X.Y.Z"
git push origin release-X.Y.Z
```

### 2. Create a Pull Request

Create a pull request from the release branch to the main branch and ensure:
- All CI checks pass
- The PR is reviewed and approved by at least one maintainer

### 3. Merge the Release PR

Once approved, merge the release PR into the main branch.

### 4. Create a Git Tag

After the PR is merged:

```bash
git checkout main
git pull
git tag -a vX.Y.Z -m "Release X.Y.Z"
git push origin vX.Y.Z
```

### 5. Build and Publish to PyPI

Build the package distributions:

```bash
python -m pip install build twine
python -m build
```

Upload to Test PyPI first (optional but recommended):

```bash
python -m twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

After verifying the test package, upload to PyPI:

```bash
python -m twine upload dist/*
```

### 6. Create a GitHub Release

1. Go to the [GitHub releases page](https://github.com/username/transmogrify/releases)
2. Click "Draft a new release"
3. Select the tag you created
4. Title the release "Transmogrify X.Y.Z"
5. Copy the relevant section from CHANGELOG.md as the description
6. Attach the built distributions
7. Publish the release

## Post-Release Tasks

After the release is published:

1. **Update development version:**
   - Increment version number in `src/transmogrify/__init__.py` with `.dev0` suffix
   - Example: After releasing `1.2.0`, update to `1.3.0.dev0`

2. **Announce the release:**
   - Post announcement on relevant channels
   - Highlight key features and improvements

3. **Monitor for issues:**
   - Watch for new issues related to the release
   - Prepare hot-fixes if critical issues arise

## Hot-Fix Releases

For critical bugs that need immediate fixing:

1. Create a hot-fix branch from the tagged release:
   ```bash
   git checkout vX.Y.Z
   git checkout -b hotfix-X.Y.(Z+1)
   ```

2. Fix the issue and commit the changes.

3. Follow the regular release process, incrementing only the patch version (Z+1).

## Release Schedule

- **Minor releases** are planned quarterly
- **Patch releases** are published as needed for bug fixes
- **Major releases** are scheduled when significant API changes are required

## Release Manager

For each release, a release manager:
- Coordinates the release process
- Ensures completion of all checklist items
- Handles any issues during the release
- Updates stakeholders on progress

The release manager should be familiar with the codebase and the release process outlined in this document. 