# Root Cause Analysis: Python 3.14 CI Failure

## Issue Summary

CI/CD workflows failed when testing with Python 3.14 during PR #11 (v2.0.2 branch).

**Error Message:**
```
ERROR: Package 'transmog' requires a different Python: 3.14.2 not in '<3.14,>=3.10'
```

## Root Cause

The failure occurred due to a mismatch between `pyproject.toml` and `poetry.lock` metadata:

1. **Initial State (commit e925e0e):** 
   - `pyproject.toml` had `requires-python = ">=3.10,<3.14"`
   - `poetry.lock` metadata matched: `python-versions = ">=3.10,<3.14"`

2. **Partial Fix (commit 14ad265):**
   - `pyproject.toml` was updated to `requires-python = ">=3.10"` (removed upper bound)
   - **Problem:** `poetry.lock` was NOT regenerated, still contained `python-versions = ">=3.10,<3.14"`

3. **Result:**
   - pip read the stale metadata from `poetry.lock` during package installation
   - Python 3.14 installations failed because the lock file indicated incompatibility
   - The package metadata advertised support for 3.14 but the lock file rejected it

## Resolution

**Complete Fix (commit 6b8c882):**
- Regenerated `poetry.lock` file using `poetry lock`
- Updated metadata to `python-versions = ">=3.10"`
- Updated content-hash to reflect the change

## Technical Details

The `poetry.lock` file contains a `[metadata]` section that includes:
- `python-versions`: The Python version constraint for the package
- `content-hash`: A hash of the dependencies and configuration

When `pyproject.toml` is modified to change Python version requirements, the lock file MUST be regenerated to update this metadata. Otherwise, package installers (pip, poetry) will use the stale constraint from the lock file.

## Lessons Learned

1. **Always regenerate `poetry.lock` after modifying `pyproject.toml`**
   - Use `poetry lock` to regenerate the lock file
   - This ensures metadata consistency between configuration and lock file

2. **Version constraints must be synchronized:**
   - `pyproject.toml` → defines the requirements
   - `poetry.lock` → caches resolved dependencies and metadata
   - Both must align for correct package installation

3. **CI failures can indicate metadata issues:**
   - If installation fails but source code supports the version, check lock file metadata
   - Compare `requires-python` in `pyproject.toml` with `python-versions` in `poetry.lock`

## Verification

Current state (as of commit 19d7d59):
- ✅ `pyproject.toml`: `requires-python = ">=3.10"`
- ✅ `poetry.lock` metadata: `python-versions = ">=3.10"`
- ✅ Classifier includes: `"Programming Language :: Python :: 3.14"`
- ✅ CI workflows test Python versions: `['3.10', '3.11', '3.12', '3.13', '3.14']`
- ✅ All tests passing on Python 3.14

## Prevention

To prevent this issue in the future:

1. **Git hook or pre-commit check:**
   - Add a check that verifies `poetry.lock` is up-to-date when `pyproject.toml` changes
   - Poetry has built-in checks for this: `poetry check --lock`

2. **Documentation:**
   - Document the requirement to run `poetry lock` after modifying `pyproject.toml`
   - Include this in contributing guidelines

3. **CI verification:**
   - Consider adding a CI step that verifies lock file consistency
   - Run `poetry check --lock` in CI to catch synchronization issues
