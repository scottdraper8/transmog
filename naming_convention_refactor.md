# Transmog Naming Convention Refactoring Plan

## Overview

This plan outlines the steps to completely refactor the naming convention system in Transmog,
**WITHOUT preserving backward compatibility**. The goal is to simplify the naming logic by:

1. **Removing indices/level numbers** from paths
2. **Eliminating all abbreviation logic**
3. **Simply combining field names** with separators
4. **Special handling only for deeply nested structures** (>4 layers)

## Phase 1: Core Naming Logic Refactoring

### Files to Modify

- `src/transmog/naming/conventions.py`
  - [ ] Remove `abbreviate_table_name` functionality
  - [ ] Refactor `get_table_name` to implement simple field combination
  - [ ] Add new function for handling deep nesting (>4 layers)
  - [ ] Simplify `sanitize_name` function
  - [ ] Remove caching if not needed for simplified logic

- `src/transmog/naming/utils.py`
  - [ ] Remove field abbreviation utilities
  - [ ] Simplify path handling utilities
  - [ ] Add utility for handling deeply nested paths

- `src/transmog/naming/__init__.py`
  - [ ] Update exports to remove abbreviation-related functions

### Files to Remove

- `src/transmog/naming/abbreviator.py`
  - [ ] Complete removal (all abbreviation logic is eliminated)

## Phase 2: Core Processing Logic Updates

### Files to Modify

- `src/transmog/core/flattener.py`
  - [ ] Remove index notation from flattened field names
  - [ ] Remove all abbreviation logic
  - [ ] Update field name generation logic
  - [ ] Simplify handling of nested arrays

- `src/transmog/core/extractor.py`
  - [ ] Update array extraction to use simplified naming
  - [ ] Remove index-based path components
  - [ ] Implement special handling for deeply nested arrays

- `src/transmog/core/hierarchy.py`
  - [ ] Update parent-child relationship handling
  - [ ] Modify table naming integration

## Phase 3: Configuration System Updates

### Files to Modify

- `src/transmog/config/__init__.py`
  - [ ] Remove abbreviation-related config options
  - [ ] Add config option for deep nesting threshold (default 4)

- `src/transmog/config/naming.py`
  - [ ] Remove abbreviation options
  - [ ] Simplify naming configuration
  - [ ] Add deep nesting configuration

## Phase 4: Result Handling Updates

### Files to Modify

- `src/transmog/process/result.py`
  - [ ] Update result handling to work with new naming scheme
  - [ ] Ensure metadata fields remain consistent

## Phase 5: Tests and Documentation Updates

### Tests to Update

- `tests/naming/test_naming.py`
  - [x] Remove abbreviation tests
  - [x] Add tests for simple field combination
  - [x] Add tests for deep nesting special case

- `tests/core/test_flattener.py`
  - [x] Update tests to match new naming scheme
  - [x] Remove abbreviation-related tests

- `tests/core/test_extractor.py`
  - [x] Update to test new array extraction naming

- All other tests that rely on naming conventions
  - [x] Update expected values to match new naming scheme

### Documentation Updates

- `README.md`
  - [x] Update examples to show new naming scheme
  - [x] Remove mentions of abbreviation

- `docs/`
  - [x] Update all documentation to reflect new naming system
  - [x] Add prominent warning about backward incompatibility
  - [x] Create migration guide for existing users

## Phase 6: Examples and Integration Updates

### Examples to Update

- `examples/`
  - [x] Update all example code to use new naming scheme
  - [x] Add example specifically showing deep nesting handling

### Additional Updates

- `src/transmog/__init__.py`
  - [x] Update public API exports to remove abbreviation-related functions

## Phase 7: Final Quality Assurance

- [ ] Run comprehensive test suite
- [ ] Perform integration testing with complex nested structures
- [ ] Verify all documentation is consistent
- [ ] Create migration examples

## Important Notes

1. **BREAKING CHANGE**: This refactoring explicitly **does not maintain backward compatibility**
2. All deprecated functionality related to abbreviation should be removed, not marked as deprecated

## Implementation Strategy

1. Begin with core naming logic changes
2. Follow with processing logic updates
3. Update configuration system
4. Update tests and documentation
5. Final integration and quality assurance
