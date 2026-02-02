# eval-tests

Evaluate test quality and coverage to identify weak, redundant, or ineffective tests.

## Task

Analyze the test suite to assess test quality, identify tests that don't provide meaningful coverage,
and ensure tests validate actual project logic rather than third-party behavior.

### Analysis Areas

1. **Test Utility and Logic Coverage**

   - Verify tests are validating actual project logic, not third-party library behavior
   - Identify tests that only exercise external dependencies without testing custom code paths
   - Ensure tests cover meaningful edge cases and error conditions specific to the project
   - Flag tests that validate obvious or trivial behavior

2. **Test Independence and Authenticity**

   - Identify over-mocked tests that don't exercise real code paths
   - Flag tests using patches or mocks to work around actual implementation issues
   - Ensure core functionality is tested with real objects, not mocked substitutes
   - Identify tests that are "programmed to pass" through excessive mocking rather than validating logic

3. **Redundancy and Duplication**

   - Find duplicate or near-duplicate tests that validate the same logic
   - Identify tests that are variations of the same scenario without meaningful differences
   - Flag tests that could be consolidated into parameterized test cases
   - Detect tests that repeat the same assertions across multiple test functions

4. **Test Integrity**

   - Verify tests actually fail when the code they test is broken
   - Identify tests with brittle assertions that pass despite logic errors
   - Flag tests that rely on implementation details rather than behavior
   - Ensure tests validate outputs, not just that code runs without error

### Red Flags to Report

- Tests that only verify third-party library calls succeed (not project-specific logic)
- Excessive mocking that prevents testing real integration between components
- Tests with multiple patches/mocks that obscure what's actually being tested
- Redundant test cases that validate identical scenarios
- Tests that pass regardless of actual implementation (e.g., always-true assertions)
- Tests that work around bugs with patches instead of fixing the underlying code
- Tests for trivial getters/setters or pass-through functions
- Tests that don't fail when the tested code is intentionally broken
- Tests with unclear purpose or assertions that don't validate meaningful behavior

### Deliverables

Provide a report including:

- List of weak or ineffective tests with specific file and test name references
- Tests that are primarily validating third-party library behavior
- Redundant test cases that could be consolidated
- Tests relying on excessive mocking or patches
- Recommendations for test improvements or removal
- Suggestions for additional tests to cover gaps in core functionality
- Impact assessment of proposed test changes

This command will be available in chat with /eval-tests
