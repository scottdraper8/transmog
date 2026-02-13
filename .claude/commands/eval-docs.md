Evaluate documentation quality and alignment with the codebase to ensure comprehensive, accurate, and user-focused guidance.

## Task

Analyze the documentation in the `docs/` folder to assess quality, accuracy, completeness, and usability.
Identify gaps, inconsistencies, redundancies, and opportunities for improvement.

### Analysis Areas

1. **Feature and Option Coverage**

   - Verify all public API functions are documented (`flatten`, `flatten_stream`, `FlattenResult`)
   - Verify all configuration parameters are documented (`TransmogConfig` fields)
   - Verify all array modes are documented and explained (`SMART`, `SEPARATE`, `INLINE`, `SKIP`)
   - Verify all ID generation strategies are documented (`random`, `natural`, `hash`, composite keys)
   - Verify all output formats are documented (`csv`, `parquet`, `orc`)
   - Verify all supported input file formats are documented (`.json`, `.jsonl`, `.json5`, `.hjson`)
   - Verify all error types are documented (`TransmogError`, `ValidationError`, `MissingDependencyError`)
   - Flag any features/options in code that lack documentation
   - Flag any documented features/options that don't exist in code

2. **Code-Documentation Alignment**

   - Verify examples in docs actually work with current API
   - Verify parameter defaults in docs match code defaults
   - Verify behavior descriptions match actual implementation
   - Verify error handling documentation matches actual error behavior
   - Flag outdated or incorrect examples
   - Flag parameter descriptions that don't match implementation
   - Flag missing edge case documentation

3. **User-Focused Content**

   - Verify docs explain "how to use" rather than "how it works internally"
   - Flag overly technical or implementation-focused explanations
   - Verify common use cases are covered with clear examples
   - Verify docs answer "why would I use this?" for each feature
   - Flag missing practical guidance or best practices
   - Verify configuration recommendations are user-centric

4. **Accessibility and Navigation**

   - Verify all pages are linked from index or navigation
   - Verify page titles are clear and descriptive
   - Verify table of contents is complete and accurate
   - Flag orphaned or unreferenced pages
   - Flag broken internal links
   - Verify code examples are properly formatted and readable
   - Verify all code blocks have proper syntax highlighting

5. **Redundancy and Organization**

   - Identify duplicate content across pages
   - Identify overlapping explanations of the same concept
   - Flag content that could be consolidated
   - Assess whether page organization makes sense
   - Suggest reorganization if needed for better flow
   - Identify content that belongs in multiple places

6. **Completeness and Clarity**

   - Verify all parameters have clear descriptions
   - Verify all options have examples
   - Verify error conditions are documented
   - Verify edge cases are addressed
   - Flag vague or ambiguous explanations
   - Flag missing context or prerequisites
   - Verify docs are comprehensive but not overwhelming

### Red Flags to Report

- Documented features that don't exist in code
- Code features that lack documentation
- Incorrect parameter defaults or types
- Examples that don't match current API
- Broken or missing links
- Orphaned documentation pages
- Duplicate or redundant content
- Overly technical explanations (implementation details vs. usage)
- Missing practical examples or use cases
- Inconsistent terminology or naming
- Unclear or ambiguous descriptions
- Missing error handling documentation
- Incomplete configuration parameter documentation
- Outdated version numbers or references

### Deliverables

Provide a report including:

- List of missing documentation with specific features/options
- List of inaccurate or outdated documentation with examples
- Redundant content that could be consolidated
- Suggested reorganization or restructuring
- Missing practical examples or use cases
- Accessibility issues (broken links, orphaned pages)
- Recommendations for improving clarity and user focus
- Impact assessment of proposed changes (breaking changes to docs structure, migration path, etc.)
