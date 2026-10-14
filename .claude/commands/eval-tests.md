Evaluate the test suite through the evidence architecture described in `tests/README.md`.

Read `tests/README.md` fully before beginning. That document defines the
paradigm, the claim-evidence tuple, the evidence channels, and the
protocol rules that govern this suite.

## Task

Analyze the test suite as a **measurement apparatus**, not as a checklist
of code paths exercised. The question is not "do tests exist" but "would
the tests notice if behavior that matters were wrong?"

### 1. Claim-evidence integrity

For each test (or a representative sample per directory), evaluate the
claim-evidence tuple `C = (P, D, R, O, S, V)`:

- **P (Proposition):** Does the test name a specific behavioral claim, or
  is it vague ("test_flatten_works", "test_basic_output")? Tests without
  a clear **P** cannot be evaluated for strength.
- **O (Oracle):** Does the assertion distinguish correct from incorrect
  output? Flag assertions that almost any implementation would satisfy:
  `isinstance(result, dict)`, `len(result) > 0`, `assert result`,
  `assert paths`. These are the weak oracles the framework rejects.
- **R (Reality):** If the claim concerns a real dependency (Parquet
  encoding, Avro schema, CSV quoting), is that dependency in the
  execution? A mocked writer cannot support an unqualified format claim.
- **D (Domain):** Is the exercised input domain adequate for the claim?
  A single hand-picked example cannot support "works for all configs."
  Check whether the corpus or generators cover the relevant space.

### 2. Observation-verdict separation

- Are observations (what the code did) kept separate from verdicts (whether
  that behavior is correct/intended)?
- Flag tests that silently promote actuality to specification — e.g.,
  golden values updated without a reviewed rationale.
- Flag tests where a characterization snapshot is treated as normative
  truth without any external justification.

### 3. Evidence proportionality

- Does the scope of each test's conclusion match the scope of its evidence?
- Flag tests that claim end-to-end guarantees from mocked or isolated
  execution.
- Flag tests that claim format fidelity from in-memory-only paths.
- Also flag the reverse error: dismissing a strong local test (e.g., a
  deterministic unit test of a pure function) for not being end-to-end.

### 4. Sensitivity

- Would the tests care if the relevant behavior were wrong? This is the
  second-order question.
- Generate fresh evidence: `uv run python scripts/audit_mutations.py --output /tmp/transmog-mutations.json`.
  Inspect provenance and individual failures, not a stale report or aggregate
  mutation score.
- Invalid/unapplied mutants, collection failures, and runner errors are not
  killed mutants. Investigate survivors without inventing classifications.
- Flag sensors whose assertions a trivial mutation would survive (e.g.,
  swapping SMART and SEPARATE would not change the assertion).
- Flag untested code paths where mutation has not been run and no oracle
  exists.

### 5. Corpus integrity

- Run `uv run python scripts/audit_corpus.py` and report missing categories.
  Category presence is an inventory, not proof of exercised behavior.
- Run `uv run pytest tests/ --audit-corpus` to validate actual fixture loads
  by executed claim-marked sensors. Do not infer usage from filename references.
- Flag any fixture that appears shaped to avoid a code bug rather than
  to exercise the code. If the code fails on a legitimate input, the
  fix belongs in the code, not the fixture.
- Check that every corpus fixture is loaded by an executed
  `@pytest.mark.claim` sensor. Unexercised fixtures need a meaningful sensor
  or removal, not a dummy test.

### 6. Scope boundary health

- Review the "Not claimed" section in `tests/README.md`. Are there
  entries that now have sensors and should be removed?
- Are there observable gaps not listed? If the evaluation discovers
  an unobserved material behavior, add it.
- Is any behavior silently treated as covered when it has no sensor
  and is not listed as unclaimed?

### 7. Evidence channel balance

- Is the suite over-reliant on a single channel (e.g., all
  characterization, no differential or metamorphic)?
- Are the channels complementary? Characterization pins exact tables;
  differential compares paths; metamorphic asserts relations; property-
  based explores generated inputs; mutation challenges oracles.
- Flag areas where a channel is missing and would add genuine evidence
  (not ceremony).

### 8. Redundancy (evidentiary, not textual)

Two tests that look similar can carry distinct fault-detection value.
Two syntactically different tests can be behaviorally redundant.
Redundancy is defined by independent evidence contribution, not by
textual similarity.

- Flag tests that are genuinely redundant: same input, same path, same
  oracle, same sensitivity. Neither adds evidence the other lacks.
- Do **not** flag tests as redundant merely because they test the same
  function. Different inputs, configs, or oracles justify separate tests.

### 9. Anti-patterns

Flag these specific patterns with file and test name references:

- `assert result` / `assert paths` / `assert len(x) > 0` as the
  primary oracle on a behavioral claim (weak oracle).
- Mocks that substitute away the dependency material to the claim
  (circular evidence).
- Tests that infer behavior from source, then assert the inference
  (self-confirming analysis — protocol rule 1).
- Fixtures or expected values that appear designed to avoid triggering
  code defects rather than exposing them (corpus loophole).
- Tests named for a behavioral property that only check execution
  succeeds (e.g., "test_cross_format_consistency" that asserts
  `len(paths) >= 1`).
- Golden values updated without a reviewed compatibility rationale.
- First-sample inference or coercion that silently truncates/discards values.
- Capability filtering that silently drops declared dependency/codec cases.
- Generators that allow structural properties to pass with no relevant rows.
- Multipart claims exercised only with single-part inputs.
- A parentage oracle that accepts the wrong existing parent.
- Normalizing decoded values so broadly that real format differences disappear.

## Deliverables

Provide a structured report:

1. **Claim-evidence audit:** Tests with missing or weak P, O, R, or D,
   grouped by directory.
2. **Sensitivity gaps:** Areas where mutations would likely survive,
   referencing the freshly generated, provenance-linked observations.
3. **Corpus health:** Output of `scripts/audit_corpus.py` plus any
   loopholes or missing type coverage.
4. **Scope boundary review:** Unclaimed guarantees, genuinely missing evidence,
   and known defects requiring regression tests and source fixes. Do not add
   placeholder skipped tests or move exercised defects out of scope.
5. **Channel balance:** Which channels are strong, which are thin,
   where adding evidence would have the highest return.
6. **Redundancy:** Genuinely duplicated evidence (not textual similarity).
7. **Anti-patterns:** Specific instances with file:line references.
8. **Recommendations:** Ordered by expected evidence return, not by
   ceremony. Each recommendation should name the claim it would
   strengthen and the channel it would add to.
