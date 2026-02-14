---
name: audit
description: >-
  Review, plan from, and update the ROADMAP.md roadmap.
  Use when the user wants to check audit status, pick the
  next item to work on, formulate an implementation plan
  from an audit item, or mark items as done.
user-invocable: true
argument-hint: [action] [item-id]
allowed-tools: Read, Edit, Grep, Glob, Task, EnterPlanMode
---

# Audit & Roadmap Skill

You manage the project's living audit document at
`ROADMAP.md` in the repository root.

## How this document was built

The audit was produced through iterative codebase
exploration — source architecture analysis, test quality
evaluation, documentation review, writer implementation
review, and performance profiling. Findings were discussed
with the user, refined with their feedback, and
consolidated into `ROADMAP.md`. It is not a static report.
It is a working roadmap: items get picked off, planned,
implemented, and marked done.

## How the audit is used going forward

The workflow is cyclical:

1. **Review** — Read ROADMAP.md, assess what's next based
   on priority, size, dependencies, and current status.
2. **Plan** — Pick an item (or cluster of related items),
   enter plan mode, explore the relevant code, and write a
   concrete implementation plan.
3. **Implement** — Execute the approved plan.
4. **Update** — Mark the item DONE in ROADMAP.md with a
   one-line summary of what was delivered. Note any new
   issues discovered during implementation as new items.
5. **Repeat** — Return to step 1.

## Actions

Interpret the user's arguments to determine which action
to take:

### `review` (default if no arguments)

Read ROADMAP.md. Summarize:

- How many items total, how many DONE, how many
  TODO/IN_PROGRESS
- What's unblocked and ready to work on next
- Suggest 1-3 items as candidates for the next plan,
  explaining why

### `plan <item-id>`

Read ROADMAP.md and find the specified item. Then enter
plan mode to formulate an implementation plan for it.
The plan should:

- Identify all files that need to change
- Describe each change concretely
- List tests to add or update
- Note dependencies on other audit items
- Define verification steps

### `status`

Read ROADMAP.md and produce a compact status report:

- Items by status (DONE / IN_PROGRESS / TODO)
- Any blocked items and what's blocking them
- Overall progress as a fraction

### `update <item-id> <new-status>`

Update the status of an item in ROADMAP.md. Valid
statuses: TODO, IN_PROGRESS, DONE. When marking DONE,
ask the user for a one-line summary of what was delivered
and add it as a progress note.

### `add`

The user wants to add a new item. Ask for: title,
description, proposed fix, size estimate, dependencies.
Insert it into the appropriate section of ROADMAP.md with
the next available ID in that section's sequence.

## Formatting rules

When editing ROADMAP.md:

- Preserve the existing document structure and formatting
  conventions
- Status values are always: `TODO`, `IN_PROGRESS`, `DONE`
- Size values are always: `S`, `M`, `L`, `XL`
- Keep descriptions concise — the audit is a reference,
  not a narrative
- When marking DONE, add a single line under Progress
  noting what was done
- Never add dates or time estimates
