# CLAUDE.md

Operational guide for agents working in this repository.

Precedence:
- Follow this file first.
- Use linked docs for deep details.
- Do not load all docs upfront; read only what the current task needs.

When to read this:
- At task start.
- Before coding, testing, committing, and opening a PR.

## Quick Operating Workflow

1. Inspect:
- Read this file.
- Open only the docs needed for the task.
- Confirm current code and call sites before changing anything.

2. Plan:
- Define smallest safe change set.
- Keep scope tight to the requested outcome.

3. Implement:
- Make minimal, reviewable diffs.
- Preserve current architecture and naming patterns.

4. Verify:
- Run canonical commands for compile/tests.
- Validate changed behavior, not only compilation.

5. Prepare PR:
- Rebase on `origin/main`.
- Ensure commit history contains only task-related commits.

6. Report:
- Summarize what changed, how it was validated, and remaining risks.

## Task Routing

Open these docs by task type:

- Build, test, release workflow: `docs/DEVELOPMENT.md`
- Architecture and runtime design: `docs/ARCHITECTURE.md`
- Query method parsing/planning/execution: `docs/QUERY.md`
- Concurrency model and guarantees: `docs/CONCURRENCY.md`
- Spring integration: `docs/SPRING_DATA.md`

## Hard Rules (Non-Negotiable)

Do:
- Use Java 21 pattern-matching switches for type dispatch paths.
- Keep hot paths allocation-light, primitive-first, and O(1) where expected.
- Use `TypeCodes` static byte constants (not an enum).
- Use `var` for local variable declarations where readable.
- Use imports instead of fully qualified class names in code.
- Use repo-relative paths in tool calls and scripts.
- Keep test output clean and deterministic.
- Keep one primary AssertJ assertion per test unless clearly justified.

Never:
- Add reflection-driven or map/string lookup logic to hot paths.
- Print with `System.out` or `System.err` in committed tests.
- Assume symbols/files/behavior without checking repository evidence.
- Mix unrelated refactors into task-focused commits.
- Open a PR with unrelated commits in branch history.

## Canonical Commands

Use these by default:

```bash
# Compile
mvn.cmd -q -e compile

# All core tests
mvn.cmd -q -e -pl memris-core test

# Single test class
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName

# Single test method
mvn.cmd -q -e -pl memris-core test -Dtest=ClassName#methodName

# Pre-PR history/cleanliness check
git status --short
git log --oneline origin/main..HEAD
git diff --check
```

For extended command variants and release steps, use `docs/DEVELOPMENT.md`.

## Git & PR Hygiene

### Branch creation

```bash
git fetch origin --prune
git checkout main
git pull --ff-only origin main
git checkout -b <type>/<short-topic>
```

### Before push

```bash
git fetch origin --prune
git rebase origin/main
```

### Pre-PR checks (required)

```bash
git status --short
git log --oneline origin/main..HEAD
git diff --check
```

Required state:
- Only intended files are changed.
- Only task commits appear above `origin/main`.
- No whitespace/errors in `git diff --check`.

### If branch history is polluted

```bash
git checkout -b <clean-branch> origin/main
git cherry-pick <intended-commit-sha>
git push -u origin <clean-branch>
```

Then open a new PR from the clean branch and close the polluted PR as superseded.

### PR rules

- Base branch: `main`.
- Head branch: task-specific and clean.
- PR description must include:
  - summary of changes
  - verification commands and outcomes
  - explicit scope boundaries

## Output/Validation Expectations

In final responses:
- List changed files and why they changed.
- List commands run and key outcomes.
- State constraints or steps not executed (if any).
- State residual risks or follow-up work only when relevant.

Validation standard:
- Do not claim completion without local verification.
- If verification is blocked, say exactly what is missing.

## Reference Links

- Development workflow and release process: `docs/DEVELOPMENT.md`
- Architecture deep dive: `docs/ARCHITECTURE.md`
- Query reference: `docs/QUERY.md`
- Concurrency model: `docs/CONCURRENCY.md`
- Spring data/integration notes: `docs/SPRING_DATA.md`
