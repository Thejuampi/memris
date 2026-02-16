# Coverage Improvement Plan: memris-core

## 1. Problem Statement
Code coverage in memris-core is low for several classes. Goal: Identify low-coverage classes and write end-to-end tests using entity classes and Memris repositories to increase coverage, following repo rules.

## 2. Success Criteria
- Classes below 60% coverage are raised above threshold (≥80% preferred).
- Tests are integration-style, using entities and repositories.
- Tests are deterministic, clean, and follow AssertJ style.
- Verification: mvn test jacoco:report, parse_jacoco.ps1 output confirms coverage improvement.

## 3. Non-Goals
- No unit-only tests unless required for reachability.
- No refactoring of unrelated code.
- No coverage improvement for unreachable/dead code unless requested.

## 4. Constraints / Invariants
- Preserve hot path constraints: O(1) dispatch, allocation-light, primitive-first.
- No reflection or map/string lookup logic in hot paths.
- Follow concurrency guarantees (docs/CONCURRENCY.md).
- Only intended files changed; clean commit history.

## 5. System Understanding
- Memris repositories: [memris-core/src/main/java/io/memris/core/Repository.java]
- Entity classes: [memris-core/src/main/java/io/memris/core/entity/User.java], [memris-core/src/main/java/io/memris/core/annotation/Entity.java]
- Typical flows: save, find, query, delete via Repository API.
- Coverage evidence: [memris-core/target/site/jacoco/jacoco.xml], [scripts/parse_jacoco.ps1]

## 6. Call-Site Trace Expectations
- End-to-end: Entity → Repository.save/find → EntityManager → StorageEngine
- Example: User entity saved via Repository, queried via Query API.

## 7. Approach (Ordered Steps)
1. Parse coverage report to identify lowest coverage classes.
2. Select 2–3 lowest coverage/high-impact classes for first iteration.
3. Write RED integration tests using entities + repositories.
4. Implement minimal changes for GREEN.
5. Run targeted and full test suites, regenerate coverage.
6. Repeat for next batch until threshold met.

## 8. Edge Cases & Failure Modes
- Unreachable code (excluded by PMD/SpotBugs).
- Hot path constraints violated (allocation, concurrency).
- Tests not deterministic (random, timing-based).
- Entity/repository setup fails (misconfiguration).
- Coverage not improved (test not reaching code).
- Deadlocks/race conditions in concurrency tests.
- Reflection accidentally introduced.
- Test output noisy or non-clean.
- Commit history polluted.
- PR checklist not met.
- Coverage report not regenerated.

## 9. Testing Strategy
- Write integration tests in [memris-core/src/test/java/io/memris/core/].
- Use AssertJ for assertions.
- Prefer end-to-end flows: create entity, save, query, delete.
- Ensure test isolation and cleanup.
- Avoid random/timing-based assertions.

## 10. Verification Commands
- mvn.cmd -q -e compile
- mvn.cmd -q -e -pl memris-core test jacoco:report
- pwsh -NoProfile -ExecutionPolicy Bypass -File scripts/parse_jacoco.ps1 -Top 400

## 11. Risk Notes
- Hot path constraints (dispatch, allocation, concurrency).
- Reflection forbidden in hot paths.
- Unreachable code may remain low coverage.
- Entity/repository setup complexity.

## 12. Execution Checkpoints
- Coverage report parsed, targets selected.
- RED tests written for selected classes.
- GREEN implementation achieved.
- Coverage improvement verified.
- Commit history clean.

---

Evidence pointers and details available in chat and subagent outputs.
