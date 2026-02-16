# E2E Coverage Plan — memris-core

## 1) Problem statement
Increase end-to-end code coverage for `memris-core` by adding realistic integration tests that exercise repository code paths using real entity classes and repository instances.

## 2) Success criteria (hard gates)
- Add at least one end-to-end test suite that uses entities from `docs/examples/ecommerce-domain.md` and repository factories to persist/query data in `memris-core` test runtime.
- Coverage for `memris-core` overall increases measurably; targeted: raise lowest-coverage classes by at least 15 percentage points each (report exact numbers after baseline).
- Tests run deterministically with existing Maven test commands: `mvn -q -e -pl memris-core test` and produce jacoco report.

## 3) Non-goals
- Not refactoring production APIs or repo architecture.
- Not adding mocks/stubs that bypass production repo code paths.

## 4) Constraints / invariants (from repo rules)
- Follow project test style (JUnit + AssertJ as used in repo).
- Use real production entrypoints (Memris repository factories, arenas).
- Keep diffs minimal and interface-stable.

## 5) System understanding (modules/components)
- `memris-core` is the core library containing entities, repository interfaces and repository factory/arena implementations.
- Tests should instantiate `MemrisRepositoryFactory` / `MemrisArena` and create repository interfaces to exercise storage and query code.
- Relevant locations: `memris-core/src/main/java` and tests under `memris-core/src/test/java`.

## 6) Call-site trace expectations
- Tests will call repository factory -> create arena -> create repository -> save entities -> run queries -> assert results.
- Evidence pointers to read during Plan Mode: `pom.xml`, `CLAUDE.md`, `docs/DEVELOPMENT.md`, `memris-core/pom.xml`, `memris-core/src/main/java`.

## 7) Approach (ordered, verifiable steps)
1. Gather baseline coverage for `memris-core` (jacoco files already present under `memris-core/target/site` or run `mvn -q -e -pl memris-core test jacoco:report`).
2. Identify lowest-coverage classes in `memris-core` (top N or bottom N). Persist baseline snapshot.
3. Design 1–2 end-to-end test classes using the `ecommerce` domain from `docs/examples/ecommerce-domain.md`:
   - `EcommerceEntitiesTest` (persistence + basic queries)
   - `EcommerceRelationsTest` (relationships traversal + composite index/queries)
4. Write RED tests that fail because features not yet covered by tests (they should compile but fail coverage assertions if added).
5. Implement minimal test helper wiring (factory creation, in-memory tables) if needed.
6. Run targeted tests and iteratively add tests to exercise uncovered code paths until green.
7. Re-generate jacoco report and produce before/after delta for targeted classes.

## 8) Edge cases & failure modes (≥10)
- Test non-determinism due to timestamps — use fixed timestamps or tolerant assertions.
- DB/table initialization differences across profiles — ensure test uses same default test config.
- Concurrent write/read timing — keep tests single-threaded.
- Missing index definitions causing queries to fallback to scan paths — tests should still work functionally.
- Entity mapping errors (missing annotations) — ensure entities match expectations.
- Resource cleanup not performed — use try-with-resources or arena.close() in @AfterEach.
- Test data collisions between tests — use per-test arenas or clear stores.
- Relying on generated IDs — assert presence rather than exact values.
- Large object graphs causing slow tests — keep small graphs.
- Platform line-ending/case sensitivity (Windows) — avoid path-dependent assertions.

## 9) Testing strategy
- Create tests that instantiate repository factory and real repositories, call `save`, `findById`, `findBy...` and relationship traversal.
- Prefer real storage backends used by `memris-core` tests (in-memory or file-backed default used by repo). Reuse existing test utilities where present.
- Keep tests deterministic and quiet.

## 10) Verification commands
- Targeted: `mvn -q -e -pl memris-core test -Dtest=EcommerceEntitiesTest#*`
- Full module: `mvn -q -e -pl memris-core test jacoco:report`
- Parse jacoco summary: `scripts/parse_jacoco.ps1 -Top 400` (already used in repo)

## 11) Risk notes
- If some low-coverage classes are unreachable via public APIs, they will be marked UNKNOWN and excluded.
- Running full test suite may be slow; iterate with narrow test selection.

## 12) Execution checkpoints
- Checkpoint 1 (exploration): collected evidence of low-coverage classes and test commands.
- Checkpoint 2 (RED): added failing test class scaffolds.
- Checkpoint 3 (GREEN): tests pass and coverage increased; report baseline vs after.

---

References to inspect during Plan Mode:
- `docs/examples/ecommerce-domain.md` (entity designs to implement in tests)
- `memris-core/pom.xml` and `memris-core/src/main/java`
- `docs/DEVELOPMENT.md` for test commands


