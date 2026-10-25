# Working in Memris

Memris is a Java 21, heap-based in-memory storage engine. ByteBuddy generates
tables, repositories, and accessors during runtime initialization.

## Repository map

| Location | Responsibility |
| --- | --- |
| `memris-core/` | Storage, indexes, queries, runtime codegen |
| `memris-spring-data-boot2/`, `memris-spring-boot-starter-2/` | Spring Boot 2 / Spring 5 (`javax.persistence`, SLF4J 1.x) |
| `memris-spring-data-boot3/`, `memris-spring-boot-starter-3/` | Spring Boot 3 / Spring 6 (`jakarta.persistence`, SLF4J 2.x) |
| `docs/`, `scripts/`, `.github/workflows/` | References, verification helpers, CI |

Keep the Spring dependency generations separate. Shared core contract changes
may need verification in both integrations. Build configuration lives in Gradle;
dependency versions are in `gradle/libs.versions.toml`, the project version in
`gradle.properties`.

## Fast, reliable feedback

Use the shortest reliable check of the requested behavior: JShell against real
project classes, a small reproduction, or an existing focused test. Iterate with
that check, then add/update durable regression tests and run the affected suite
before handing off behavior changes. Test-first ordering is optional.

Compile only the module/source set needed for the next check. Keep the Gradle
daemon and incremental outputs warm; use `clean` when investigating stale outputs.
Documentation edits need documentation checks rather than a Java test run.

## Core runtime invariants

Apply these constraints when changing `memris-core/`:

- Storage uses heap-backed columns. Preserve primitive column and row-id APIs
  on execution paths; entity-facing types may be boxed or converted.
- Keep per-row reads, writes, and query dispatch free of reflective field access
  and repeated property-name resolution. Reuse compiled column/access plans.
- `TypeCodes` uses static `byte` constants consumed by generated code and typed
  storage dispatch. Extend the matching readers, writers, and tests for new types.
- Generated executors, savers, and materializers are arena-owned. Preserve cache
  isolation and cleanup on close; avoid static mutable runtime caches.
- Preserve terminal, fail-fast arena close semantics and concurrent repository
  creation. Respect the row seqlock, generation, tombstone, and publication
  protocols. Row/index updates do not provide transaction or snapshot isolation;
  consult `docs/CONCURRENCY.md` when changing these behaviors.
- Complexity depends on the operation. Keep per-row allocation low and measure
  performance changes with the relevant JMH benchmark.
- Tests live in `src/test/java`; JMH is a separate source set in `src/jmh/java`.
  Normal tests exclude the `benchmark` tag. Test factories/arenas must be isolated
  and closed after use.
- Use `:memris-core:threadSafetyTest` for arena/codegen concurrency changes.
  Embedded access performance uses `EmbeddedPathBenchmark` and
  `scripts/check-jmh-regression.py`; extended commands are in `docs/DEVELOPMENT.md`.

## Git workflow and workspace hygiene

- Keep the workspace orderly: task-only tracked changes, scratch files and build
  outputs in ignored locations, and a clean working tree after committing.
  Preserve unrelated user work and warm build caches.
- Before a new change, fetch `origin --prune` and synchronize local `main` with
  `git pull --ff-only origin main` when it is safe to switch. Create a
  `codex/<topic>` feature branch from the latest `origin/main`; this repository's
  integration branch is `main`.
- Use a worktree when parallel tasks, an occupied checkout, or independent
  validation benefit from isolation. Reuse a suitable existing checkout/worktree
  for a single task; avoid creating disposable checkouts without a purpose.
- For each completed, independently reviewable repository change, commit it,
  push the feature branch, and open or update its PR to `main` before handoff.
  Report the PR URL and relevant verification results. Keep follow-up commits
  for the same unmerged change on that PR.
- After confirming a PR is merged, fetch/prune, return to local `main`, and pull
  with `--ff-only`. Remove the task's merged local branch and disposable worktree
  once they are clean and no longer needed. Leave local branches synchronized
  with their remote counterparts.

## Commands

Run from the root with JDK 21. Use `./gradlew` on Bash, `.\gradlew.bat` on Windows
PowerShell. The wrapper pins Gradle; daemon and caches are configured in
`gradle.properties`.

```sh
./gradlew :memris-core:classes
./gradlew :memris-core:writeRuntimeClasspath
./gradlew :memris-core:test --tests 'io.memris.core.TypeCodesTest'
./gradlew test
python scripts/check-docs-drift.py
```

The selected test is an example; substitute the class or `ClassName.methodName`
for the behavior being changed. JShell setup, broader checks, and benchmarks are
in `docs/DEVELOPMENT.md`.

## Context by task

| Task | Reference to consult when needed |
| --- | --- |
| Build, test, JShell, or releases | `docs/DEVELOPMENT.md`, relevant Gradle file/workflow |
| Storage/runtime boundaries | `docs/ARCHITECTURE.md` |
| Query semantics or method parsing | `docs/QUERY.md` |
| Row publication, concurrent indexes, arena lifecycle | `docs/CONCURRENCY.md` |
| Spring adapters/auto-configuration | `docs/SPRING_DATA.md` |

Historical baselines and `docs/ROADMAP.md` describe measurements or intentions;
check the implementation and tests before treating them as current behavior.
Keep durable agent guidance in this single root `AGENTS.md` file. Local plans
and permission settings stay out of Git.
