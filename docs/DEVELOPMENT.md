# Development Guide

Use JDK 21 and the checked-in Gradle wrapper: `./gradlew` on Bash or
`.\gradlew.bat` on Windows PowerShell. No system Gradle installation is needed.
`settings.gradle.kts` lists the five modules, `gradle/libs.versions.toml` holds
dependency/tool versions, and `gradle.properties` holds the project version.

The daemon, build cache, configuration cache, and parallel execution are enabled.
Reuse the same checkout and JDK between commands; keep incremental outputs for
routine development. Use `clean` to diagnose stale outputs or verify a clean build.

## Fast feedback, then regression tests

Start with the smallest observable proof that exercises the behavior being
changed: a JShell probe, a small reproduction, or an existing focused test.
Use real project entry points and check the expected result, including the
relevant boundary case. Iterate with that check while the code is changing.

Once the behavior works, turn the useful cases into durable tests, run the
affected suite, and expand verification when shared contracts are involved.
Test-first development is available when useful; strict RED/GREEN sequencing
is not required. A successful REPL experiment guides the implementation and
does not replace the final regression tests.

## JShell

Prepare the core classes and runtime classpath once:

```sh
./gradlew :memris-core:writeRuntimeClasspath
```

On Bash:

```bash
jshell --class-path "$(cat memris-core/build/runtime-classpath.txt)"
```

On PowerShell:

```powershell
$memrisClasspath = (Get-Content -Raw memris-core/build/runtime-classpath.txt).Trim()
jshell --class-path $memrisClasspath
```

For example, check type resolution and arena initialization against the actual
core classes and dependencies:

```java
import io.memris.core.TypeCodes;
import io.memris.repository.MemrisRepositoryFactory;
if (TypeCodes.forClass(int.class) != TypeCodes.TYPE_INT) throw new AssertionError("int dispatch");
try (var factory = new MemrisRepositoryFactory()) {
    try (var arena = factory.createArena()) {
        if (arena.getArenaId() <= 0) throw new AssertionError("arena initialization");
    }
}
```

Replace the example with the entry point and assertions for the feature. To use
compiled test entities/repositories, run `:memris-core:writeTestRuntimeClasspath`
and open JShell with `memris-core/build/test-runtime-classpath.txt` instead.

For a save/query probe with those existing fixtures:

```java
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
try (var factory = new MemrisRepositoryFactory(); var arena = factory.createArena()) {
    var repo = arena.createRepository(TestEntityRepository.class);
    var saved = repo.save(new TestEntity(null, "Ada", 37));
    if (!repo.findById(saved.id).orElseThrow().name.equals("Ada")) throw new AssertionError("findById");
    if (repo.findByName("Ada").size() != 1) throw new AssertionError("derived query");
}
```

Redefine methods or classes entered as snippets directly in the session. Save
useful snippets with `/save memris-core/build/probe.jsh`. When changing checked-in
Java classes, run `:memris-core:classes` (or `:memris-core:testClasses` for fixtures)
in another terminal, then use `/reload` in JShell to reload classes and replay
the session. This rebuilds the affected source set, not the full project. Rebuild
the classpath file when dependencies change and reopen JShell with that classpath.
Close disposable arenas/factories in each probe so replay starts from fresh state.
See the [JDK 21 JShell reference](https://docs.oracle.com/en/java/javase/21/docs/specs/man/jshell.html).

## Build and test commands

| Purpose | Command |
| --- | --- |
| Compile core only | `./gradlew :memris-core:classes` |
| Compile core tests/fixtures | `./gradlew :memris-core:testClasses` |
| All real-world scenarios and execution modes | `./gradlew realWorldTest` |
| One real-world scenario in one mode | `./gradlew :memris-core:realWorldBytecodeTest --tests 'io.memris.realworld.ECommerceRealWorldTest'` |
| Select one test | `./gradlew :memris-core:test --tests 'io.memris.core.TypeCodesTest.resolvesCommonTypes'` |
| Test core | `./gradlew :memris-core:test` |
| Test a Spring adapter | `./gradlew :memris-spring-data-boot3:test` |
| Test all modules | `./gradlew test` |
| Package all modules and run tests | `./gradlew build` |
| Include static analysis | `./gradlew build -Pquality=true` |
| Generate core coverage | `./gradlew :memris-core:jacocoTestReport` |

Use the matching `boot2` task for Boot 2 changes. Gradle builds project
dependencies as needed without installing artifacts into a local Maven repository.
Core contract changes should be checked against both Spring integrations.
Test results are under each module's `build/test-results/` and `build/reports/tests/`.

Tests use JUnit 5 and AssertJ. Related assertions can share a test when they
describe one business outcome. Public behavior is covered by the centralized
real-world scenarios; isolated tests cover internal edge cases. The scenario map,
execution matrix, and retained unit-test responsibilities are in [TESTING.md](TESTING.md).
Use deterministic data and close test factories/arenas.

Normal `test` runs the real-world matrix before the remaining unit tests. Each
matrix variant has a separate results/report directory under `build/`.
JaCoCo combines the four variants with the unit-test results.

Normal tests exclude the `benchmark` tag. JMH has its own source set and task,
so ordinary test compilation does not compile the benchmarks. JaCoCo XML reports
can be inspected with `python scripts/parse_jacoco.py`.

Optional mutation testing retains the existing scoped targets and thresholds:
`./gradlew :memris-core:pitest --no-configuration-cache`. It runs separately from
the normal build and writes reports under `memris-core/build/reports/pitest/`.

## Runtime boundaries

Storage is heap-backed. Metadata extraction, query compilation, and ByteBuddy
generation happen at runtime initialization. Generated execution paths reuse
primitive columns, `TypeCodes`, and compiled `ColumnAccessPlan` instances.
Scans are linear; index lookups and sorting have different operation costs.
Use the workload being changed to assess performance.

Generated artifact caches belong to an arena. Consult
[ARCHITECTURE.md](ARCHITECTURE.md), [CONCURRENCY.md](CONCURRENCY.md), and
the root `AGENTS.md` for lifecycle and row/index guarantees before changing them.
Spring Boot 2 uses Spring 5, `javax.persistence`, and SLF4J 1.x; Boot 3 uses
Spring 6, `jakarta.persistence`, and SLF4J 2.x. Keep these dependency families separate.

## Concurrency and performance

Run the arena/codegen suite in isolated forks with parallel class execution:

```sh
./gradlew :memris-core:threadSafetyTest
```

Run the embedded-path JMH suite:

```sh
./gradlew :memris-core:jmh
```

Select another benchmark with `-PjmhIncludes=...`. Warmup iterations, measurement
iterations, forks, and the result path are configurable with
`-PjmhWarmupIterations`, `-PjmhIterations`, `-PjmhForks`, and `-PjmhResult`.
The result path is relative to `memris-core/build/` and defaults to
`reports/jmh/embedded.json`.

```sh
python scripts/check-jmh-regression.py --baseline memris-core/src/jmh/resources/embedded-path-baseline.json --current memris-core/build/reports/jmh/embedded.json --flat-threshold 0.10 --embedded-threshold 0.15
```

The concurrency and performance workflows live in `.github/workflows/`.
Record JDK, hardware, workload, warmup, and forks with performance results.
[BENCHMARK_BASELINES.md](BENCHMARK_BASELINES.md) contains historical measurements.

## Documentation and analysis

Install `requirements.txt` in a Python environment once, then run:

```sh
python scripts/check-docs-drift.py
python -m mkdocs build --site-dir build/docs-site
git diff --check
```

The drift check covers known API/version patterns; MkDocs checks the documentation
build. Update [QUERY.md](QUERY.md), [SPRING_DATA.md](SPRING_DATA.md), or
[CONCURRENCY.md](CONCURRENCY.md) when the corresponding public behavior changes.

Add `-Pquality=true` to run Checkstyle, PMD, and SpotBugs with `check`/`build`.
This separates the normal test loop from the broader analysis previously run by
Maven's `verify` phase. PMD reports violations without failing the build, matching
the previous policy. XML rule/filter files live under `config/`.

## Agent guidance and releases

All agent instructions, engine invariants, and the Git/worktree workflow live in
the single root `AGENTS.md`. Keep persistent instructions focused on durable
project facts and easy-to-miss constraints, and consult references according to
the task. Local plans and tool sessions are ignored. Add a shared skill when a
recurring workflow needs additional guidance.
This follows OpenAI's guidance on
[repository instructions](https://learn.chatgpt.com/docs/agent-configuration/agents-md)
and [focused agent context](https://developers.openai.com/blog/rethinking-skills-and-prompts-for-gpt-6-astra).

Use conventional commit subjects describing technical changes without generated
assistant attribution. Include the relevant verification results in PRs.

The manual release workflow is `.github/workflows/release.yml`, dispatched on
`main`. It tests and packages a release version, publishes signed artifacts to
Maven Central through Gradle, then records the release tag and next snapshot.
Central Portal credentials and signing keys are supplied by CI secrets. For local
publication inspection, use `./gradlew publishAllPublicationsToBuildRepository`;
artifacts and generated POMs go to `build/repository/` without publishing remotely.
