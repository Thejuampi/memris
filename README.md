# Memris

[![CI](https://github.com/Thejuampi/memris/actions/workflows/ci.yml/badge.svg)](https://github.com/Thejuampi/memris/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.thejuampi/memris.svg?label=maven%20central)](https://central.sonatype.com/artifact/io.github.thejuampi/memris)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Memris is a Java 21 in-memory storage engine with heap-backed columns and
runtime-generated repositories. It provides typed storage access, indexes, and
Spring Data-style query methods for application-owned working sets.

ByteBuddy generates tables, repositories, savers, and materializers during runtime
initialization. Compiled access plans handle embedded paths; storage and query
execution reuse column indices and typed readers instead of reflective field
access on each row.

## Current status

| Version | Status |
| --- | --- |
| `0.3.0` | Latest published version of all five artifacts on [Maven Central](https://repo.maven.apache.org/maven2/io/github/thejuampi/memris/maven-metadata.xml) |
| `0.3.1-SNAPSHOT` | Current repository version in [gradle.properties](gradle.properties) |

The feature descriptions below refer to **current `main`**. The
[0.3.0 release notes](docs/releases/0.3.0.md) describe the numeric-boundary,
query-contract, Spring startup, packaging, and test-suite changes since the
previous release. See the [roadmap](docs/ROADMAP.md) for the baseline and next steps.

## Implemented on main

| Area | Available behavior |
| --- | --- |
| Storage and code generation | Heap-backed paged columns; bytecode and method-handle table implementations; generated savers/materializers; compiled embedded access plans |
| Repositories | Explicit `@Id`, generated numeric IDs for new entities, CRUD and bulk operations, derived find/count/exists methods |
| Queries | Comparisons, inclusive ranges, IN/NOT_IN, null and boolean predicates, case-insensitive equality/patterns, `AllIgnoreCase`, `removeBy`, AND/OR, ordering, first/top, and distinct modifiers |
| Annotated queries | A JPQL-like subset with named/positional parameters, joins, record projections, grouping/count/HAVING, and modifying queries |
| Indexes | Hash, ordered range (`BTREE`), prefix, suffix, and composite hash/range indexes |
| Mapping | Primitive/boxed scalar fields, strings, time and big-number types, registered converters, embedded graphs, and eager relationship hydration including many-to-many |
| Arenas | Separate data/repository lifetimes, arena-owned generated-artifact caches, concurrent repository creation, and terminal arena close |
| Spring | Separate Boot 2/Spring 5 and Boot 3/Spring 6 integrations, repository discovery/injection, named arenas, and JPA attribute converter beans |
| Verification | Real-world repository scenarios across four table/codegen combinations, internal edge-case tests, concurrency checks, static analysis, and JMH guardrails |

The current implementation has the following boundaries:

- Data lives in the current process. Persistence, recovery, transactions, and
  snapshot isolation are not implemented.
- Row writes and reads use seqlock/publication protocols. Index changes and row
  changes do not form an atomic transaction, and queries do not take a consistent
  snapshot across rows. See [CONCURRENCY.md](docs/CONCURRENCY.md).
- The default metadata provider uses Memris annotations, including
  `io.memris.core.Id` and relationship annotations. Spring's JPA converter bridge
  does not normalize JPA entity mappings. See [SPRING_DATA.md](docs/SPRING_DATA.md).
- Relationships load eagerly. The annotated query language supports a subset of
  JPQL; general SQL, subqueries, and aggregates beyond count are outside it.

### Physical storage

Logical Java types share three column implementations:

| Column | Logical values |
| --- | --- |
| `PageColumnInt` / `int[]` | `int`, `boolean`, `byte`, `short`, `char`, and sortable-encoded `float` |
| `PageColumnLong` / `long[]` | `long`, sortable-encoded `double`, and epoch-based `Instant`, `LocalDate`, `LocalDateTime`, and `java.util.Date` |
| `PageColumnString` / `String[]` | Strings and encoded `BigDecimal`/`BigInteger`; registered converters also map types such as UUID and LocalTime onto supported storage types |

Boxed/reference fields have presence information for nulls. Big-number query
support is limited to equality, inequality, and membership. The implementation
mapping is in [TableGenerator](memris-core/src/main/java/io/memris/storage/heap/TableGenerator.java).

## Quick start

Use Java 21. The published core artifact is:

```xml
<dependency>
  <groupId>io.github.thejuampi</groupId>
  <artifactId>memris</artifactId>
  <version>0.3.0</version>
</dependency>
```

Gradle Kotlin:

```kotlin
implementation("io.github.thejuampi:memris:0.3.0")
```

Gradle Groovy:

```groovy
implementation 'io.github.thejuampi:memris:0.3.0'
```

The following minimal core example works with the published artifact and current
`main`. Save the three classes in separate files.

`User.java`:

```java
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;

@Entity
public class User {
    @Id
    public Long id;

    @Index(type = Index.IndexType.HASH)
    public String email;
    public String name;
    public int age;

    public User() {}

    public User(String email, String name, int age) {
        this.email = email;
        this.name = name;
        this.age = age;
    }
}
```

`UserRepository.java`:

```java
import io.memris.repository.MemrisRepository;
import java.util.List;
import java.util.Optional;

public interface UserRepository extends MemrisRepository<User> {
    User save(User user);
    Optional<User> findById(Long id);
    List<User> findByAgeBetween(int min, int max);
    List<User> findByNameContainingIgnoreCase(String name);
    long countByAgeGreaterThan(int age);
    boolean existsByEmail(String email);
}
```

`MemrisRepository<T>` is a marker interface: declare the operations your repository
needs. The factory generates their implementation.

`Main.java`:

```java
import io.memris.repository.MemrisRepositoryFactory;

public class Main {
    public static void main(String[] args) {
        try (var factory = new MemrisRepositoryFactory();
             var arena = factory.createArena()) {
            var users = arena.createRepository(UserRepository.class);
            var alice = users.save(new User("alice@example.com", "Alice", 30));

            System.out.println(users.findById(alice.id).orElseThrow().name); // Alice
            System.out.println(users.findByAgeBetween(18, 65).size());       // 1
            System.out.println(users.findByNameContainingIgnoreCase("ALI").size()); // 1
            System.out.println(users.countByAgeGreaterThan(25));            // 1
            System.out.println(users.existsByEmail("alice@example.com"));  // true
        }
    }
}
```

Closing the arena releases its data and caches. An `@Index` does not impose
uniqueness: duplicate emails can belong to different IDs. An `Optional<T>` query
returns the first matching result, or empty; use ordering when the choice among
matches matters. The [query reference](docs/QUERY.md#nulls-duplicate-values-and-single-results)
describes null, negation, and cardinality contracts.

## Spring modules

All artifacts use group `io.github.thejuampi`.

| Application | Integration artifact | Starter artifact | Persistence converter API |
| --- | --- | --- | --- |
| Boot 2 / Spring 5 | `memris-spring-data-boot2` | `memris-spring-boot-starter-2` | `javax.persistence` |
| Boot 3 / Spring 6 | `memris-spring-data-boot3` | `memris-spring-boot-starter-3` | `jakarta.persistence` |

Choose one generation and keep its dependencies separate. Spring repositories
extend `MemrisSpringRepository<T, ID>` and use `@EnableMemrisRepositories`.
The [Spring reference](docs/SPRING_DATA.md) describes the metadata boundary;
the [inventory scenarios](memris-spring-data-boot3/src/test/java/io/memris/realworld/SpringInventoryRealWorldTest.java)
show the current repository/converter workflow. The equivalent Boot 2 scenario
lives in its own module. [Independent consumers](verification/consumers/README.md)
verify all three packaged dependency graphs in CI and before publication.

## Build and verify

Use JDK 21 and the checked-in Gradle wrapper:

```sh
./gradlew build
./gradlew realWorldTest
./gradlew :memris-core:threadSafetyTest
python scripts/check-docs-drift.py
```

On Windows PowerShell, use `.\gradlew.bat` in place of `./gradlew`.
`test` and `build` include the real-world matrix: `BYTECODE` and `METHOD_HANDLE`
tables, each with runtime executor codegen enabled and disabled. Disabling that
codegen option does not disable all ByteBuddy generation.

Public behavior is centralized in `io.memris.realworld`; unit tests isolate
internal edge cases. See [TESTING.md](docs/TESTING.md) for the scenario map,
individual task names, reports, and coverage aggregation.

To inspect artifacts in a local build repository:

```sh
./gradlew publishAllPublicationsToBuildRepository
```

Artifacts go to `build/repository/`. Development commands, configuration, JShell,
quality checks, and the release workflow are in [DEVELOPMENT.md](docs/DEVELOPMENT.md).

### Performance

Scans are linear in the rows examined. Index lookup cost also includes collecting
matching rows and materializing results; a range query is not constant-cost.
Use workload measurements rather than assuming a throughput multiplier.

JMH lives in `memris-core/src/jmh/java`. The
[performance workflow](.github/workflows/perf.yml) checks `EmbeddedPathBenchmark`
against the recorded [baseline](memris-core/src/jmh/resources/embedded-path-baseline.json)
using [check-jmh-regression.py](scripts/check-jmh-regression.py).
The separate [concurrency workflow](.github/workflows/thread-safety.yml) runs
arena/codegen checks in parallel, isolated JVM forks.

## Next steps and references

The next priorities are converted query arguments, consistent relationship hydration
for built-in lookups, and measured query optimization. Spring mapping/ID evidence
is covered by the real-world matrix. MVCC, transactions, off-heap storage, and distribution remain
design proposals. See the [roadmap](docs/ROADMAP.md) for scope and acceptance criteria.

- [Architecture](docs/ARCHITECTURE.md)
- [Query reference](docs/QUERY.md)
- [Spring integration](docs/SPRING_DATA.md)
- [Concurrency guarantees](docs/CONCURRENCY.md)
- [Test scenarios](docs/TESTING.md)
- [Development and releases](docs/DEVELOPMENT.md)

Contributions are welcome. Repository and coding-agent instructions are in
[AGENTS.md](AGENTS.md).

## License and author

[MIT](LICENSE). Juan Pablo Abelardo Lescano — [@Thejuampi](https://github.com/Thejuampi).
