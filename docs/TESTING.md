# Test scenarios

`./gradlew realWorldTest` is the entry point for public behavior across core,
Spring Boot 2, and Spring Boot 3. Scenarios live in `io.memris.realworld` in each
module's `src/test/java`. They use the actual engine, generated repositories,
storage, indexes, converters, and Spring contexts.

## Execution matrix

Every real-world suite runs in four separate JVM configurations:

| Task suffix | Table implementation | Runtime codegen |
| --- | --- | --- |
| `realWorldBytecodeTest` | ByteBuddy bytecode | Enabled |
| `realWorldBytecodeFallbackTest` | ByteBuddy bytecode | Disabled |
| `realWorldMethodHandleTest` | Method handles | Enabled |
| `realWorldMethodHandleFallbackTest` | Method handles | Disabled |

Core scenarios obtain their arena configuration from `RealWorldSupport`; Spring
scenarios bind the same matrix values through application properties. Index/scan
comparisons are parameters within the scenarios. Factories and contexts close at
the end of each case. Locale cases lock the locale resource and restore all JVM
locale defaults in `finally`.

`./gradlew test` includes all four variants and the remaining internal tests.
To iterate on one scenario and mode:

```sh
./gradlew :memris-core:realWorldBytecodeTest --tests 'io.memris.realworld.IndexedCatalogRealWorldTest'
./gradlew :memris-spring-data-boot3:realWorldMethodHandleFallbackTest
```

Results live under each module's `build/test-results/<task>/` and
`build/reports/tests/<task>/`. `jacocoTestReport` merges the four real-world
execution files and the internal tests, so changing the test layout does not
exclude repository coverage from the report.

## Scenario map

The core directory is [realworld](../memris-core/src/test/java/io/memris/realworld/).
The suites are grouped by observable behavior rather than runtime implementation
methods:

| Behavior | Suites |
| --- | --- |
| Customer search, nulls, duplicate emails, Optional cardinality, Unicode, typed analytics | `ECommerceRealWorldTest` |
| Checkout, order items, customer/order navigation and updates | `CheckoutRealWorldTest` |
| Catalog availability, derived/annotated count and exists during stock changes | `CatalogSummaryRealWorldTest` |
| Ranking, first/top, annotated limits | `CatalogRankingRealWorldTest` |
| Hash/range/prefix/suffix searches after repricing, renaming, deletion, row reuse and clearing | `IndexedCatalogRealWorldTest` |
| Composite hash and range predicates, count and exists | `CompositeIndexRealWorldTest` |
| CRUD, bulk operations, IN, grouping and HAVING | `RepositoryCrudAndGroupingRealWorldTest`, `RepositoryIdRealWorldTest` |
| Annotated queries, projections, joins, ordering, updates and deletes | `EcommerceAnnotatedQueriesRealWorldTest` |
| Embedded graphs, converted assignments and repeated hydration | `EmbeddedMappingRealWorldTest`, `ConvertedMappingRealWorldTest` |
| Relationships, lifecycle callbacks, wide/nullable materialization and churn | `RelationshipsAndTypesRealWorldTest` |
| Nullable, integer, floating-point ordering and scalar ranges | `NullableQueriesRealWorldTest`, `NumericOrderingRealWorldTest`, `FloatingPointOrderingRealWorldTest`, `ScalarRangesRealWorldTest` |
| Integer/long extrema with scans, range indexes and indexed residuals | `NumericBoundaryRealWorldTest` |
| Arena isolation, default arenas and terminal arena close | `ArenaLifecycleRealWorldTest` |

Both Spring modules have `SpringInventoryRealWorldTest`: automatic repository
discovery and injection, JPA converter round trips (including nulls), stock
queries and updates, deletion, named-arena isolation, fresh application contexts,
configuration overrides, and arena cleanup on context close. Each module retains
its own persistence API and Spring dependency generation.

The same scenarios also cover generic inherited converters, embedded nullable
stock mappings with LocalDate/UUID values, receipt/inventory join hydration,
`AUTO` Long IDs, `IDENTITY` Integer IDs, and assigned String warehouse keys.
ID workflows include imported/generated key collisions, non-default primary-key
names/positions, updates/deletion, and range exhaustion. Negative scenarios reject
JPA-only IDs at context startup and unsupported/named generation declarations
during repository creation. These public contracts remain in
`SpringInventoryRealWorldTest`, without a parallel unit-test suite.

## Internal edge cases

The remaining component tests isolate risks that a business workflow cannot
reliably force or observe:

| Area | Retained checks |
| --- | --- |
| Architecture and metadata | Module boundaries, ambiguous IDs, invalid embedded paths, defensive metadata and typed dispatch contracts |
| Query parsing | Invalid/ambiguous method names, nested paths, precedence, escaped literals and parser/compiler representations |
| Storage and row sets | Page/word/chunk boundaries, publication, seqlocks, generations, stale/tombstoned rows, packed references and snapshot copies |
| Index internals | Null/sentinel bounds, partial removals, filtered probes, concurrent mutation and ordered key/set invariants |
| Runtime and generated code | Custom handler registration, invalid direct-handler inputs, unsupported operators, NaN/infinity helpers, cache identity and arena isolation, fallback/error paths and absence of per-row reflection |
| Concurrency | Capacity/free-list races, publication, simultaneous repository creation and terminal close |
| Spring metadata | Persistence-generation-specific ID discovery and invalid entity declarations |

Ordinary handler EQ/IN/range/pattern tests with fake storage have been replaced by
the real repository scenarios. `TypeHandlerEdgeCaseTest` retains invalid inputs
that correctly typed repository interfaces cannot express. Diagnostic index
tests, JVM-library-only assertions, no-op class/constructor checks, and timing
thresholds in functional tests have been removed. Performance measurements use
the existing JMH source set, including `StringPatternBenchmark` and
`PatternMatchingBenchmark`.

The concurrency guard command remains `./gradlew :memris-core:threadSafetyTest`;
it runs the arena/codegen cases in parallel, isolated JVM forks.
