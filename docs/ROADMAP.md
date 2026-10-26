# Roadmap

Baseline: the 0.3.0 release line, reviewed on October 25, 2026. Implementation and executable scenarios determine
current support; an annotation, lexer token, or historical plan alone does not.

## Version and release state

| Item | Current state |
| --- | --- |
| Published artifacts | `0.2.4` for core, both Spring integrations, and both starters; see [Maven Central metadata](https://repo.maven.apache.org/maven2/io/github/thejuampi/memris/maven-metadata.xml) |
| Repository version | `0.3.0-SNAPSHOT` in [gradle.properties](../gradle.properties) |
| Release automation | Manual Gradle-based [release workflow](../.github/workflows/release.yml) from `main`, with packaged-consumer checks, signed publication, a tag/GitHub Release, dependency-example updates, and the next snapshot commit |

Published and repository versions are tracked separately in `gradle.properties`.
The [0.3.0 release notes](releases/0.3.0.md) describe the changes in this release line.

## Implemented baseline

| Area | Implemented behavior | Evidence |
| --- | --- | --- |
| Heap storage | Paged int/long/string columns, typed row APIs, presence/null tracking, row generations and tombstones | [TableGenerator](../memris-core/src/main/java/io/memris/storage/heap/TableGenerator.java), [storage tests](../memris-core/src/test/java/io/memris/storage/) |
| Runtime initialization | Generated tables/repositories/savers/materializers, compiled embedded access plans, arena-owned generated-artifact caches | [EmbeddedMappingRealWorldTest](../memris-core/src/test/java/io/memris/realworld/EmbeddedMappingRealWorldTest.java), [ConvertedMappingRealWorldTest](../memris-core/src/test/java/io/memris/realworld/ConvertedMappingRealWorldTest.java) |
| Repository operations | CRUD and bulk operations, explicit IDs, typed find/count/exists queries, grouping and HAVING | [RepositoryCrudAndGroupingRealWorldTest](../memris-core/src/test/java/io/memris/realworld/RepositoryCrudAndGroupingRealWorldTest.java) |
| Query execution | Comparisons, ranges, membership, positive/negative patterns, boolean/null predicates, AND/OR, sorting/top/first/distinct, a JPQL-like annotated subset and record projections | [ECommerceRealWorldTest](../memris-core/src/test/java/io/memris/realworld/ECommerceRealWorldTest.java), [EcommerceAnnotatedQueriesRealWorldTest](../memris-core/src/test/java/io/memris/realworld/EcommerceAnnotatedQueriesRealWorldTest.java) |
| Indexes | Hash, range, prefix and suffix indexes; composite hash/range probes and indexed residual filtering | [IndexedCatalogRealWorldTest](../memris-core/src/test/java/io/memris/realworld/IndexedCatalogRealWorldTest.java), [CompositeIndexRealWorldTest](../memris-core/src/test/java/io/memris/realworld/CompositeIndexRealWorldTest.java) |
| Mapping and relationships | Scalar converters, embedded graphs, eager one-to-one/many-to-one/one-to-many/many-to-many hydration | [CheckoutRealWorldTest](../memris-core/src/test/java/io/memris/realworld/CheckoutRealWorldTest.java), [RelationshipsAndTypesRealWorldTest](../memris-core/src/test/java/io/memris/realworld/RelationshipsAndTypesRealWorldTest.java) |
| Concurrency and lifecycle | CAS-based row allocation/tombstones, row seqlocks and publication, concurrent repository creation, isolated arenas and terminal arena close | [Concurrency contract](CONCURRENCY.md), [ArenaLifecycleRealWorldTest](../memris-core/src/test/java/io/memris/realworld/ArenaLifecycleRealWorldTest.java), [concurrency workflow](../.github/workflows/thread-safety.yml) |
| Spring integrations | Separate Boot 2/`javax` and Boot 3/`jakarta` modules, repository scanning/injection, configuration overrides, named arenas, converter round trips and context cleanup | [Boot 2 inventory scenarios](../memris-spring-data-boot2/src/test/java/io/memris/realworld/SpringInventoryRealWorldTest.java), [Boot 3 inventory scenarios](../memris-spring-data-boot3/src/test/java/io/memris/realworld/SpringInventoryRealWorldTest.java) |
| Verification | Four table/codegen configurations, centralized real-world scenarios, focused internal edge cases, merged coverage, static analysis and JMH guardrails | [TESTING.md](TESTING.md), [DEVELOPMENT.md](DEVELOPMENT.md), [performance workflow](../.github/workflows/perf.yml) |

`NOT_IN`, `LIKE`, and `NOT_LIKE` already execute at repository level. Dedicated
column scan kernels or faster dispatch for these operators would be optimizations,
not the introduction of missing query features. Full scans remain linear; index
operations include the cost of collecting matches and materializing entities.

Row coordination does not provide transactions or snapshot isolation. Index and
row publication can be observed at different times. Existing lock-free/CAS-based
structures do not make every operation lock-free or eliminate all contention.

### Recently completed

- [#51](https://github.com/Thejuampi/memris/pull/51): Gradle wrapper/build migration and consolidated repository guidance.
- [#52](https://github.com/Thejuampi/memris/pull/52): Checkstyle and SpotBugs failures resolved.
- [#53](https://github.com/Thejuampi/memris/pull/53): int/long exclusive range boundaries no longer overflow; regressions cover extrema, scans, indexes, residuals, count and exists.
- [#54](https://github.com/Thejuampi/memris/pull/54): Six disabled e-commerce tests restored with explicit contracts; locale-independent patterns, `LikeIgnoreCase` dispatch and empty ordered selections corrected.
- [#55](https://github.com/Thejuampi/memris/pull/55): Real-world suite centralization and the four-mode matrix; stock/catalog/workforce workflows; redundant tests removed; Spring startup, delegation and Memris ID discovery corrected.

## 0.3.0 release line

The release includes the completed query aliases (`EqualsIgnoreCase`,
`IsIgnoreCase`, `AllIgnoreCase`, `IsNotNull`, and `removeBy`), locale/index parity
scenarios, and independent [core and starter consumers](https://github.com/Thejuampi/memris/tree/main/verification/consumers).
The consumers validate repository discovery, CRUD, converter/null handling, named
arenas, and terminal context shutdown through packaged dependency graphs.
Both adapters publish the required persistence API alongside their own Spring
generation. Publication checks inspect all five artifact versions and POMs.

The [release workflow](../.github/workflows/release.yml) runs these checks before
signed publication, then records the tag, GitHub Release, documentation versions,
and next snapshot. The version table above records publication state.

## Next priorities

These are proposed next PRs, in order, without promised delivery dates or
throughput multipliers.

### 1. Expand Spring compatibility evidence

Keep the metadata contract explicit: the default engine provider reads Memris
annotations. Spring `EntityInformation` accepts a Memris ID or its generation's
JPA ID, and the converter bridge handles JPA attribute converters. This does not
provide a general JPA metadata adapter. Extend repository scenarios for any
additional mappings or method spellings before documenting them as supported.

Document ID strategies from executed behavior: the existence of
`GenerationType.CUSTOM`/`IdGenerator` alone does not establish a registration or
execution path. Keep Boot 2 and Boot 3 dependency families separate.

### 2. Optimize measured bottlenecks

Use the existing JMH suites to select work: scan/membership dispatch, string
patterns, index selectivity, sorting, materialization and write contention. Native
NOT_IN/LIKE kernels, fewer per-row allocations, or changes to concurrent index
updates should follow measurements rather than an assumed speedup.

Acceptance: before/after results with commit, JDK, host and dataset details;
allocation/throughput evidence for the affected workload; unchanged public
contracts across indexed and scan paths; and reproducible regression thresholds.
Keep heap storage, primitive execution APIs, arena cache ownership and publication
protocols intact.

## Longer-term proposals

The following ideas are uncommitted design work, not implemented features or
requirements for the next release:

| Proposal | Decision needed before implementation |
| --- | --- |
| MVCC / snapshot reads | Define observable consistency, version lifetime, reclamation and memory costs |
| Transactions | Define atomicity across rows/indexes, conflict behavior, rollback and the public API |
| SIMD scans | Establish a workload benefit and a Java/runtime compatibility policy |
| Off-heap / FFM / persistence | Decide whether a separate storage backend fits the current heap-based contract, including recovery and lifecycle semantics |
| Schema evolution | Define migration, compatibility and behavior of already-generated repositories |
| Distribution / replication | Establish a concrete use case and a separate consistency, partitioning and failure model |

## References

- [README](../README.md): project entry point and minimal example
- [TESTING.md](TESTING.md): scenario ownership and execution matrix
- [DEVELOPMENT.md](DEVELOPMENT.md): builds, quality checks, JMH and releases
- [QUERY.md](QUERY.md): query language and supported method spellings
- [SPRING_DATA.md](SPRING_DATA.md): Spring module boundaries
- [ARCHITECTURE.md](ARCHITECTURE.md) and [CONCURRENCY.md](CONCURRENCY.md): implementation model and guarantees
