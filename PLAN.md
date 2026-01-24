# Memris - Refined Implementation Plan

## North Star
**Goal:** Spring Data–compatible in-memory engine with indexes + joins + paging/sorting, no JDBC/DataSource, non-ACID semantics tuned for tests and inner-loop development.

**Non-goals:**
* No JPA semantics (EntityManager, flush modes, cascades, lazy proxies)
* No full JPQL/HQL promise

**Compatibility target:**
* `CrudRepository` / `PagingAndSortingRepository`
* High coverage of derived queries (`findBy…And…`, `In`, `Between`, comparisons, `OrderBy`, `Top/First`, `existsBy`, `countBy`)

## Core Architecture

### 1. Query Kernel (no Spring, no reflection, no entities)
**Primitives:**
* `RowId` (int/long)
* `Column<T>` accessor (compiled getter or offset-based)
* `Table` (rows + indexes)
* `Predicate`, `JoinCondition`
* `PlanNode` (scan/filter/join/sort/limit/project)
* `Executor` (runs a plan, returns RowIds or projected results)

### 2. Storage Layout
**Hybrid row store:**
* Store rows in compact internal representation ("row record"), not arbitrary POJOs
* Maintain separate "materializer" to rehydrate entities for repository return values

### 3. Index API
**Contracts:**
* `EqualityIndex<K>`: `lookup(K) -> RowIdSet`
* `RangeIndex<K>`: `between(a,b)`, `gt(x)`, `lt(x)`, and `orderedIterator()`
* `JoinIndex` / adjacency store for relationships:
  * `ownerId -> childRowIds`
  * `leftId -> rightIds` for M:N

### 4. RowIdSet
**Representations:**
* Simple `IntArrayList` for small sets
* BitSet-like for dense sets
* Optional roaring-bitmap style later

### 5. Query Planning
**Internal algebra:**
* `Scan(table)`
* `Filter(child, predicate)`
* `Join(left, right, joinType, condition)`
* `Project(child, columns)`
* `Sort(child, orderings)`
* `Limit(child, offset, limit)`

**Cost model:**
* Estimate cardinality using index stats + heuristics
* Join selection: index nested loop vs hash join
* Sort pushdown: range index stream vs in-memory sort

### 6. MVCC Snapshots
**Minimal semantics:**
* Global `version` counter increments on commit
* Tables store rows with `(createdVersion, deletedVersion?)`
* Readers capture `readVersion` and ignore changes after it

## Implementation Phases

### Phase 1 — Kernel + indexes (demo: 10M lookup)
* Table + row layout
* Hash index + range index
* Filter execution with index selection
* JMH + README benchmark chart

### Phase 2 — Joins (demo: 1M x 10M join)
* Hash join + index nested loop join
* Adjacency join store for 1:N and M:N
* `explain()` shows join choice

### Phase 3 — Planner + visualization (demo: plan diffs)
* Cost estimates + index choice
* GraphViz output for plans
* Actual vs estimated row counts

### Phase 4 — MVCC snapshots (demo: concurrent readers/writer)
* Snapshot versioning
* Lock-free reads (or minimal lock)
* Deterministic replay option

### Phase 5 — Spring Data adapter (demo: drop-in tests)
* Derived queries → internal plans
* `@FastMemDataTest` slice
* Sample Spring Boot app + test suite speed comparison

## Technical Approach

### Prototype First Strategy
* Start with `TreeMap`/skip list + `BitSet` for correctness
* Replace components one at a time with custom implementations
* Keep demos shipping continuously while going deep on learning

### Key Dependencies
* Java 21+
* No external DataSource required
* Optional: RoaringBitmap for compressed indexes (Phase 3+)
* Optional: Panama Vector API for SIMD (Phase 4+)