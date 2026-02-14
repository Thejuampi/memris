# Roadmap

## Implemented Features

### Phase 1: Critical Concurrency Fixes ✅

**1.1 Fix Critical Concurrency Issues** [Priority: CRITICAL] ✅
- Fix free-list race condition (data corruption) ✅
- Fix tombstone BitSet concurrency ✅
- Implement seqlock for row updates ✅
- Fix RepositoryRuntime ID counter concurrency ✅
- **Status**: Complete - All critical concurrency issues resolved

### Phase 2: Performance Optimizations ✅

**2.1 Scan Method Completeness** [Priority: HIGH] ✅
- Add missing scan methods for all type codes ✅
- Complete scanIn implementations ✅
- **Status**: Complete - scanIn method coverage for all types

**2.2 Query Performance** [Priority: HIGH] ✅
- Nested loop scanIn optimization (O(n) → O(1) average case) ✅
- O(n) string pattern scans (documented trade-off, kept as-is) ✅
- Allocation reduction in hot paths ✅
- **Status**: Complete - Query execution optimized

**2.3 Relationship Support** [Priority: HIGH] ✅
- @OneToOne relationships ✅
- @ManyToOne relationships ✅
- @OneToMany relationships ✅
- @ManyToMany relationships ✅
- **Status**: Complete - All relationship types fully implemented

**1.2 Advanced Concurrency Optimizations** [Priority: HIGH] ⏳
- Stripe-based index updates (4-8x throughput) - Not started
- Optimistic locking for updates (2x throughput) - Not started
- ~~Lock-free data structures~~ (5-10x throughput) ~~COMPLETED~~ - Not started
- **Status**: Planned - Future work

---

## Future Features

### Phase 3: Advanced Concurrency (Medium-Term)

**3.1 Snapshot Isolation** [Priority: MEDIUM] ⏳
- MVCC implementation for snapshot queries
- Per-row version chains
- Background version garbage collection
- Expected: 3-5x read throughput, snapshot consistency

**3.2 Transaction Support** [Priority: MEDIUM] ⏳
- ACID transaction primitives
- Begin/commit/rollback API
- Optimistic concurrency control
- Expected: Data consistency guarantees

### Phase 4: Query Enhancements (Medium-Term)

**4.1 Additional Scan Methods** [Priority: MEDIUM] ⏳
- scanNotIn implementations
- scanLike / scanNotLike pattern matching
- Expected: Complete scan method coverage

### Phase 5: Storage Evolution (Long-Term)

**5.1 FFM Off-Heap Storage** [Priority: LOW] ⏳
- Off-heap storage using Java Foreign Function & Memory (FFM) API
- Reduced GC pressure for large datasets
- Direct memory control
- Memory-mapped file persistence
- Expected: Better performance for large datasets

**5.2 SIMD Vectorization** [Priority: LOW] ⏳
- Java Vector API integration
- Parallel scan operations
- SIMD-optimized comparisons
- Expected: 2-4x scan throughput

### Phase 6: Enterprise Features (Long-Term)

**6.1 Schema Evolution** [Priority: LOW] ⏳
- Online schema changes
- Migration support
- Backward compatibility
- Expected: Zero-downtime schema updates

**6.2 Distributed Storage** [Priority: LOW] ⏳
- Partitioned tables
- Distributed queries
- Replication
- Expected: Horizontal scalability

---

## Status Legend

| Status | Meaning |
|---------|----------|
| ✅ Implemented | Feature is complete and tested |
| 🚧 In Progress | Feature is currently being developed |
| ⏳ Planned | Feature is planned but not started |
| 📋 Proposed | Feature is proposed for consideration |
| ❌ Blocked | Feature has known blockers |

---

## See Also

- [CONCURRENCY.md](CONCURRENCY.md) - Current concurrency model and improvements
- [ARCHITECTURE.md](ARCHITECTURE.md) - Current architecture
- [SPRING_DATA.md](SPRING_DATA.md) - Spring Data integration
