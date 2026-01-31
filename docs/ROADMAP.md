# Roadmap

## Future Features

### Phase 1: Concurrency Improvements (Short-Term)

**1.1 Fix Critical Concurrency Issues** [Priority: CRITICAL]
- Fix free-list race condition (data corruption)
- Fix tombstone BitSet concurrency
- Implement seqlock for row updates
- Expected: Correct concurrent operations

**1.2 Performance Optimizations** [Priority: HIGH]
- Stripe-based index updates (4-8x throughput)
- Optimistic locking for updates (2x throughput)
- Lock-free data structures (5-10x throughput)
- Expected: Significantly improved concurrent access

### Phase 2: Advanced Concurrency (Medium-Term)

**2.1 Snapshot Isolation** [Priority: MEDIUM]
- MVCC implementation for snapshot queries
- Per-row version chains
- Background version garbage collection
- Expected: 3-5x read throughput, snapshot consistency

**2.2 Transaction Support** [Priority: MEDIUM]
- ACID transaction primitives
- Begin/commit/rollback API
- Optimistic concurrency control
- Expected: Data consistency guarantees

### Phase 3: Relationship Features (Medium-Term)

**3.1 @OneToMany Support** [Priority: HIGH]
- Bidirectional relationships
- Collection field mapping
- Cascade operations
- Expected: Full relationship support

**3.2 @ManyToMany Support** [Priority: MEDIUM]
- Join table generation
- Many-to-many relationship mapping
- Optimized join execution
- Expected: Many-to-many queries

### Phase 4: Storage Evolution (Long-Term)

**4.1 FFM Off-Heap Storage** [Priority: LOW]
- Off-heap storage using Java Foreign Function & Memory (FFM) API
- Reduced GC pressure for large datasets
- Direct memory control
- Memory-mapped file persistence
- Expected: Better performance for large datasets

**4.2 SIMD Vectorization** [Priority: LOW]
- Java Vector API integration
- Parallel scan operations
- SIMD-optimized comparisons
- Expected: 2-4x scan throughput

### Phase 5: Enterprise Features (Long-Term)

**5.1 Schema Evolution** [Priority: LOW]
- Online schema changes
- Migration support
- Backward compatibility
- Expected: Zero-downtime schema updates

**5.2 Distributed Storage** [Priority: LOW]
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
- [IMPLEMENTATION.md](IMPLEMENTATION.md) - Current implementation status
