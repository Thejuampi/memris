# Table Capacity Analysis and Dynamic Growth Plan

## Current Limitation

**Default Capacity:** 1,048,576 rows (1024 pages × 1024 rows per page)

**Location:** `MemrisConfiguration.java` lines 129-130
```java
private int defaultPageSize = 1024;
private int defaultMaxPages = 1024;
```

**Why the limit exists:**
The current architecture pre-allocates all data structures at table creation time:

1. **AbstractTable** pre-allocates:
   - `tombstones`: AtomicIntegerArray[maxPages * pageSize]
   - `rowGenerations`: long[maxPages * pageSize]
   - `rowSeqLocks`: AtomicLongArray[maxPages * pageSize]

2. **PageColumn* classes** pre-allocate:
   - `data`: int[]/long[]/String[capacity]
   - `present`: byte[capacity]

3. **IdIndex classes** pre-allocate:
   - Hash maps with initial capacity

This design was chosen for:
- **Zero-allocation hot path** - No GC pressure during inserts
- **Predictable latency** - No resizing delays during operations
- **Memory locality** - Dense arrays for cache efficiency
- **Simplicity** - No concurrency concerns during resize

## The Problem

For 4 billion rows with current architecture:
- Memory required: ~64GB just for row metadata (tombstones + generations + locks)
- Column data: Additional memory per column
- Realistically unusable without terabytes of RAM

## Solution: Dynamic Growth

Instead of pre-allocating 4B rows, implement **tiered dynamic expansion**:

### Architecture Changes

#### 1. Segment-Based Storage (like G1 GC)
```
Tier 1: 16 pages (16K rows) - initial
Tier 2: 256 pages (256K rows) - when 75% full
Tier 3: 4096 pages (4M rows) - when 75% full
Tier 4: 65536 pages (64M rows) - when 75% full
Tier 5: 1048576 pages (1B rows) - when 75% full
Tier 6: 4194304 pages (4B rows) - maximum
```

#### 2. Column Storage Changes
Replace fixed arrays with segment tree:
```java
public class PageColumnLong {
    private volatile long[][] segments;  // Indirection array
    private final AtomicInteger segmentCount = new AtomicInteger(1);
    private static final int SEGMENT_SIZE = 16384;  // 16K rows per segment
    
    public long get(int index) {
        int segment = index >>> 14;  // / 16384
        int offset = index & 0x3FFF; // % 16384
        return segments[segment][offset];
    }
    
    public void set(int index, long value) {
        int segment = index >>> 14;
        int offset = index & 0x3FFF;
        segments[segment][offset] = value;
    }
    
    // Grow when needed
    private void ensureCapacity(int index) {
        int requiredSegment = index >>> 14;
        if (requiredSegment >= segmentCount.get()) {
            growSegments(requiredSegment + 1);
        }
    }
    
    private void growSegments(int newCount) {
        // Double the segment array size
        // Allocate new segments lazily
    }
}
```

#### 3. Metadata Storage Changes
Replace fixed arrays with concurrent hash maps for sparse metadata:
```java
// Instead of:
private final AtomicIntegerArray tombstones;  // Fixed size
private final long[] rowGenerations;           // Fixed size

// Use:
private final ConcurrentHashMap<Integer, Integer> tombstones;  // Sparse
private final ConcurrentHashMap<Integer, Long> rowGenerations; // Sparse
```

Or use Long2LongOpenHashMap from fastutil for better performance.

#### 4. Allocation Strategy
```java
public class TableCapacityManager {
    private static final int[] TIERS = {16, 256, 4096, 65536, 1048576, 4194304};
    private volatile int currentTier = 0;
    private final AtomicInteger allocatedRows = new AtomicInteger(0);
    
    public boolean needsGrowth() {
        return allocatedRows.get() > (TIERS[currentTier] * 0.75);
    }
    
    public void grow() {
        if (currentTier < TIERS.length - 1) {
            currentTier++;
            // Trigger segment allocation across all columns
        }
    }
}
```

### Benefits

1. **Memory Efficiency:** 
   - Start with ~256KB per table (16K rows × 16 bytes metadata)
   - Grow only when needed
   - 4B rows would only use memory for actual data, not empty capacity

2. **Performance:**
   - Hot path: Single array access (same speed as now)
   - Growth: Happens infrequently, amortized cost
   - No GC overhead during normal operations

3. **Scalability:**
   - Support 4B rows with reasonable memory usage
   - Each column grows independently
   - Can handle sparse data efficiently

### Implementation Plan

#### Phase 1: AbstractTable Dynamic Metadata
- Replace fixed arrays with ConcurrentHashMap for tombstones
- Replace rowGenerations with Long2LongOpenHashMap
- Replace rowSeqLocks with striped locks

#### Phase 2: PageColumn Dynamic Segments  
- Implement segment-based storage in PageColumnInt/Long/String
- Add segment growth logic
- Ensure thread-safe expansion

#### Phase 3: Configuration Update
- Remove maxPages limit or set to Integer.MAX_VALUE
- Add growth strategy configuration
- Add memory monitoring

#### Phase 4: Testing
- Benchmark insert performance at different sizes
- Test concurrent growth scenarios
- Memory usage validation

### Migration Strategy

This is a **breaking change** requiring:
1. Update all PageColumn* classes
2. Update AbstractTable
3. Update table generators
4. Update serialization format
5. Comprehensive testing

### Alternative: Quick Fix for Benchmarks

For immediate benchmark relief without full dynamic growth:
```java
// MemrisConfiguration.builder()
//     .defaultMaxPages(65536)  // 67M rows
//     .build();
```

**Trade-off:** Uses ~1GB RAM per table at startup vs ~1MB currently.

## Recommendation

**Short-term:** Increase defaultMaxPages to 65536 for benchmarks (67M rows)
**Long-term:** Implement segment-based dynamic growth for true 4B row support

The dynamic growth architecture will enable:
- Tables with billions of rows
- Memory-proportional-to-data usage
- Zero-allocation hot paths
- Predictable performance
