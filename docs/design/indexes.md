# Index Implementations

*For overall architecture, see [ARCHITECTURE.md](../ARCHITECTURE.md)*

## Overview

Memris provides several index types optimized for different query patterns. All indexes store `RowId` sets rather than entity references, enabling efficient set operations and avoiding object allocation.

## Index Types Summary

| Index | Structure | Lookup | Range | Use Case |
|-------|-----------|--------|-------|----------|
| `HashIndex<K>` | ConcurrentHashMap | O(1) | No | Equality queries |
| `RangeIndex<K>` | ConcurrentSkipListMap | O(log n) | O(log n + m) | Comparison, range queries |
| `StringPrefixIndex` | ConcurrentHashMap | O(k) | No | STARTING_WITH |
| `StringSuffixIndex` | Reversed prefix index | O(k) | No | ENDING_WITH |
| `CompositeHashIndex` | HashIndex wrapper | O(1) | No | Multi-column equality |
| `CompositeRangeIndex` | RangeIndex wrapper | O(log n) | O(log n + m) | Multi-column range |

Where:
- k = string length
- m = number of matches
- n = number of unique keys

## HashIndex

**Purpose**: O(1) equality lookups on any hashable key type.

### Structure

```java
public final class HashIndex<K> {
    private final ConcurrentHashMap<K, MutableRowIdSet> index;
    private final RowIdSetFactory setFactory;
}
```

### Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `add(key, rowId)` | O(1) | Add row to key's set |
| `remove(key, rowId)` | O(1) | Remove row from key's set |
| `removeAll(key)` | O(1) | Remove all rows for key |
| `lookup(key)` | O(1) | Get all rows for key |
| `lookup(key, filter)` | O(m) | Filtered lookup |
| `clear()` | O(1) | Clear all entries |

### Thread Safety

- Uses `ConcurrentHashMap.compute()` for atomic updates
- Lock-free reads via `ConcurrentHashMap.get()`
- RowIdSet upgrades handled during add operations

### Example Usage

```java
HashIndex<String> emailIndex = new HashIndex<>();
emailIndex.add("user@example.com", rowId);

RowIdSet matches = emailIndex.lookup("user@example.com");
```

## RangeIndex

**Purpose**: O(log n) lookups for comparison and range queries on Comparable types.

### Structure

```java
public final class RangeIndex<K extends Comparable<K>> {
    private final ConcurrentSkipListMap<K, MutableRowIdSet> index;
    private final RowIdSetFactory setFactory;
}
```

### Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `add(key, rowId)` | O(log n) | Add row to key's set |
| `remove(key, rowId)` | O(log n) | Remove row from key's set |
| `lookup(key)` | O(log n) | Exact key match |
| `between(lower, upper)` | O(log n + m) | Range [lower, upper] |
| `greaterThan(value)` | O(log n + m) | Values > value |
| `greaterThanOrEqual(value)` | O(log n + m) | Values >= value |
| `lessThan(value)` | O(log n + m) | Values < value |
| `lessThanOrEqual(value)` | O(log n + m) | Values <= value |
| `clear()` | O(1) | Clear all entries |

### Thread Safety

- Uses `ConcurrentSkipListMap` for thread-safe ordered access
- Range operations return consistent snapshots
- RowIdSet collection is thread-safe

### Example Usage

```java
RangeIndex<Long> ageIndex = new RangeIndex<>();
ageIndex.add(25L, rowId);
ageIndex.add(30L, rowId2);

// Find ages between 20 and 30
RowIdSet matches = ageIndex.between(20L, 30L);

// Find ages > 25
RowIdSet older = ageIndex.greaterThan(25L);
```

## StringPrefixIndex

**Purpose**: O(k) prefix matching for STARTING_WITH queries.

### Structure

```java
public final class StringPrefixIndex {
    private final ConcurrentHashMap<String, MutableRowIdSet> prefixMap;
    private final boolean ignoreCase;
}
```

### Design

Instead of a trie, uses a HashMap storing all prefixes of each indexed string:
- `"hello"` -> entries for "h", "he", "hel", "hell", "hello"

This provides O(k) lookup where k = prefix length.

### Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `add(key, rowId)` | O(k) | Add all prefixes |
| `remove(key, rowId)` | O(k) | Remove from all prefixes |
| `startsWith(prefix)` | O(k) | Find matching rows |
| `notStartsWith(prefix, allRows)` | O(n) | Inverse match |
| `clear()` | O(1) | Clear all entries |

### Case Sensitivity

```java
// Case-sensitive (default)
StringPrefixIndex sensitive = new StringPrefixIndex();

// Case-insensitive
StringPrefixIndex insensitive = new StringPrefixIndex(true);
```

### Example Usage

```java
StringPrefixIndex nameIndex = new StringPrefixIndex();
nameIndex.add("Johnson", rowId);
nameIndex.add("Johnston", rowId2);

// Find names starting with "John"
RowIdSet matches = nameIndex.startsWith("John");
// Returns both rowId and rowId2
```

## StringSuffixIndex

**Purpose**: O(k) suffix matching for ENDING_WITH queries.

### Structure

```java
public final class StringSuffixIndex {
    private final StringPrefixIndex reversedIndex;
}
```

### Design

Stores strings in reverse order, delegating to StringPrefixIndex:
- `"smith"` -> stored as `"htims"`
- `endsWith("th")` -> becomes `startsWith("ht")` on reversed index

### Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `add(key, rowId)` | O(k) | Add reversed string |
| `remove(key, rowId)` | O(k) | Remove reversed string |
| `endsWith(suffix)` | O(k) | Find matching rows |
| `notEndsWith(suffix, allRows)` | O(n) | Inverse match |
| `clear()` | O(1) | Clear all entries |

### Example Usage

```java
StringSuffixIndex emailIndex = new StringSuffixIndex();
emailIndex.add("user@example.com", rowId);
emailIndex.add("admin@example.com", rowId2);

// Find emails ending with "@example.com"
RowIdSet matches = emailIndex.endsWith("@example.com");
```

## CompositeHashIndex

**Purpose**: O(1) lookups on multi-column composite keys.

### Structure

```java
public final class CompositeHashIndex {
    private final HashIndex<CompositeKey> delegate;
}
```

### CompositeKey

```java
public final class CompositeKey implements Comparable<CompositeKey> {
    private final Object[] values;
    
    public static CompositeKey of(Object[] values);
    public static Object minSentinel();  // Lower bound marker
    public static Object maxSentinel();  // Upper bound marker
}
```

### Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `add(key, rowId)` | O(1) | Add row to composite key |
| `remove(key, rowId)` | O(1) | Remove row from composite key |
| `lookup(key, filter)` | O(1) | Filtered lookup |
| `clear()` | O(1) | Clear all entries |

### Example Usage

```java
CompositeHashIndex index = new CompositeHashIndex();
CompositeKey key = CompositeKey.of(new Object[]{"active", "premium"});
index.add(key, rowId);

RowIdSet matches = index.lookup(key, filter);
```

## CompositeRangeIndex

**Purpose**: O(log n) range queries on multi-column composite keys.

### Structure

```java
public final class CompositeRangeIndex {
    private final RangeIndex<CompositeKey> delegate;
}
```

### Operations

| Method | Complexity | Description |
|--------|------------|-------------|
| `add(key, rowId)` | O(log n) | Add row to composite key |
| `remove(key, rowId)` | O(log n) | Remove row from composite key |
| `lookup(key, filter)` | O(log n) | Exact match |
| `between(lower, upper, filter)` | O(log n + m) | Range query |
| `greaterThan(value, filter)` | O(log n + m) | Greater than |
| `greaterThanOrEqual(value, filter)` | O(log n + m) | Greater than or equal |
| `lessThan(value, filter)` | O(log n + m) | Less than |
| `lessThanOrEqual(value, filter)` | O(log n + m) | Less than or equal |
| `clear()` | O(1) | Clear all entries |

### Sentinel Values

For partial range queries, use sentinel markers:

```java
// Find all rows where first column > "active"
CompositeKey lower = CompositeKey.of(new Object[]{"active", CompositeKey.maxSentinel()});
RowIdSet matches = index.greaterThan(lower, filter);
```

## RowIdSet Integration

All indexes return `RowIdSet` rather than materialized entities:

```java
public interface RowIdSet {
    int size();
    boolean contains(RowId rowId);
    LongEnumerator enumerator();
}
```

### Efficient Iteration

```java
RowIdSet rows = index.lookup(key);
LongEnumerator e = rows.enumerator();
while (e.hasNext()) {
    RowId rowId = RowId.fromLong(e.nextLong());
    // Process row ID directly (no boxing)
}
```

## Index Selection Strategy

Query planner selects indexes based on predicate type:

| Predicate | Index Selected |
|-----------|----------------|
| `x = value` | HashIndex |
| `x IN (values)` | HashIndex |
| `x > value` | RangeIndex |
| `x < value` | RangeIndex |
| `x BETWEEN a AND b` | RangeIndex |
| `x LIKE 'prefix%'` | StringPrefixIndex |
| `x LIKE '%suffix'` | StringSuffixIndex |
| `x LIKE '%contains%'` | Table scan (no index) |
| `x, y = a, b` | CompositeHashIndex |
| `x, y > a, b` | CompositeRangeIndex |

## Memory Considerations

### Index Overhead

| Index | Per-Entry Overhead |
|-------|-------------------|
| HashIndex | ConcurrentHashMap entry + MutableRowIdSet |
| RangeIndex | SkipList node + MutableRowIdSet |
| StringPrefixIndex | O(k) entries per string (k = length) |
| StringSuffixIndex | O(k) entries per string (same as prefix) |

### RowIdSet Upgrade Threshold

RowIdSet implementations automatically upgrade from sparse to dense at 4K entries:
- Sparse: `long[]` array (lower memory for small sets)
- Dense: `BitSet` (O(1) contains for large sets)

## Thread Safety

All index implementations are thread-safe:

| Operation | Thread Safety |
|-----------|---------------|
| Add/Remove | CAS-based updates via ConcurrentHashMap.compute() |
| Lookup | Lock-free reads |
| Range scan | Consistent snapshot via NavigableMap |
| Clear | Atomic map.clear() |

## Performance Guidelines

1. **Use HashIndex for equality**: O(1) vs O(log n) for range index
2. **Prefer RangeIndex for ordered data**: Enables efficient range queries
3. **Use StringPrefixIndex for prefix queries**: O(k) vs O(n) scan
4. **Use StringSuffixIndex for suffix queries**: O(k) vs O(n) scan
5. **Composite indexes for multi-column predicates**: Avoid intersection overhead
6. **Consider memory overhead**: StringPrefixIndex stores O(k) entries per value
