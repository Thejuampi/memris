# Selection Pipeline Design

*For overall architecture, see [ARCHITECTURE.md](../ARCHITECTURE.md)*

## Guiding Principle
**O(1) first, O(log n) second, O(n) forbidden.**

All core operations must meet this contract.

## SelectionVector Interface

```java
public interface SelectionVector {
    int size();                         // O(1)
    boolean contains(int rowIndex);     // O(1) - bitset or array lookup
    int[] toIntArray();                 // O(n) - export only
    IntEnumerator enumerator();         // O(1) - primitive cursor
    SelectionVector filter(Predicate p, SelectionVectorFactory f); // O(n) over selected
}
```

## RowIdSet Interface

Row-level selection with 64-bit RowId support:

```java
public interface RowIdSet {
    int size();                         // O(1)
    boolean contains(RowId rowId);      // O(1)
    LongEnumerator enumerator();        // O(1) - primitive cursor
}
```

## Implementations

### SelectionVector Implementations

| Implementation | `contains()` | `add()` | Memory | Use Case |
|----------------|--------------|---------|--------|----------|
| `IntSelection` | O(n) linear | O(1) amortized | Sparse | < 4K rows |
| `BitsetSelection` | O(1) | O(1) | Dense | >= 4K rows |

### RowIdSet Implementations

| Implementation | `contains()` | `add()` | Memory | Use Case |
|----------------|--------------|---------|--------|----------|
| `SparseRowIdSet` | O(n) linear | O(1) amortized | Sparse | < 4K rows |
| `DenseRowIdSet` | O(1) | O(1) | Dense | >= 4K rows |

## Factory Upgrade Logic

```java
public MutableSelectionVector create(int expectedSize) {
    if (expectedSize >= BITSET_THRESHOLD) {
        return new BitsetSelection();
    }
    return new IntSelection(Math.max(expectedSize, 16));
}

public MutableSelectionVector maybeUpgrade(MutableSelectionVector set) {
    if (set instanceof IntSelection s && s.size() >= BITSET_THRESHOLD) {
        return upgradeToBitset(s);
    }
    return set;
}
```

### RowIdSetFactory

```java
public MutableRowIdSet create(int expectedSize) {
    if (expectedSize >= BITSET_THRESHOLD) {
        return new DenseRowIdSet();
    }
    return new SparseRowIdSet(expectedSize);
}

public MutableRowIdSet maybeUpgrade(MutableRowIdSet set) {
    if (set instanceof SparseRowIdSet s && s.size() >= BITSET_THRESHOLD) {
        return upgradeToDense(s);
    }
    return set;
}
```

## Scan Path

```
PageColumn -> Scan Operation -> int[] offsets
```

1. Read value from primitive array (int[], long[], String[])
2. Compare with predicate value
3. Collect matching offsets into result array
4. No intermediate SelectionVector - direct offset arrays

### PageColumn Scan Methods

| Column Type | Scan Methods |
|-------------|--------------|
| `PageColumnInt` | `scanEquals`, `scanGreaterThan`, `scanLessThan`, `scanGreaterThanOrEqual`, `scanLessThanOrEqual`, `scanBetween`, `scanIn` |
| `PageColumnLong` | `scanEquals`, `scanGreaterThan`, `scanLessThan`, `scanGreaterThanOrEqual`, `scanLessThanOrEqual`, `scanBetween`, `scanIn` |
| `PageColumnString` | `scanEquals`, `scanEqualsIgnoreCase`, `scanIn`, `scanNull` |

All scan methods return `int[]` of matching row offsets with O(n) complexity where n is the published row count.

## Primitive Enumerators

No boxing, no Iterator, no Iterable:

```java
public interface IntEnumerator {
    boolean hasNext();
    int nextInt();  // primitive int
}

public interface LongEnumerator {
    boolean hasNext();
    long nextLong(); // primitive long
}
```

## O(1) Guarantees

| Operation | Complexity | Implementation |
|-----------|------------|----------------|
| `SelectionVector.contains()` | O(1) | BitSet check or int array scan for small sets |
| `RowIdSet.contains()` | O(1) | BitSet or array scan |
| `HashIndex.lookup()` | O(1) average | ConcurrentHashMap |
| `RangeIndex.lookup()` | O(log n) | ConcurrentSkipListMap |
| `SelectionVector.enumerator()` | O(1) | Return preallocated cursor object |
| `BitsetSelection.add()` | O(1) | BitSet.set() |
| `IntSelection.add()` | O(1) amortized | Array append with 2x resize |
| `DenseRowIdSet.add()` | O(1) | BitSet.set() |
| `SparseRowIdSet.add()` | O(1) amortized | Array append with 2x resize |

## Selection in Query Execution

### Index Lookup Path

```
HashIndex.lookup(key) -> RowIdSet -> int[] offsets -> Entity materialization
```

### Table Scan Path

```
PageColumn.scanEquals(value, published) -> int[] offsets -> Entity materialization
```

### Filtered Scan Path

```
PageColumn.scan*() -> int[] offsets -> RowIdSet filter -> int[] filtered -> Entity materialization
```

## RowIdSets Utility

Factory methods for empty and singleton sets:

```java
public final class RowIdSets {
    public static RowIdSet empty();              // Singleton empty set
    public static RowIdSet of(RowId rowId);      // Singleton set
    public static RowIdSet of(RowId... rowIds);  // Small immutable set
}
```

## Forbidden Patterns

- Linear scan without early termination
- `for (Object o : collection)` iteration (use IntEnumerator/LongEnumerator)
- `Collection.contains()` on large sets (use BitsetSelection/DenseRowIdSet)
- Boxing primitives in hot paths (Integer, Long, Boolean)
- Creating SelectionVector for single-row lookups (use direct column access)

## Integration with Indexes

### HashIndex Integration

```java
HashIndex<String> emailIndex = ...;
RowIdSet rows = emailIndex.lookup("user@example.com");
LongEnumerator e = rows.enumerator();
while (e.hasNext()) {
    RowId rowId = RowId.fromLong(e.nextLong());
    // Materialize entity
}
```

### RangeIndex Integration

```java
RangeIndex<Long> ageIndex = ...;
RowIdSet rows = ageIndex.between(18L, 65L);
// Process matching rows
```

### StringPrefixIndex Integration

```java
StringPrefixIndex nameIndex = ...;
RowIdSet rows = nameIndex.startsWith("John");
// All names starting with "John"
```

### StringSuffixIndex Integration

```java
StringSuffixIndex domainIndex = ...;
RowIdSet rows = domainIndex.endsWith("@example.com");
// All emails ending with "@example.com"
```
