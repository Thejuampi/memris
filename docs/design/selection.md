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

## Implementations

| Implementation | `contains()` | `add()` | Memory | Use Case |
|----------------|--------------|---------|--------|----------|
| `IntSelection` | O(n) linear | O(1) amortized | Sparse | < 4K rows |
| `BitsetSelection` | O(1) | O(1) | Dense | >= 4K rows |

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

## SIMD Scan Path

```
FFM MemorySegment -> VectorMask -> mask.toLong() -> enumerate bits -> SelectionVector
```

1. Load vector from off-heap segment
2. Compare with predicate value
3. Convert mask to long (64 lanes packed)
4. Extract set bits using trailing zero tricks
5. Add to SelectionVector
6. Upgrade if needed

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
| `HashIndex.lookup()` | O(1) average | ConcurrentHashMap |
| `RangeIndex.lookup()` | O(log n) | ConcurrentSkipListMap |
| `RowIdSet.contains()` | O(1) | BitSet or array scan |
| `SelectionVector.enumerator()` | O(1) | Return preallocated cursor object |
| `BitsetSelection.add()` | O(1) | BitSet.set() |
| `IntSelection.add()` | O(1) amortized | Array append with 2x resize |

## Forbidden Patterns

- ❌ Linear scan for filtering (use SIMD vector masks)
- ❌ `for (Object o : collection)` iteration (use IntEnumerator)
- ❌ `Collection.contains()` on large sets (use BitsetSelection)
- ❌ Boxing primitives in hot paths (Integer, Long, Boolean)
