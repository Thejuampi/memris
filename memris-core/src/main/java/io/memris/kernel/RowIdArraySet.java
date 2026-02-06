package io.memris.kernel;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.StampedLock;

/**
 * High-performance row ID set for small cardinalities.
 * <p>
 * Optimized for the HashIndex access pattern where writers are serialized
 * by ConcurrentHashMap.compute() and readers are concurrent via index.get().
 * <p>
 * <b>Contract:</b>
 * <ul>
 *   <li>This is a <b>set</b> - no duplicates allowed. add() is idempotent.</li>
 *   <li>remove() is idempotent - no-op when value absent.</li>
 *   <li>size() returns the cardinality (unique count).</li>
 *   <li>enumerator() is snapshot-at-creation and never fails due to concurrent mutation.</li>
 *   <li>Iteration order is unspecified.</li>
 * </ul>
 * <p>
 * <b>Write path:</b> In-place array mutation under write-lock. O(1) amortized.
 * Writer lock is uncontended when used inside CHM.compute().
 * <p>
 * <b>Read path:</b> Wait-free optimistic reads via StampedLock. Zero CAS,
 * zero syscalls in the common case. Falls back to read-lock only if a
 * concurrent write is detected.
 */
public final class RowIdArraySet implements MutableRowIdSet {
    private static final int DEFAULT_CAPACITY = 16;

    private final StampedLock lock = new StampedLock();
    private long[] values;
    private int size;

    public RowIdArraySet() {
        this.values = new long[DEFAULT_CAPACITY];
    }

    public RowIdArraySet(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be non-negative");
        }
        this.values = new long[Math.max(DEFAULT_CAPACITY, initialCapacity)];
    }

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        long stamp = lock.writeLock();
        try {
            // Check for duplicates (set semantics)
            var target = rowId.value();
            for (var i = 0; i < size; i++) {
                if (values[i] == target) {
                    return; // Already present
                }
            }
            ensureCapacity(size + 1);
            values[size++] = target;
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }
        long stamp = lock.writeLock();
        try {
            var target = rowId.value();
            for (var i = 0; i < size; i++) {
                if (values[i] != target) {
                    continue;
                }
                var lastIndex = size - 1;
                values[i] = values[lastIndex];
                values[lastIndex] = 0L;
                size = lastIndex;
                return;
            }
        } finally {
            lock.unlockWrite(stamp);
        }
    }

    @Override
    public int size() {
        // Optimistic read: ~2ns when uncontended
        long stamp = lock.tryOptimisticRead();
        int s = size;
        if (lock.validate(stamp)) {
            return s;
        }
        // Fallback: writer active, take read lock
        stamp = lock.readLock();
        try {
            return size;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }
        var target = rowId.value();

        // Optimistic read: zero-cost if no concurrent writer
        long stamp = lock.tryOptimisticRead();
        var v = values;
        var s = size;
        if (lock.validate(stamp)) {
            for (var i = 0; i < s; i++) {
                if (v[i] == target) {
                    return true;
                }
            }
            if (lock.validate(stamp)) {
                return false;
            }
        }

        // Fallback: writer was active during our read
        stamp = lock.readLock();
        try {
            for (var i = 0; i < size; i++) {
                if (values[i] == target) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public long[] toLongArray() {
        // Optimistic snapshot
        long stamp = lock.tryOptimisticRead();
        var v = values;
        var s = size;
        if (lock.validate(stamp)) {
            var result = Arrays.copyOf(v, s);
            if (lock.validate(stamp)) {
                return result;
            }
        }
        // Fallback
        stamp = lock.readLock();
        try {
            return Arrays.copyOf(values, size);
        } finally {
            lock.unlockRead(stamp);
        }
    }

    @Override
    public LongEnumerator enumerator() {
        // Snapshot under lock protection - enumerator is then lock-free
        var snapshot = toLongArray();
        return new LongEnumerator() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < snapshot.length;
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return snapshot[index++];
            }
        };
    }

    private void ensureCapacity(int neededCapacity) {
        if (neededCapacity <= values.length) {
            return;
        }
        values = Arrays.copyOf(values, Math.max(values.length * 2, neededCapacity));
    }
}
