package io.memris.kernel;

import java.util.Arrays;
import java.util.NoSuchElementException;

public final class RowIdArraySet implements MutableRowIdSet {
    private static final int DEFAULT_CAPACITY = 16;

    private long[] values;
    private int size;

    public RowIdArraySet() {
        this.values = new long[DEFAULT_CAPACITY];
    }

    public RowIdArraySet(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be non-negative");
        }
        var capacity = Math.max(DEFAULT_CAPACITY, initialCapacity);
        this.values = new long[capacity];
    }

    @Override
    public synchronized void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        ensureCapacity(size + 1);
        values[size++] = rowId.value();
    }

    @Override
    public synchronized void remove(RowId rowId) {
        if (rowId == null || size == 0) {
            return;
        }
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
    }

    @Override
    public synchronized int size() {
        return size;
    }

    @Override
    public synchronized boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }
        var target = rowId.value();
        for (var i = 0; i < size; i++) {
            if (values[i] == target) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized long[] toLongArray() {
        return Arrays.copyOf(values, size);
    }

    @Override
    public LongEnumerator enumerator() {
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
        var newCapacity = Math.max(values.length * 2, neededCapacity);
        values = Arrays.copyOf(values, newCapacity);
    }
}
