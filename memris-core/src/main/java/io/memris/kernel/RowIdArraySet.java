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
        this.values = new long[Math.max(DEFAULT_CAPACITY, initialCapacity)];
    }

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        ensureCapacity(size + 1);
        values[size++] = rowId.value();
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }
        long target = rowId.value();
        for (int i = 0; i < size; i++) {
            if (values[i] == target) {
                int last = size - 1;
                values[i] = values[last];
                values[last] = 0L;
                size--;
                return;
            }
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }
        long target = rowId.value();
        for (int i = 0; i < size; i++) {
            if (values[i] == target) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long[] toLongArray() {
        return Arrays.copyOf(values, size);
    }

    @Override
    public LongEnumerator enumerator() {
        return new LongEnumerator() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < size;
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return values[index++];
            }
        };
    }

    private void ensureCapacity(int desired) {
        if (desired <= values.length) {
            return;
        }
        int newCapacity = Math.max(values.length * 2, desired);
        values = Arrays.copyOf(values, newCapacity);
    }
}
