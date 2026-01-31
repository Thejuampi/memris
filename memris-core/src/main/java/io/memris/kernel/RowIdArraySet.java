package io.memris.kernel;

import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;

public final class RowIdArraySet implements MutableRowIdSet {
    private static final int DEFAULT_CAPACITY = 16;

    private final AtomicReference<State> state;

    public RowIdArraySet() {
        this.state = new AtomicReference<>(new State(new long[DEFAULT_CAPACITY], 0));
    }

    public RowIdArraySet(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new IllegalArgumentException("initialCapacity must be non-negative");
        }
        int capacity = Math.max(DEFAULT_CAPACITY, initialCapacity);
        this.state = new AtomicReference<>(new State(new long[capacity], 0));
    }

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        long value = rowId.value();
        while (true) {
            State current = state.get();
            int size = current.size;
            int capacity = current.values.length;
            long[] nextValues;
            if (size >= capacity) {
                int newCapacity = Math.max(capacity * 2, size + 1);
                nextValues = Arrays.copyOf(current.values, newCapacity);
            } else {
                nextValues = Arrays.copyOf(current.values, capacity);
            }
            nextValues[size] = value;
            State next = new State(nextValues, size + 1);
            if (state.compareAndSet(current, next)) {
                return;
            }
        }
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }
        long target = rowId.value();
        while (true) {
            State current = state.get();
            int size = current.size;
            long[] values = current.values;
            int index = -1;
            for (int i = 0; i < size; i++) {
                if (values[i] == target) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return;
            }
            long[] nextValues = Arrays.copyOf(values, values.length);
            int last = size - 1;
            nextValues[index] = nextValues[last];
            nextValues[last] = 0L;
            State next = new State(nextValues, size - 1);
            if (state.compareAndSet(current, next)) {
                return;
            }
        }
    }

    @Override
    public int size() {
        return state.get().size;
    }

    @Override
    public boolean contains(RowId rowId) {
        if (rowId == null) {
            return false;
        }
        long target = rowId.value();
        State current = state.get();
        long[] values = current.values;
        for (int i = 0; i < current.size; i++) {
            if (values[i] == target) {
                return true;
            }
        }
        return false;
    }

    @Override
    public long[] toLongArray() {
        State current = state.get();
        return Arrays.copyOf(current.values, current.size);
    }

    @Override
    public LongEnumerator enumerator() {
        State snapshot = state.get();
        return new LongEnumerator() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < snapshot.size;
            }

            @Override
            public long nextLong() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return snapshot.values[index++];
            }
        };
    }

    private record State(long[] values, int size) {
    }
}
