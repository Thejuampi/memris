package io.memris.kernel;

import java.util.BitSet;
import java.util.NoSuchElementException;

public final class RowIdBitSet implements MutableRowIdSet {
    private final BitSet bitSet = new BitSet();
    private int size;

    @Override
    public void add(RowId rowId) {
        if (rowId == null) {
            throw new IllegalArgumentException("rowId required");
        }
        long value = rowId.value();
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("rowId too large for bitset: " + value);
        }
        int index = (int) value;
        if (!bitSet.get(index)) {
            bitSet.set(index);
            size++;
        }
    }

    @Override
    public void remove(RowId rowId) {
        if (rowId == null) {
            return;
        }
        long value = rowId.value();
        if (value > Integer.MAX_VALUE) {
            return;
        }
        int index = (int) value;
        if (bitSet.get(index)) {
            bitSet.clear(index);
            size--;
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
        long value = rowId.value();
        if (value > Integer.MAX_VALUE) {
            return false;
        }
        return bitSet.get((int) value);
    }

    @Override
    public long[] toLongArray() {
        long[] values = new long[size];
        int index = 0;
        for (int bit = bitSet.nextSetBit(0); bit >= 0; bit = bitSet.nextSetBit(bit + 1)) {
            values[index++] = bit;
        }
        return values;
    }

    @Override
    public LongEnumerator enumerator() {
        return new LongEnumerator() {
            private int current = bitSet.nextSetBit(0);

            @Override
            public boolean hasNext() {
                return current >= 0;
            }

            @Override
            public long nextLong() {
                if (current < 0) {
                    throw new NoSuchElementException();
                }
                long value = current;
                current = bitSet.nextSetBit(current + 1);
                return value;
            }
        };
    }
}
