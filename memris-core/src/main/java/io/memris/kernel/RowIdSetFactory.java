package io.memris.kernel;

public record RowIdSetFactory(int bitSetThreshold) {
    private static final int DEFAULT_BITSET_THRESHOLD = 4096;

    public RowIdSetFactory {
        if (bitSetThreshold <= 0) {
            throw new IllegalArgumentException("bitSetThreshold must be positive");
        }
    }

    public static RowIdSetFactory defaultFactory() {
        return new RowIdSetFactory(DEFAULT_BITSET_THRESHOLD);
    }

    public MutableRowIdSet create(int expectedSize) {
        if (expectedSize >= bitSetThreshold) {
            return new RowIdBitSet();
        }
        return new RowIdArraySet(Math.max(expectedSize, 16));
    }

    public MutableRowIdSet maybeUpgrade(MutableRowIdSet set) {
        if (set instanceof RowIdArraySet arraySet && arraySet.size() >= bitSetThreshold) {
            RowIdBitSet bitSet = new RowIdBitSet();
            LongEnumerator e = arraySet.enumerator();
            while (e.hasNext()) {
                bitSet.add(RowId.fromLong(e.nextLong()));
            }
            return bitSet;
        }
        return set;
    }
}
