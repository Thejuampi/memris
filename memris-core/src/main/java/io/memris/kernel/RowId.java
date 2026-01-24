package io.memris.kernel;

/**
 * 64-bit composite row identifier.
 * Layout: [48 bits page id][16 bits offset].
 */
public final class RowId implements Comparable<RowId> {
    private static final int OFFSET_BITS = 16;
    private static final int PAGE_BITS = 48;
    private static final long OFFSET_MASK = (1L << OFFSET_BITS) - 1;
    private static final long PAGE_MASK = (1L << PAGE_BITS) - 1;

    private final long value;

    public RowId(long pageId, int offset) {
        if (pageId < 0 || pageId > PAGE_MASK) {
            throw new IllegalArgumentException("pageId out of range: " + pageId);
        }
        if (offset < 0 || (offset & ~OFFSET_MASK) != 0) {
            throw new IllegalArgumentException("offset out of range: " + offset);
        }
        this.value = (pageId << OFFSET_BITS) | (offset & OFFSET_MASK);
    }

    private RowId(long value) {
        this.value = value;
    }

    public long value() {
        return value;
    }

    public long page() {
        return value >>> OFFSET_BITS;
    }

    public int offset() {
        return (int) (value & OFFSET_MASK);
    }

    public static RowId fromLong(long value) {
        return new RowId(value);
    }

    @Override
    public int compareTo(RowId other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RowId rowId = (RowId) obj;
        return value == rowId.value;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(value);
    }

    @Override
    public String toString() {
        return "RowId{page=" + page() + ", offset=" + offset() + "}";
    }
}
