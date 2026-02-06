package io.memris.index;

import java.util.Arrays;

public final class CompositeKey implements Comparable<CompositeKey> {
    private static final Sentinel MIN = new Sentinel(-1);
    private static final Sentinel MAX = new Sentinel(1);

    private final Object[] values;

    private CompositeKey(Object[] values) {
        this.values = values;
    }

    public static CompositeKey of(Object[] values) {
        return new CompositeKey(values.clone());
    }

    /**
     * Lower-bound marker used only for composite range query expansion.
     */
    public static Object minSentinel() {
        return MIN;
    }

    /**
     * Upper-bound marker used only for composite range query expansion.
     */
    public static Object maxSentinel() {
        return MAX;
    }

    @Override
    public int compareTo(CompositeKey other) {
        var length = Math.min(values.length, other.values.length);
        for (var i = 0; i < length; i++) {
            var cmp = comparePart(values[i], other.values[i]);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(values.length, other.values.length);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static int comparePart(Object left, Object right) {
        if (left == right) {
            return 0;
        }
        if (left instanceof Sentinel l) {
            return l.order;
        }
        if (right instanceof Sentinel r) {
            return -r.order;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }
        if (left instanceof Comparable comp) {
            return comp.compareTo(right);
        }
        return left.toString().compareTo(right.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof CompositeKey other)) {
            return false;
        }
        return Arrays.equals(values, other.values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    private record Sentinel(int order) {
    }
}
