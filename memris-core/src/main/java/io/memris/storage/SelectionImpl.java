package io.memris.storage;

import java.util.Arrays;

/**
 * Sparse selection implementation backed by long[] packed refs.
 * <p>
 * <b>Performance characteristics:</b>
 * <ul>
 *   <li>contains(): O(log n) binary search (sorted refs)</li>
 *   <li>intersect(): O(n+m) merge</li>
 *   <li>union(): O(n+m) merge with dedupe</li>
 *   <li>subtract(): O(n+m) merge</li>
 * </ul>
 */
public final class SelectionImpl implements Selection {

    private final long[] refs;

    public SelectionImpl(long[] refs) {
        this.refs = normalize(refs);
    }

    @Override
    public int size() {
        return refs.length;
    }

    @Override
    public boolean contains(long ref) {
        int lo = 0;
        int hi = refs.length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            long value = refs[mid];
            if (value < ref) {
                lo = mid + 1;
            } else if (value > ref) {
                hi = mid - 1;
            } else {
                return true;
            }
        }
        return false;
    }

    @Override
    public long[] toRefArray() {
        return refs;
    }

    @Override
    public int[] toIntArray() {
        var result = new int[refs.length];
        for (var i = 0; i < refs.length; i++) {
            result[i] = Selection.index(refs[i]);
        }
        return result;
    }

    @Override
    public Selection union(Selection other) {
        var a = ensureSorted(refs);
        var b = ensureSorted(other.toRefArray());

        if (a.length == 0) {
            return new SelectionImpl(b);
        }
        if (b.length == 0) {
            return new SelectionImpl(a);
        }

        var combined = new long[a.length + b.length];
        var i = 0;
        var j = 0;
        var k = 0;

        while (i < a.length && j < b.length) {
            var va = a[i];
            var vb = b[j];
            if (va == vb) {
                combined[k++] = va;
                i++;
                j++;
            } else if (va < vb) {
                combined[k++] = va;
                i++;
            } else {
                combined[k++] = vb;
                j++;
            }
        }

        while (i < a.length) {
            combined[k++] = a[i++];
        }
        while (j < b.length) {
            combined[k++] = b[j++];
        }

        return new SelectionImpl(trim(combined, k));
    }

    @Override
    public Selection intersect(Selection other) {
        var a = ensureSorted(refs);
        var b = ensureSorted(other.toRefArray());

        if (a.length == 0 || b.length == 0) {
            return new SelectionImpl(new long[0]);
        }

        var result = new long[Math.min(a.length, b.length)];
        var i = 0;
        var j = 0;
        var k = 0;

        while (i < a.length && j < b.length) {
            var va = a[i];
            var vb = b[j];
            if (va == vb) {
                result[k++] = va;
                i++;
                j++;
            } else if (va < vb) {
                i++;
            } else {
                j++;
            }
        }

        return new SelectionImpl(trim(result, k));
    }

    @Override
    public Selection subtract(Selection other) {
        var a = ensureSorted(refs);
        var b = ensureSorted(other.toRefArray());

        if (a.length == 0) {
            return new SelectionImpl(new long[0]);
        }
        if (b.length == 0) {
            return new SelectionImpl(a);
        }

        var result = new long[a.length];
        var i = 0;
        var j = 0;
        var k = 0;

        while (i < a.length && j < b.length) {
            var va = a[i];
            var vb = b[j];
            if (va == vb) {
                i++;
                j++;
            } else if (va < vb) {
                result[k++] = va;
                i++;
            } else {
                j++;
            }
        }

        while (i < a.length) {
            result[k++] = a[i++];
        }

        return new SelectionImpl(trim(result, k));
    }

    private static long[] ensureSorted(long[] refs) {
        if (refs.length < 2) {
            return refs;
        }
        for (var i = 1; i < refs.length; i++) {
            if (refs[i] < refs[i - 1]) {
                var copy = refs.clone();
                Arrays.sort(copy);
                return copy;
            }
        }
        return refs;
    }

    private static long[] normalize(long[] refs) {
        return ensureSorted(refs);
    }

    private static long[] trim(long[] refs, int length) {
        if (length == refs.length) {
            return refs;
        }
        if (length == 0) {
            return new long[0];
        }
        var result = new long[length];
        System.arraycopy(refs, 0, result, 0, length);
        return result;
    }
}
