package io.memris.storage;

import java.util.ArrayList;

/**
 * Sparse selection implementation backed by long[] packed refs.
 * <p>
 * <b>Performance characteristics:</b>
 * <ul>
 *   <li>contains(): O(n) linear search</li>
 *   <li>intersect(): O(n*m) naive implementation</li>
 *   <li>union(): O(n+m) concatenation</li>
 *   <li>Zero allocations in hot path</li>
 * </ul>
 */
public final class SelectionImpl implements Selection {

    private final long[] refs;

    public SelectionImpl(long[] refs) {
        this.refs = refs;
    }

    @Override
    public int size() {
        return refs.length;
    }

    @Override
    public boolean contains(long ref) {
        // Linear search (acceptable for sparse selections)
        for (long r : refs) {
            if (r == ref) {
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
        int[] result = new int[refs.length];
        for (int i = 0; i < refs.length; i++) {
            result[i] = Selection.index(refs[i]);
        }
        return result;
    }

    @Override
    public Selection union(Selection other) {
        long[] otherRefs = other.toRefArray();
        long[] combined = new long[refs.length + otherRefs.length];
        
        System.arraycopy(refs, 0, combined, 0, refs.length);
        System.arraycopy(otherRefs, 0, combined, refs.length, otherRefs.length);
        
        return new SelectionImpl(combined);
    }

    @Override
    public Selection intersect(Selection other) {
        long[] otherRefs = other.toRefArray();
        
        ArrayList<Long> intersection = new ArrayList<>();
        
        for (long r : refs) {
            for (long o : otherRefs) {
                if (r == o) {
                    intersection.add(r);
                    break;
                }
            }
        }
        
        long[] result = new long[intersection.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = intersection.get(i);
        }
        
        return new SelectionImpl(result);
    }
}
