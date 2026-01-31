package io.memris.storage;

/**
 * Selection vector with packed generation tracking.
 * <p>
 * Uses long[] where each element is a packed row reference:
 * <pre>
 * long ref = (gen << 32) | (index & 0xffffffffL)
 * int index = (int) ref;
 * long gen = (ref >>> 32);
 * </pre>
 * <p>
 * <b>Why packed refs?</b>
 * <ul>
 *   <li>Zero allocations in hot path - primitive array only</li>
 *   <li>Generation tracking for stale ref detection</li>
 *   <li>Compatible with existing int[] test infrastructure</li>
 *   <li>Intersection/union operations on long[] (fastest)</li>
 * </ul>
 */
public interface Selection {

    /**
     * Get number of selected rows.
     */
    int size();

    /**
     * Check if packed reference is in selection.
     * <p>
     * O(n) linear search for sparse selections.
     */
    boolean contains(long ref);

    /**
     * Get all packed references.
     */
    long[] toRefArray();

    /**
     * Get row indices (legacy int[] - compatibility).
     * <p>
     * Discards generation information - for materializer that
     * checks generation separately.
     */
    int[] toIntArray();

    /**
     * Create union of two selections.
     * <p>
     * Returns a sorted, deduplicated selection.
     */
    Selection union(Selection other);

    /**
     * Create intersection of two selections.
     * <p>
     * Uses sorted merge for O(n+m).
     */
    Selection intersect(Selection other);

    /**
     * Create difference of two selections (this - other).
     * <p>
     * Returns elements in this selection that are not in other, using O(n+m) merge.
     */
    Selection subtract(Selection other);

    // ===== UTILS: PACKED REF OPERATIONS =====

    /**
     * Extract row index from packed reference.
     */
    static int index(long ref) {
        return (int) ref;
    }

    /**
     * Extract generation from packed reference.
     */
    static long generation(long ref) {
        return ref >>> 32;
    }

    /**
     * Create packed reference from index and generation.
     */
    static long pack(int index, long gen) {
        return (gen << 32) | (index & 0xffffffffL);
    }

    /**
     * Check if generation matches packed reference.
     */
    static boolean isSameGen(long ref, long expectedGen) {
        return generation(ref) == expectedGen;
    }
}
