package io.memris.kernel;

/**
 * Immutable view of a set of row IDs.
 * <p>
 * <b>Contract:</b>
 * <ul>
 *   <li>This is a <b>set</b> - no duplicate row IDs.</li>
 *   <li>size() returns the cardinality (number of unique row IDs).</li>
 *   <li>contains() tests membership in O(1) or O(n) depending on implementation.</li>
 *   <li>toLongArray() returns a snapshot copy of all row IDs.</li>
 *   <li>enumerator() provides a lock-free iterator over a snapshot.</li>
 *   <li>Iteration order is unspecified and may vary between implementations.</li>
 * </ul>
 */
public interface RowIdSet {
    /**
     * Returns the number of unique row IDs in this set.
     * @return cardinality (always non-negative)
     */
    int size();

    /**
     * Tests if the given row ID is present in this set.
     * @param rowId the row ID to test
     * @return true if present, false otherwise
     */
    boolean contains(RowId rowId);

    /**
     * Returns a snapshot array of all row IDs in this set.
     * <p>
     * The returned array is a copy and safe to modify.
     * The array length equals size().
     * @return snapshot array of row ID values
     */
    long[] toLongArray();

    /**
     * Returns an enumerator over a snapshot of this set's row IDs.
     * <p>
     * The enumerator iterates over a snapshot taken at creation time.
     * It is safe to use even if the underlying set is modified concurrently.
     * @return lock-free enumerator over row IDs
     */
    LongEnumerator enumerator();
}
