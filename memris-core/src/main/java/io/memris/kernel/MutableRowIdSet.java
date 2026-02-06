package io.memris.kernel;

/**
 * Mutable set of row IDs.
 * <p>
 * <b>Contract:</b>
 * <ul>
 *   <li>This is a <b>set</b> - no duplicate row IDs. add() is idempotent.</li>
 *   <li>remove() is idempotent - no-op when value absent.</li>
 *   <li>size() returns the cardinality (unique count).</li>
 *   <li>All read operations inherited from RowIdSet remain thread-safe.</li>
 *   <li>Iteration order is unspecified.</li>
 * </ul>
 * <p>
 * Implementations must ensure that readers see consistent snapshots
 * even during concurrent modifications. Writers are typically serialized
 * by the containing index (e.g., ConcurrentHashMap.compute).
 */
public interface MutableRowIdSet extends RowIdSet {
    /**
     * Adds a row ID to this set.
     * <p>
     * If the row ID is already present, this is a no-op (idempotent).
     * @param rowId the row ID to add
     * @throws IllegalArgumentException if rowId is null
     */
    void add(RowId rowId);

    /**
     * Removes a row ID from this set.
     * <p>
     * If the row ID is not present, this is a no-op (idempotent).
     * @param rowId the row ID to remove
     */
    void remove(RowId rowId);
}
