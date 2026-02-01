package io.memris.storage;

/**
 * Low-level table interface for ByteBuddy-generated entity tables.
 * <p>
 * <b>Critical design principles:</b>
 * <ul>
 *   <li>ALL scans return int[] (rowIndex) - compatible con SelectionVector existente</li>
 *   <li>ALL reads are typed (readLong, readInt, readString) - no boxing</li>
 *   <li>NO materialization of domain objects T</li>
 *   <li>Generation tracking via internal long[] rowGenerations</li>
 *   <li>Seqlock per-row for concurrent write safety</li>
 * </ul>
 */
public interface GeneratedTable {

    // ===== METADATA =====

    int columnCount();
    byte typeCodeAt(int columnIndex);
    long allocatedCount();
    long liveCount();

    // ===== SEQLOCK =====

    /**
     * Read a value with seqlock validation.
     * Retries if the seqlock changes during the read (indicating concurrent write).
     *
     * @param rowIndex the row index
     * @param reader the function to read the value
     * @param <T> the value type
     * @return the consistently read value
     */
    <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader);
    
    // ===== PRIMARY KEY INDEX =====
    
    /**
     * Look up primary key ID, returns packed ref (with generation).
     * <p>
     * Returns -1 if not found.
     */
    long lookupById(long id);
    
    /**
     * Look up primary key ID for String IDs.
     */
    long lookupByIdString(String id);
    
    /**
     * Remove entry from primary key index.
     */
    void removeById(long id);
    
    // ===== ROW LIFECYCLE =====
    
    /**
     * Insert row with values, returns packed ref.
     * <p>
     * Protocol:
     * <ol>
     *   <li>Reserve row index (allocate or reuse)</li>
     *   <li>Begin seqlock (version = odd)</li>
     *   <li>Write all column values</li>
     *   <li>End seqlock (version = even, publish)</li>
     *   <li>Update primary key index with generation</li>
     * </ol>
     */
    long insertFrom(Object[] values);
    
    /**
     * Mark row as tombstoned, add to free-list with current generation.
     */
    void tombstone(long ref);
    
    /**
     * Check if row is live (not tombstoned + generation matches).
     */
    boolean isLive(long ref);
    
    /**
     * Get current generation counter.
     */
    long currentGeneration();

    /**
     * Get generation for a specific row index.
     */
    long rowGeneration(int rowIndex);
    
    // ===== TYPED SCANS (HOT PATH - int[] rowIndex, primitivo) =====
    
    /**
     * Scan long column for equality.
     * <p>
     * Returns int[] of row indices (NOT packed refs).
     * Compatible with SelectionVector.toIntArray().
     */
    int[] scanEqualsLong(int columnIndex, long value);
    
    /**
     * Scan int column for equality.
     */
    int[] scanEqualsInt(int columnIndex, int value);
    
    /**
     * Scan String column for equality (case-sensitive).
     */
    int[] scanEqualsString(int columnIndex, String value);
    
    /**
     * Scan String column for equality (case-insensitive).
     */
    int[] scanEqualsStringIgnoreCase(int columnIndex, String value);
    
    /**
     * Scan int/long column for BETWEEN.
     */
    int[] scanBetweenInt(int columnIndex, int min, int max);
    int[] scanBetweenLong(int columnIndex, long min, long max);
    
    /**
     * Scan column for IN collection.
     */
    int[] scanInLong(int columnIndex, long[] values);
    int[] scanInInt(int columnIndex, int[] values);
    int[] scanInString(int columnIndex, String[] values);
    
    /**
     * Scan all non-tombstoned rows.
     *
     * @return int[] of row indices
     */
    int[] scanAll();
    
    // ===== TYPED READS (HOT PATH - direct primitive access + seqlock) =====
    
    /**
     * Read long value from row index.
     * <p>
     * Checks seqlock: if version is odd (writing), retry.
     */
    long readLong(int columnIndex, int rowIndex);
    
    /**
     * Read int value from row index.
     */
    int readInt(int columnIndex, int rowIndex);
    
    /**
     * Read String value from row index.
     */
    String readString(int columnIndex, int rowIndex);

    /**
     * Check if value is present (non-null) at row index.
     */
    boolean isPresent(int columnIndex, int rowIndex);
}
