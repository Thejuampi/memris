package io.memris.storage.heap;

import io.memris.kernel.RowId;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base class for generated tables with typed column storage.
 * <p>
 * Provides common functionality:
 * <ul>
 *   <li>RowId allocation (monotonic append-only)</li>
 *   <li>Page management (column capacity tracking)</li>
 *   <li>Tombstone tracking for deletes</li>
 *   <li>Row count management</li>
 * </ul>
 * <p>
 * Generated tables (e.g., PersonTable) extend this and add:
 * <ul>
 *   <li>Typed columns (PageColumnLong, PageColumnInt, PageColumnString)</li>
 *   <li>Typed ID index (LongIdIndex, etc.)</li>
 *   <li>Domain-specific insert/find methods</li>
 * </ul>
 */
public abstract class AbstractTable {

    private final String name;
    private final int pageSize;
    private final int maxPages;
    private final AtomicLong nextRowId;
    private final AtomicInteger liveRowCount;
    private final java.util.BitSet tombstones;
    
    // Free-list for row reuse (O(1) allocation)
    private int[] freeList = new int[16];
    private int freeListSize = 0;
    
    // Generation tracking for stale ref detection
    private final AtomicLong globalGeneration = new AtomicLong(1);
    private long[] rowGenerations;

    /**
     * Create an AbstractTable.
     *
     * @param name     table name
     * @param pageSize page size (must be positive)
     * @param maxPages maximum pages (must be positive)
     */
    protected AbstractTable(String name, int pageSize, int maxPages) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("name required");
        }
        if (pageSize <= 0) {
            throw new IllegalArgumentException("pageSize must be positive: " + pageSize);
        }
        if (maxPages <= 0) {
            throw new IllegalArgumentException("maxPages must be positive: " + maxPages);
        }
        this.name = name;
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.nextRowId = new AtomicLong(0);
        this.liveRowCount = new AtomicInteger(0);
        this.tombstones = new java.util.BitSet(maxPages * pageSize);
        this.rowGenerations = new long[maxPages * pageSize];
    }

    /**
     * Get table name.
     *
     * @return table name
     */
    public String name() {
        return name;
    }

    /**
     * Get page size.
     *
     * @return page size
     */
    public int pageSize() {
        return pageSize;
    }

    /**
     * Get maximum pages.
     *
     * @return maximum pages
     */
    public int maxPages() {
        return maxPages;
    }

    /**
     * Get total capacity.
     *
     * @return total capacity (pageSize * maxPages)
     */
    public int capacity() {
        return pageSize * maxPages;
    }

    /**
     * Allocate next RowId with free-list reuse.
     * <p>
     * First tries to reuse from free-list (O(1) pop).
     * If free-list empty, allocates monotonically.
     *
     * @return the allocated RowId
     * @throws IllegalStateException if capacity exceeded
     */
    protected RowId allocateRowId() {
        // Try free-list first (O(1) pop from end)
        if (freeListSize > 0) {
            int reusedRowId = freeList[--freeListSize];
            long generation = globalGeneration.incrementAndGet();
            rowGenerations[reusedRowId] = generation;
            clearTombstoneInternal(reusedRowId);
            int pageId = reusedRowId / pageSize;
            int offset = reusedRowId % pageSize;
            return new RowId(pageId, offset);
        }
        
        // Monotonic allocation
        long rowId = nextRowId.getAndIncrement();
        if (rowId >= capacity()) {
            throw new IllegalStateException("Table capacity exceeded: " + capacity());
        }
        long generation = globalGeneration.incrementAndGet();
        rowGenerations[(int) rowId] = generation;
        int pageId = (int) (rowId / pageSize);
        int offset = (int) (rowId % pageSize);
        return new RowId(pageId, offset);
    }
    
    /**
     * Deallocate a row ID and add to free-list.
     */
    protected void deallocateRowId(int rowId) {
        if (freeListSize >= freeList.length) {
            freeList = java.util.Arrays.copyOf(freeList, freeList.length * 2);
        }
        freeList[freeListSize++] = rowId;
    }
    
    /**
     * Get current global generation.
     */
    public long currentGeneration() {
        return globalGeneration.get();
    }
    
    /**
     * Get generation for a specific row.
     */
    public long rowGeneration(int rowId) {
        return rowGenerations[rowId];
    }
    
    private void clearTombstoneInternal(int rowId) {
        if (tombstones.get(rowId)) {
            tombstones.clear(rowId);
        }
    }

    /**
     * Mark a row as deleted (tombstone) with generation validation.
     *
     * @param rowId the row to delete
     * @param generation the expected generation (stale refs rejected)
     * @return true if tombstoned, false if stale generation
     */
    public boolean tombstone(RowId rowId, long generation) {
        int index = toIndex(rowId);
        // Validate generation - reject stale refs
        if (rowGenerations[index] != generation) {
            return false;
        }
        if (!tombstones.get(index)) {
            tombstones.set(index);
            liveRowCount.decrementAndGet();
            deallocateRowId(index);
        }
        return true;
    }
    
    /**
     * Legacy tombstone without generation check (for migration).
     */
    public void tombstone(RowId rowId) {
        int index = toIndex(rowId);
        if (!tombstones.get(index)) {
            tombstones.set(index);
            liveRowCount.decrementAndGet();
            deallocateRowId(index);
        }
    }

    /**
     * Check if a row is tombstoned.
     *
     * @param rowId the row to check
     * @return true if tombstoned
     */
    public boolean isTombstone(RowId rowId) {
        return tombstones.get(toIndex(rowId));
    }

    public void clearTombstone(RowId rowId) {
        int index = toIndex(rowId);
        if (tombstones.get(index)) {
            tombstones.clear(index);
            liveRowCount.incrementAndGet();
        }
    }

    /**
     * Increment live row count (call after insert).
     */
    protected void incrementRowCount() {
        liveRowCount.incrementAndGet();
    }

    /**
     * Get live row count.
     *
     * @return number of non-deleted rows
     */
    public long rowCount() {
        return liveRowCount.get();
    }

    /**
     * Get total allocated row count (including tombstoned).
     *
     * @return total allocated rows
     */
    public long allocatedCount() {
        return nextRowId.get();
    }

    /**
     * Convert RowId to linear index.
     */
    private int toIndex(RowId rowId) {
        return (int) rowId.page() * pageSize + rowId.offset();
    }

    /**
     * Get current allocation position (for debugging).
     *
     * @return current page and offset
     */
    public String allocationPosition() {
        long allocated = nextRowId.get();
        int pageId = (int) (allocated / pageSize);
        int offset = (int) (allocated % pageSize);
        return "page=" + pageId + ", offset=" + offset;
    }
}
