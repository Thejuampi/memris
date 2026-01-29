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
     * Allocate next RowId (append-only).
     * <p>
     * RowIds are allocated monotonically: page 0 fills first, then page 1, etc.
     *
     * @return the allocated RowId
     * @throws IllegalStateException if capacity exceeded
     */
    protected RowId allocateRowId() {
        long rowId = nextRowId.getAndIncrement();
        if (rowId >= capacity()) {
            throw new IllegalStateException("Table capacity exceeded: " + capacity());
        }
        int pageId = (int) (rowId / pageSize);
        int offset = (int) (rowId % pageSize);
        return new RowId(pageId, offset);
    }

    /**
     * Mark a row as deleted (tombstone).
     *
     * @param rowId the row to delete
     */
    public void tombstone(RowId rowId) {
        int index = toIndex(rowId);
        if (!tombstones.get(index)) {
            tombstones.set(index);
            liveRowCount.decrementAndGet();
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
