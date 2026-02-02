package io.memris.storage.heap;

import io.memris.kernel.RowId;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

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
    private final AtomicInteger allocatedPages;
    private final AtomicLong nextRowId;
    private final AtomicInteger liveRowCount;
    private final AtomicReferenceArray<RowMetaPage> rowMetaPages;

    // Lock-free free-list for row reuse (O(1) allocation)
    private final LockFreeFreeList freeList = new LockFreeFreeList();

    // Generation tracking for stale ref detection
    private final AtomicLong globalGeneration = new AtomicLong(1);

    private static final class RowMetaPage {
        private final long[] generations;
        private final AtomicIntegerArray tombstones;
        private final AtomicLongArray seqLocks;

        private RowMetaPage(int pageSize) {
            this.generations = new long[pageSize];
            this.tombstones = new AtomicIntegerArray(pageSize);
            this.seqLocks = new AtomicLongArray(pageSize);
        }
    }

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
        if (pageSize > 65535) {
            throw new IllegalArgumentException("pageSize exceeds max offset size: " + pageSize);
        }
        if (maxPages <= 0) {
            throw new IllegalArgumentException("maxPages must be positive: " + maxPages);
        }
        var capacity = (long) pageSize * (long) maxPages;
        if (capacity > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("capacity exceeds Integer.MAX_VALUE: " + capacity);
        }
        this.name = name;
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.nextRowId = new AtomicLong(0);
        this.liveRowCount = new AtomicInteger(0);
        this.allocatedPages = new AtomicInteger(0);
        this.rowMetaPages = new AtomicReferenceArray<>(maxPages);
    }

    /**
     * Create an AbstractTable with an initial page allocation.
     *
     * @param name          table name
     * @param pageSize      page size (must be positive)
     * @param maxPages      maximum pages (must be positive)
     * @param initialPages  initial pages to allocate (must be positive and <= maxPages)
     */
    protected AbstractTable(String name, int pageSize, int maxPages, int initialPages) {
        this(name, pageSize, maxPages);
        if (initialPages <= 0) {
            throw new IllegalArgumentException("initialPages must be positive: " + initialPages);
        }
        if (initialPages > maxPages) {
            throw new IllegalArgumentException("initialPages exceeds maxPages: " + initialPages);
        }
        this.allocatedPages.set(initialPages);
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
     * Get currently allocated pages.
     */
    public int allocatedPages() {
        return allocatedPages.get();
    }

    private void validateRowIndex(int rowIndex) {
        if (rowIndex < 0) {
            throw new IndexOutOfBoundsException("rowIndex must be non-negative: " + rowIndex);
        }
        var pageId = rowIndex / pageSize;
        if (pageId >= maxPages) {
            throw new IndexOutOfBoundsException("rowIndex exceeds maxPages: " + rowIndex);
        }
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
        // Try lock-free free-list first (O(1) pop)
        var reusedRowId = freeList.pop();
        if (reusedRowId >= 0) {
            var generation = globalGeneration.incrementAndGet();
            ensurePageAllocated(reusedRowId);
            setRowGenerationAt(reusedRowId, generation);
            clearTombstoneInternal(reusedRowId);
            var pageId = reusedRowId / pageSize;
            var offset = reusedRowId % pageSize;
            return new RowId(pageId, offset);
        }

        // Monotonic allocation
        var rowId = nextRowId.getAndIncrement();
        if (rowId >= capacity()) {
            throw new IllegalStateException("Table capacity exceeded: " + capacity());
        }
        ensurePageAllocated((int) rowId);
        var generation = globalGeneration.incrementAndGet();
        setRowGenerationAt((int) rowId, generation);
        var pageId = (int) (rowId / pageSize);
        var offset = (int) (rowId % pageSize);
        return new RowId(pageId, offset);
    }

    private void ensurePageAllocated(int rowIndex) {
        var pageId = rowIndex / pageSize;
        getOrCreateMetaPage(pageId);
        var requiredPages = pageId + 1;
        var currentPages = allocatedPages.get();
        while (requiredPages > currentPages) {
            if (allocatedPages.compareAndSet(currentPages, requiredPages)) {
                return;
            }
            currentPages = allocatedPages.get();
        }
    }

    private RowMetaPage getOrCreateMetaPage(int pageId) {
        if (pageId < 0 || pageId >= maxPages) {
            throw new IllegalStateException("Page exceeds maxPages: " + pageId);
        }
        var existing = rowMetaPages.get(pageId);
        if (existing != null) {
            return existing;
        }
        var created = new RowMetaPage(pageSize);
        if (rowMetaPages.compareAndSet(pageId, null, created)) {
            return created;
        }
        return rowMetaPages.get(pageId);
    }

    private RowMetaPage metaPage(int rowIndex) {
        var pageId = rowIndex / pageSize;
        var page = rowMetaPages.get(pageId);
        if (page == null) {
            page = getOrCreateMetaPage(pageId);
        }
        return page;
    }

    protected long rowGenerationAt(int rowIndex) {
        var page = rowMetaPages.get(rowIndex / pageSize);
        if (page == null) {
            return 0L;
        }
        return page.generations[rowIndex % pageSize];
    }

    private void setRowGenerationAt(int rowIndex, long generation) {
        var offset = rowIndex % pageSize;
        var page = metaPage(rowIndex);
        page.generations[offset] = generation;
    }

    /**
     * Deallocate a row ID and add to lock-free free-list.
     */
    protected void deallocateRowId(int rowId) {
        freeList.push(rowId);
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
        validateRowIndex(rowId);
        return rowGenerationAt(rowId);
    }

    /**
     * Begin seqlock for writing to a row.
     * Increments the seqlock to make it odd (writing in progress).
     *
     * @param rowIndex the row index
     * @return the seqlock value before incrementing
     */
    public long beginSeqLock(int rowIndex) {
        validateRowIndex(rowIndex);
        var spins = 0;
        var seqLocks = metaPage(rowIndex).seqLocks;
        while (true) {
            var offset = rowIndex % pageSize;
            var current = seqLocks.get(offset);
            if ((current & 1L) != 0L) {
                backoff(spins++);
                continue;
            }
            if (seqLocks.compareAndSet(offset, current, current + 1L)) {
                return current;
            }
            backoff(spins++);
        }
    }

    /**
     * End seqlock for writing to a row.
     * Increments the seqlock to make it even (write complete).
     *
     * @param rowIndex the row index
     */
    public void endSeqLock(int rowIndex) {
        validateRowIndex(rowIndex);
        var seqLocks = metaPage(rowIndex).seqLocks;
        var offset = rowIndex % pageSize;
        seqLocks.incrementAndGet(offset);
    }

    /**
     * Read seqlock value for a row.
     *
     * @param rowIndex the row index
     * @return the current seqlock value
     */
    public long getSeqLock(int rowIndex) {
        validateRowIndex(rowIndex);
        var seqLocks = metaPage(rowIndex).seqLocks;
        var offset = rowIndex % pageSize;
        return seqLocks.get(offset);
    }

    /**
     * Read a value with seqlock validation.
     * Retries if the seqlock changes during the read (indicating concurrent write).
     *
     * @param rowIndex the row index
     * @param reader the function to read the value
     * @param <T> the value type
     * @return the consistently read value
     */
    public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) {
        validateRowIndex(rowIndex);
        var seqLocks = metaPage(rowIndex).seqLocks;
        var offset = rowIndex % pageSize;
        while (true) {
            var seqBefore = seqLocks.get(offset);
            if ((seqBefore & 1) == 1) {
                // Writer active, retry
                continue;
            }
            T value = reader.get();
            var seqAfter = seqLocks.get(offset);
            if (seqBefore == seqAfter) {
                return value;
            }
            // Seqlock changed, retry
        }
    }

    private static void backoff(int spins) {
        if (spins < 10) {
            Thread.onSpinWait();
            return;
        }
        if (spins < 20) {
            Thread.yield();
            return;
        }
        LockSupport.parkNanos(1L);
    }

    private void clearTombstoneInternal(int rowId) {
        var tombstonePage = metaPage(rowId).tombstones;
        var offset = rowId % pageSize;
        tombstonePage.set(offset, 0);
    }

    /**
     * Mark a row as deleted (tombstone) with generation validation.
     *
     * @param rowId the row to delete
     * @param generation the expected generation (stale refs rejected)
     * @return true if tombstoned, false if stale generation
     */
    public boolean tombstone(RowId rowId, long generation) {
        var index = toIndex(rowId);
        validateRowIndex(index);
        // Validate generation - reject stale refs
        if (rowGenerationAt(index) != generation) {
            return false;
        }
        var tombstonePage = metaPage(index).tombstones;
        var offset = index % pageSize;
        // CAS loop to ensure exactly one decrement
        while (true) {
            var current = tombstonePage.get(offset);
            if (current != 0) {
                return true; // Already tombstoned
            }
            if (tombstonePage.compareAndSet(offset, 0, 1)) {
                liveRowCount.decrementAndGet();
                deallocateRowId(index);
                return true;
            }
            // CAS failed, retry
        }
    }

    /**
     * Legacy tombstone without generation check (for migration).
     */
    public void tombstone(RowId rowId) {
        var index = toIndex(rowId);
        validateRowIndex(index);
        var tombstonePage = metaPage(index).tombstones;
        var offset = index % pageSize;
        // CAS loop to ensure exactly one decrement
        while (true) {
            var current = tombstonePage.get(offset);
            if (current != 0) {
                return; // Already tombstoned
            }
            if (tombstonePage.compareAndSet(offset, 0, 1)) {
                liveRowCount.decrementAndGet();
                deallocateRowId(index);
                return;
            }
            // CAS failed, retry
        }
    }

    /**
     * Check if a row is tombstoned.
     *
     * @param rowId the row to check
     * @return true if tombstoned
     */
    public boolean isTombstone(RowId rowId) {
        var index = toIndex(rowId);
        validateRowIndex(index);
        var pageId = index / pageSize;
        var page = rowMetaPages.get(pageId);
        if (page == null) {
            return false;
        }
        var offset = index % pageSize;
        return page.tombstones.get(offset) != 0;
    }

    public void clearTombstone(RowId rowId) {
        var index = toIndex(rowId);
        validateRowIndex(index);
        var tombstonePage = metaPage(index).tombstones;
        var offset = index % pageSize;
        // CAS loop to ensure exactly one increment
        while (true) {
            var current = tombstonePage.get(offset);
            if (current == 0) {
                return; // Not tombstoned
            }
            if (tombstonePage.compareAndSet(offset, 1, 0)) {
                liveRowCount.incrementAndGet();
                return;
            }
            // CAS failed, retry
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
        var allocated = nextRowId.get();
        var pageId = (int) (allocated / pageSize);
        var offset = (int) (allocated % pageSize);
        return "page=" + pageId + ", offset=" + offset;
    }
}
