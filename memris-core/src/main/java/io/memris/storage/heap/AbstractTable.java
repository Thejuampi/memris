package io.memris.storage.heap;

import io.memris.kernel.RowId;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
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
    private final AtomicIntegerArray[] tombstonePages;
    private final long[][] rowGenerationPages;
    private final AtomicLongArray[] rowSeqLockPages;
    private final Object pageAllocationLock = new Object();

    // Lock-free free-list for row reuse (O(1) allocation)
    private final LockFreeFreeList freeList = new LockFreeFreeList();

    // Generation tracking for stale ref detection
    private final AtomicLong globalGeneration = new AtomicLong(1);

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
        long capacity = (long) pageSize * (long) maxPages;
        if (capacity > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("capacity exceeds Integer.MAX_VALUE: " + capacity);
        }
        this.name = name;
        this.pageSize = pageSize;
        this.maxPages = maxPages;
        this.nextRowId = new AtomicLong(0);
        this.liveRowCount = new AtomicInteger(0);
        this.allocatedPages = new AtomicInteger(0);
        this.tombstonePages = new AtomicIntegerArray[maxPages];
        this.rowGenerationPages = new long[maxPages][];
        this.rowSeqLockPages = new AtomicLongArray[maxPages];
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
        int pageId = rowIndex / pageSize;
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
        int reusedRowId = freeList.pop();
        if (reusedRowId >= 0) {
            long generation = globalGeneration.incrementAndGet();
            ensurePageAllocated(reusedRowId);
            setRowGenerationAt(reusedRowId, generation);
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
        ensurePageAllocated((int) rowId);
        long generation = globalGeneration.incrementAndGet();
        setRowGenerationAt((int) rowId, generation);
        int pageId = (int) (rowId / pageSize);
        int offset = (int) (rowId % pageSize);
        return new RowId(pageId, offset);
    }

    private void ensurePageAllocated(int rowIndex) {
        int pageId = rowIndex / pageSize;
        allocatePage(pageId);
        int requiredPages = pageId + 1;
        int currentPages = allocatedPages.get();
        while (requiredPages > currentPages) {
            if (allocatedPages.compareAndSet(currentPages, requiredPages)) {
                return;
            }
            currentPages = allocatedPages.get();
        }
    }

    private void allocatePage(int pageId) {
        if (pageId < 0 || pageId >= maxPages) {
            throw new IllegalStateException("Page exceeds maxPages: " + pageId);
        }
        if (rowGenerationPages[pageId] != null) {
            return;
        }
        synchronized (pageAllocationLock) {
            if (rowGenerationPages[pageId] == null) {
                rowGenerationPages[pageId] = new long[pageSize];
                tombstonePages[pageId] = new AtomicIntegerArray(pageSize);
                rowSeqLockPages[pageId] = new AtomicLongArray(pageSize);
            }
        }
    }

    private AtomicLongArray rowSeqLockPage(int rowIndex) {
        int pageId = rowIndex / pageSize;
        AtomicLongArray page = rowSeqLockPages[pageId];
        if (page == null) {
            allocatePage(pageId);
            page = rowSeqLockPages[pageId];
        }
        return page;
    }

    private AtomicIntegerArray tombstonePage(int rowIndex) {
        int pageId = rowIndex / pageSize;
        AtomicIntegerArray page = tombstonePages[pageId];
        if (page == null) {
            allocatePage(pageId);
            page = tombstonePages[pageId];
        }
        return page;
    }

    protected long rowGenerationAt(int rowIndex) {
        int pageId = rowIndex / pageSize;
        long[] page = rowGenerationPages[pageId];
        if (page == null) {
            return 0L;
        }
        return page[rowIndex % pageSize];
    }

    private void setRowGenerationAt(int rowIndex, long generation) {
        int pageId = rowIndex / pageSize;
        int offset = rowIndex % pageSize;
        long[] page = rowGenerationPages[pageId];
        if (page == null) {
            allocatePage(pageId);
            page = rowGenerationPages[pageId];
        }
        page[offset] = generation;
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
        int spins = 0;
        AtomicLongArray seqLocks = rowSeqLockPage(rowIndex);
        while (true) {
            int offset = rowIndex % pageSize;
            long current = seqLocks.get(offset);
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
        AtomicLongArray seqLocks = rowSeqLockPage(rowIndex);
        int offset = rowIndex % pageSize;
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
        AtomicLongArray seqLocks = rowSeqLockPage(rowIndex);
        int offset = rowIndex % pageSize;
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
        AtomicLongArray seqLocks = rowSeqLockPage(rowIndex);
        int offset = rowIndex % pageSize;
        while (true) {
            long seqBefore = seqLocks.get(offset);
            if ((seqBefore & 1) == 1) {
                // Writer active, retry
                continue;
            }
            T value = reader.get();
            long seqAfter = seqLocks.get(offset);
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
        AtomicIntegerArray tombstonePage = tombstonePage(rowId);
        int offset = rowId % pageSize;
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
        int index = toIndex(rowId);
        validateRowIndex(index);
        // Validate generation - reject stale refs
        if (rowGenerationAt(index) != generation) {
            return false;
        }
        AtomicIntegerArray tombstonePage = tombstonePage(index);
        int offset = index % pageSize;
        // CAS loop to ensure exactly one decrement
        while (true) {
            int current = tombstonePage.get(offset);
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
        int index = toIndex(rowId);
        validateRowIndex(index);
        AtomicIntegerArray tombstonePage = tombstonePage(index);
        int offset = index % pageSize;
        // CAS loop to ensure exactly one decrement
        while (true) {
            int current = tombstonePage.get(offset);
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
        int index = toIndex(rowId);
        validateRowIndex(index);
        int pageId = index / pageSize;
        AtomicIntegerArray tombstonePage = tombstonePages[pageId];
        if (tombstonePage == null) {
            return false;
        }
        int offset = index % pageSize;
        return tombstonePage.get(offset) != 0;
    }

    public void clearTombstone(RowId rowId) {
        int index = toIndex(rowId);
        validateRowIndex(index);
        AtomicIntegerArray tombstonePage = tombstonePage(index);
        int offset = index % pageSize;
        // CAS loop to ensure exactly one increment
        while (true) {
            int current = tombstonePage.get(offset);
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
        long allocated = nextRowId.get();
        int pageId = (int) (allocated / pageSize);
        int offset = (int) (allocated % pageSize);
        return "page=" + pageId + ", offset=" + offset;
    }
}
