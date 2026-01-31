package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.BeforeEach;

import io.memris.kernel.RowId;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for AbstractTable tombstone thread-safety.
 */
class AbstractTableTombstoneConcurrencyTest {

    private TestTable table;

    @BeforeEach
    void setUp() {
        table = new TestTable();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentTombstone_shouldDecrementRowCountExactlyOnce() throws InterruptedException {
        int rowCount = 100;
        int threadCount = 4;

        // Allocate rows
        RowId[] rows = new RowId[rowCount];
        for (int i = 0; i < rowCount; i++) {
            rows[i] = table.allocate();
        }

        // Verify initial count
        assertEquals(rowCount, table.rowCount(), "Initial row count should be " + rowCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);

        // Each thread tries to tombstone all rows
        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (RowId row : rows) {
                        // Get generation for this row
                        int index = (int) (row.page() * 64 + row.offset());
                        long generation = table.rowGeneration(index);
                        table.tombstone(row, generation);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(30, TimeUnit.SECONDS), "Threads should complete within timeout");
        executor.shutdown();

        // After all tombstones, row count should be 0
        assertEquals(0, table.rowCount(), 
            "Row count should be 0 after tombstoning all rows, but was " + table.rowCount());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentTombstoneAndClear_shouldMaintainConsistency() throws InterruptedException {
        int rowCount = 50;
        int threadCount = 4;

        // Allocate rows
        RowId[] rows = new RowId[rowCount];
        for (int i = 0; i < rowCount; i++) {
            rows[i] = table.allocate();
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Half threads tombstone, half clear
        for (int t = 0; t < threadCount; t++) {
            final boolean shouldTombstone = t % 2 == 0;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < 100; i++) {
                        RowId row = rows[i % rows.length];
                        int index = (int) (row.page() * 64 + row.offset());
                        long generation = table.rowGeneration(index);
                        
                        if (shouldTombstone) {
                            table.tombstone(row, generation);
                        } else {
                            table.clearTombstone(row);
                        }
                    }
                } catch (Exception e) {
                    errorCount.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(0, errorCount.get(), "Errors occurred during concurrent operations");
        // Row count should be between 0 and rowCount (inclusive)
        long finalCount = table.rowCount();
        assertTrue(finalCount >= 0 && finalCount <= rowCount,
            "Final row count " + finalCount + " should be between 0 and " + rowCount);
    }

    /**
     * Test table for accessing protected methods.
     */
    static class TestTable extends AbstractTable {
        private static final int PAGE_SIZE = 64;
        private static final int MAX_PAGES = 10;

        TestTable() {
            super("Test", PAGE_SIZE, MAX_PAGES);
        }

        RowId allocate() {
            RowId rowId = allocateRowId();
            incrementRowCount();
            return rowId;
        }

        void deallocate(RowId rowId) {
            int index = (int) (rowId.page() * PAGE_SIZE + rowId.offset());
            deallocateRowId(index);
        }
    }
}
