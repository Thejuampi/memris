package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.BeforeEach;

import io.memris.kernel.RowId;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat(table.rowCount()).isEqualTo(rowCount);

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
        var completed = completeLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(new TombstoneSummary(completed, table.rowCount())).usingRecursiveComparison()
                .isEqualTo(new TombstoneSummary(true, 0));
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
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        var completed = completeLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();

        long finalCount = table.rowCount();
        var inRange = finalCount >= 0 && finalCount <= rowCount;
        assertThat(new TombstoneClearSummary(completed, errorCount.get(), inRange))
                .usingRecursiveComparison()
                .isEqualTo(new TombstoneClearSummary(true, 0, true));
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

    private record TombstoneSummary(boolean completed, long finalCount) {
    }

    private record TombstoneClearSummary(boolean completed, int errorCount, boolean countInRange) {
    }
}
