package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.BeforeEach;

import io.memris.kernel.RowId;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for AbstractTable free-list thread-safety.
 */
class AbstractTableFreeListConcurrencyTest {

    private TestTable table;

    @BeforeEach
    void setUp() {
        table = new TestTable();
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentFreeListAllocation_shouldNeverReturnDuplicateRowIds() throws InterruptedException {
        int threadCount = 4;
        int allocationsPerThread = 100;

        // First, allocate and deallocate some rows to populate free-list
        RowId[] initialRows = new RowId[50];
        for (int i = 0; i < initialRows.length; i++) {
            initialRows[i] = table.allocate();
        }
        // Deallocate half to create free-list entries
        for (int i = 0; i < initialRows.length / 2; i++) {
            table.deallocate(initialRows[i]);
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);
        ConcurrentHashMap<RowId, Integer> rowIdCounts = new ConcurrentHashMap<>();
        AtomicInteger duplicateCount = new AtomicInteger(0);
        AtomicReference<RowId> firstDuplicate = new AtomicReference<>();

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < allocationsPerThread; i++) {
                        RowId rowId = table.allocate();
                        Integer previous = rowIdCounts.put(rowId, 1);
                        if (previous != null) {
                            duplicateCount.incrementAndGet();
                            firstDuplicate.compareAndSet(null, rowId);
                        }
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

        assertThat(new AllocationSummary(completed, duplicateCount.get(), firstDuplicate.get()))
                .usingRecursiveComparison()
                .isEqualTo(new AllocationSummary(true, 0, null));
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentFreeListPushPop_shouldMaintainConsistency() throws InterruptedException {
        int threadCount = 4;
        int operationsPerThread = 200;

        // Pre-allocate rows for deallocation
        RowId[] preAllocated = new RowId[operationsPerThread * threadCount / 2];
        for (int i = 0; i < preAllocated.length; i++) {
            preAllocated[i] = table.allocate();
        }

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    int startIdx = threadId * operationsPerThread / 2;
                    
                    // Alternate between allocate and deallocate
                    for (int i = 0; i < operationsPerThread; i++) {
                        if (i % 2 == 0 && startIdx + i/2 < preAllocated.length) {
                            // Deallocate
                            table.deallocate(preAllocated[startIdx + i/2]);
                        } else {
                            // Allocate
                            RowId rowId = table.allocate();
                            if (rowId == null) {
                                errorCount.incrementAndGet();
                            }
                        }
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

        assertThat(new OperationSummary(completed, errorCount.get())).usingRecursiveComparison()
                .isEqualTo(new OperationSummary(true, 0));
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
            return allocateRowId();
        }

        void deallocate(RowId rowId) {
            // Convert RowId back to index
            int index = (int) (rowId.page() * PAGE_SIZE + rowId.offset());
            deallocateRowId(index);
        }
    }

    private record AllocationSummary(boolean completed, int duplicateCount, RowId firstDuplicate) {
    }

    private record OperationSummary(boolean completed, int errorCount) {
    }
}
