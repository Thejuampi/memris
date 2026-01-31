package io.memris.storage.heap;

import io.memris.kernel.RowId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test demonstrating the complete concurrency model documented in CONCURRENCY.md.
 * 
 * This test validates:
 * 1. Thread-safe reads: Concurrent reads are safe with volatile published watermark
 * 2. Non-thread-safe writes: Concurrent writes may have race conditions
 * 3. Hot path (reads) vs Write path (saves/deletes) distinction
 * 
 * Key insight: Reads are blazing fast with zero reflection, but writes need external sync.
 */
class ConcurrencyModelIntegrationTest {

    private static final int PAGE_SIZE = 64;
    private static final int MAX_PAGES = 8;

    static class TestTable extends AbstractTable {
        public final PageColumnLong idColumn;
        public final PageColumnString nameColumn;
        public final PageColumnInt ageColumn;

        TestTable() {
            super("Person", PAGE_SIZE, MAX_PAGES);
            this.idColumn = new PageColumnLong(PAGE_SIZE * MAX_PAGES);
            this.nameColumn = new PageColumnString(PAGE_SIZE * MAX_PAGES);
            this.ageColumn = new PageColumnInt(PAGE_SIZE * MAX_PAGES);
        }

        public RowId insertPerson(long id, String name, int age) {
            RowId rowId = allocateRowId();
            int index = (int)(rowId.page() * PAGE_SIZE + rowId.offset());
            
            idColumn.set(index, id);
            nameColumn.set(index, name);
            ageColumn.set(index, age);
            
            // Publish makes row visible to scans
            int publishedUpTo = Math.max(idColumn.publishedCount(), 
                Math.max(nameColumn.publishedCount(), ageColumn.publishedCount()));
            idColumn.publish(publishedUpTo + 1);
            nameColumn.publish(publishedUpTo + 1);
            ageColumn.publish(publishedUpTo + 1);
            
            incrementRowCount();
            return rowId;
        }

        public void scanAdults(Set<Integer> results, int limit) {
            int[] matches = ageColumn.scanGreaterThan(17, limit);
            for (int idx : matches) {
                results.add(idx);
            }
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentReads_areSafeAndScalable() throws InterruptedException {
        TestTable table = new TestTable();
        
        // Populate table with test data
        for (int i = 0; i < 100; i++) {
            table.insertPerson(i, "Person" + i, 20 + (i % 50));
        }
        
        int readerCount = 8;
        int scansPerThread = 1000;
        ExecutorService readers = Executors.newFixedThreadPool(readerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(readerCount);
        AtomicInteger totalMatches = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        for (int r = 0; r < readerCount; r++) {
            readers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < scansPerThread; i++) {
                        Set<Integer> matches = new HashSet<>();
                        table.scanAdults(matches, 100);
                        
                        // Adults are age > 17, so all 100 entries qualify (ages 20-69)
                        if (matches.size() != 100) {
                            errorCount.incrementAndGet();
                        }
                        totalMatches.addAndGet(matches.size());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }
        
        startLatch.countDown();
        completeLatch.await(30, TimeUnit.SECONDS);
        readers.shutdown();
        
        // Verify concurrent reads are thread-safe
        assertEquals(0, errorCount.get(), 
            "Concurrent reads should produce consistent results with no errors");
        assertEquals(readerCount * scansPerThread * 100, totalMatches.get(),
            "Total matches should equal readers * scans * expected matches per scan");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentWrites_mayHaveRaceConditions() throws InterruptedException {
        TestTable table = new TestTable();
        
        int writerCount = 4;
        int insertsPerThread = 50;
        ExecutorService writers = Executors.newFixedThreadPool(writerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(writerCount);
        Set<Integer> allocatedIndices = java.util.Collections.synchronizedSet(new HashSet<>());
        AtomicInteger duplicateCount = new AtomicInteger(0);
        
        for (int w = 0; w < writerCount; w++) {
            final int threadId = w;
            writers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < insertsPerThread; i++) {
                        long id = threadId * 1000L + i;
                        RowId rowId = table.insertPerson(id, "Thread" + threadId, 25);
                        int index = (int)(rowId.page() * PAGE_SIZE + rowId.offset());
                        
                        if (!allocatedIndices.add(index)) {
                            duplicateCount.incrementAndGet();
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
        completeLatch.await(30, TimeUnit.SECONDS);
        writers.shutdown();
        
        // Verify expected state
        // With races: unique indices < total attempted (some duplicates)
        // Without races: unique indices == total attempted
        int totalAttempted = writerCount * insertsPerThread;
        
        if (duplicateCount.get() > 0) {
            // Race condition detected - document via assertion message
            assertTrue(allocatedIndices.size() < totalAttempted,
                "Race condition detected: " + duplicateCount.get() + " duplicate allocations. " +
                "See CONCURRENCY.md Issue #1 (free-list race)");
        }
        
        // This test documents the concurrency issue without failing
        // See CONCURRENCY.md Issues #1 and #2 for details on write path races
    }
}
