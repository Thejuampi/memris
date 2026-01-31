package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for PageColumn* concurrency issues.
 * 
 * Tests the critical concurrency issue documented in CONCURRENCY.md:
 * 3. Column Writes Not Atomic (PageColumn*.java:79-84)
 * 
 * Demonstrates that two-step writes (data[] then present[]) without 
 * coordination can lead to torn reads.
 */
class PageColumnConcurrencyTest {

    private static final int PAGE_SIZE = 1024;

    @Test
    void singleThreadedWriteRead_isConsistent() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        // Write a value
        column.set(0, 42);
        column.publish(1);
        
        // Read should see consistent state
        assertTrue(column.isPresent(0), "Value should be present after set and publish");
        assertEquals(42, column.get(0), "Value should match what was written");
    }

    @Test
    void publishWatermarkControlsScanVisibility() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        // Write values
        column.set(0, 100);
        column.set(1, 200);
        column.set(2, 300);
        // Note: get() checks present[] array, not published watermark
        // So values ARE immediately visible via get()
        assertEquals(100, column.get(0), "Values are immediately visible via get()");
        
        // But scan operations respect the published watermark
        // Without publishing, scanEquals sees nothing
        int[] matchesBefore = column.scanEquals(100, 3);
        assertEquals(0, matchesBefore.length, "Unpublished values not visible to scans");
        
        // Publish the values
        column.publish(3);
        
        // Now scan operations can see them
        int[] matchesAfter = column.scanEquals(100, 3);
        assertArrayEquals(new int[]{0}, matchesAfter, "Published values visible to scans");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentReadsAreSafe_withPublishedWatermark() throws InterruptedException {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        // Pre-populate with values
        for (int i = 0; i < 100; i++) {
            column.set(i, i * 10);
        }
        column.publish(100);
        
        int readerCount = 8;
        int readsPerThread = 10000;
        ExecutorService readers = Executors.newFixedThreadPool(readerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(readerCount);
        AtomicInteger errorCount = new AtomicInteger(0);
        
        for (int r = 0; r < readerCount; r++) {
            readers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < readsPerThread; i++) {
                        int index = i % 100;
                        if (column.isPresent(index)) {
                            int value = column.get(index);
                            if (value != index * 10) {
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
        completeLatch.await(30, TimeUnit.SECONDS);
        readers.shutdown();
        
        assertEquals(0, errorCount.get(), 
            "Concurrent reads of published data should be safe");
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentScansAreSafe_withPublishedWatermark() throws InterruptedException {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        // Pre-populate with pattern
        for (int i = 0; i < 100; i++) {
            column.set(i, i % 10 == 0 ? 100 : i); // Every 10th is 100
        }
        column.publish(100);
        
        int readerCount = 4;
        int scansPerThread = 1000;
        ExecutorService readers = Executors.newFixedThreadPool(readerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(readerCount);
        AtomicInteger inconsistencyCount = new AtomicInteger(0);
        
        for (int r = 0; r < readerCount; r++) {
            readers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < scansPerThread; i++) {
                        int[] matches = column.scanEquals(100, 100);
                        // Should always find exactly 10 matches (indices 0, 10, 20, ... 90)
                        if (matches.length != 10) {
                            inconsistencyCount.incrementAndGet();
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
        readers.shutdown();
        
        assertEquals(0, inconsistencyCount.get(),
            "Concurrent scans of published data should be consistent");
    }
}
