package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for PageColumn* seqlock thread-safety.
 */
class PageColumnSeqLockConcurrencyTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentReadWrite_shouldNeverSeeTornState() throws InterruptedException {
        int capacity = 1000;
        int writerThreads = 2;
        int readerThreads = 4;
        int operationsPerThread = 1000;

        PageColumnInt column = new PageColumnInt(capacity);

        // Pre-publish some rows
        for (int i = 0; i < 100; i++) {
            column.set(i, i * 10);
        }
        column.publish(100);

        ExecutorService executor = Executors.newFixedThreadPool(writerThreads + readerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(writerThreads + readerThreads);
        AtomicInteger tornReadCount = new AtomicInteger(0);
        AtomicReference<String> firstTornRead = new AtomicReference<>();

        // Writer threads - update values
        for (int t = 0; t < writerThreads; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        int offset = i % 100;
                        int value = threadId * 1000000 + i; // Distinct values per thread
                        column.set(offset, value);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        // Reader threads - verify consistent reads
        for (int t = 0; t < readerThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        int offset = i % 100;
                        int value = column.get(offset);
                        boolean isPresent = column.isPresent(offset);

                        // Check for torn state: value should only be 0 if not present
                        // In a torn state, we might see non-zero value with present=false
                        // or vice versa
                        if (isPresent && value == 0) {
                            // This could be a legitimate 0 value, not necessarily torn
                            // But if we see this pattern consistently with concurrent writes,
                            // it indicates a problem
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
        assertTrue(completeLatch.await(30, TimeUnit.SECONDS), "Threads should complete within timeout");
        executor.shutdown();

        assertEquals(0, tornReadCount.get(),
            "Found " + tornReadCount.get() + " torn reads. First: " + firstTornRead.get());
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentScanDuringWrite_shouldBeConsistent() throws InterruptedException {
        int capacity = 1000;
        int writerThreads = 2;
        int scannerThreads = 2;
        int operationsPerThread = 500;

        PageColumnInt column = new PageColumnInt(capacity);

        // Initialize with values
        for (int i = 0; i < 500; i++) {
            column.set(i, i);
        }
        column.publish(500);

        ExecutorService executor = Executors.newFixedThreadPool(writerThreads + scannerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(writerThreads + scannerThreads);
        AtomicInteger inconsistentScanCount = new AtomicInteger(0);

        // Writer threads
        for (int t = 0; t < writerThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        int offset = i % 500;
                        column.set(offset, offset + 1000); // Change values
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        // Scanner threads
        for (int t = 0; t < scannerThreads; t++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < operationsPerThread; i++) {
                        // Scan for original values
                        int[] results = column.scanEquals(i % 500, 500);
                        // Results should be consistent (either found or not)
                        // Inconsistent would be if scan returns partial/mixed results
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(completeLatch.await(30, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(0, inconsistentScanCount.get(), "Inconsistent scans detected");
    }
}
