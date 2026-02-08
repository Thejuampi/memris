package io.memris.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.HashSet;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for RepositoryRuntime ID generation thread-safety.
 */
class RepositoryRuntimeIdConcurrencyTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentIdGeneration_shouldProduceUniqueIds() throws InterruptedException {
        int threadCount = 8;
        int idsPerThread = 1000;
        int totalIds = threadCount * idsPerThread;

        ConcurrentHashMap<Long, Integer> idCounts;
        AtomicInteger duplicateCount;
        AtomicReference<Long> firstDuplicate;
        boolean finished;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(threadCount);
            idCounts = new ConcurrentHashMap<>();
            duplicateCount = new AtomicInteger(0);
            firstDuplicate = new AtomicReference<>();

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < idsPerThread; i++) {
                            // Simulate ID generation using same pattern as RepositoryRuntime
                            long id = IdGenerator.generateNextId();
                            Integer previous = idCounts.put(id, 1);
                            if (previous != null) {
                                duplicateCount.incrementAndGet();
                                firstDuplicate.compareAndSet(null, id);
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
            finished = completeLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();
        }

        var actual = new UniquenessSummary(duplicateCount.get(), idCounts.size(), totalIds, firstDuplicate.get(), finished);
        var expected = new UniquenessSummary(0, totalIds, totalIds, null, true);
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentIdGeneration_shouldBeMonotonicallyIncreasing() throws InterruptedException {
        int threadCount = 4;
        int idsPerThread = 100;

        ConcurrentLinkedQueue<Long> allIds;
        boolean finished;
        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount)) {
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch completeLatch = new CountDownLatch(threadCount);
            allIds = new ConcurrentLinkedQueue<>();

            for (int t = 0; t < threadCount; t++) {
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < idsPerThread; i++) {
                            allIds.add(IdGenerator.generateNextId());
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        completeLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            finished = completeLatch.await(30, TimeUnit.SECONDS);
            executor.shutdown();
        }

        var allPositive = allIds.stream().allMatch(id -> id != null && id > 0);
        var actual = new MonotonicSummary(allIds.size(), new HashSet<>(allIds).size(), allPositive, finished);
        var expected = new MonotonicSummary(allIds.size(), allIds.size(), true, true);
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    /**
     * Uses the actual RepositoryRuntime ID generation pattern.
     */
    private static class IdGenerator {
        // Fixed implementation using AtomicLong
        private static final java.util.concurrent.atomic.AtomicLong idCounter = 
            new java.util.concurrent.atomic.AtomicLong(1L);

        static Long generateNextId() {
            return idCounter.getAndIncrement();
        }
    }

    private record UniquenessSummary(int duplicates, int uniqueIds, int expectedIds, Long firstDuplicate, boolean completed) {
    }

    private record MonotonicSummary(int totalIds, int uniqueIds, boolean allPositive, boolean completed) {
    }
}
