package io.memris.runtime;

import io.memris.core.MemrisConfiguration;
import io.memris.testutil.TestArena;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryStressTest {

    private static final long TARGET_TOTAL_ROWS = 1_000_000L;
    private static final long MIN_TOTAL_ROWS = 100_000L;
    private static final long MAX_TOTAL_ROWS = 300_000L;
    private static final long ESTIMATED_BYTES_PER_ROW = 1024L;
    private static final String TOTAL_ROWS_PROPERTY = "memris.stress.totalRows";
    private static final int UPDATE_DIVISOR = 2;
    private static final int DELETE_DIVISOR = 10;
    private static final int SAMPLE_SIZE = 1000;
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(60);
    private static final Duration POLL_INTERVAL = Duration.ofMillis(200);

    @Test
    @Tag("stress")
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    @DisplayName("Stress: concurrent insert/update/delete on large row set")
    void shouldHandleConcurrentInsertUpdateDeleteOnOneMillionRows() throws InterruptedException {
        var totalRows = resolveTotalRows();
        var config = MemrisConfiguration.builder()
                .enablePrefixIndex(false)
                .enableSuffixIndex(false)
                .build();
        var actual = TestArena.withArena(config, arena -> {
            var repo = arena.createRepository(TestEntityRepository.class);
            var threads = Math.max(8, Runtime.getRuntime().availableProcessors());
            var executor = Executors.newFixedThreadPool(threads);
            try {
                var insertsCompleted = runConcurrentInserts(repo, executor, threads, totalRows);
                var modificationsCompleted = runConcurrentUpdatesAndDeletes(repo, executor, threads, totalRows);

                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);

                var expectedDeleted = countDivisible(totalRows, DELETE_DIVISOR);
                var expectedRemaining = totalRows - expectedDeleted;
                var sampleIds = buildSampleIds(totalRows, SAMPLE_SIZE);
                var pollResult = pollForConsistency(repo, expectedRemaining, sampleIds);

                return new StressSnapshot(
                        insertsCompleted,
                        modificationsCompleted,
                        pollResult.actualCount,
                        expectedRemaining,
                        pollResult.sampleOk,
                        pollResult.timedOut,
                        pollResult.mismatch
                );
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(exception);
            }
        });

        var expectedDeleted = countDivisible(totalRows, DELETE_DIVISOR);
        var expectedRemaining = totalRows - expectedDeleted;
        var expected = new StressSnapshot(true, true, expectedRemaining, expectedRemaining, true, false, null);
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    private boolean runConcurrentInserts(TestEntityRepository repo, ExecutorService executor, int threads, long totalRows)
            throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threads);

        for (int t = 0; t < threads; t++) {
            long start = (totalRows * t) / threads + 1;
            long end = (totalRows * (t + 1)) / threads;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (long id = start; id <= end; id++) {
                        repo.save(new TestEntity(id,
                                "seed-" + id,
                                (int) (id % 100),
                                "dept-" + (id % 10)));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        return completeLatch.await(10, TimeUnit.MINUTES);
    }

    private boolean runConcurrentUpdatesAndDeletes(TestEntityRepository repo, ExecutorService executor, int threads,
            long totalRows)
            throws InterruptedException {
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threads * 2);

        for (int t = 0; t < threads; t++) {
            long start = (totalRows * t) / threads + 1;
            long end = (totalRows * (t + 1)) / threads;

            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (long id = start; id <= end; id++) {
                        if (id % UPDATE_DIVISOR == 0 && id % DELETE_DIVISOR != 0) {
                            repo.save(new TestEntity(id,
                                    "updated-" + id,
                                    (int) (id % 100) + 1000,
                                    "dept-" + (id % 10)));
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });

            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (long id = start; id <= end; id++) {
                        if (id % DELETE_DIVISOR == 0) {
                            repo.deleteById(id);
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
        return completeLatch.await(10, TimeUnit.MINUTES);
    }

    private PollResult pollForConsistency(TestEntityRepository repo, long expectedCount, long[] sampleIds)
            throws InterruptedException {
        long deadline = System.nanoTime() + POLL_TIMEOUT.toNanos();
        long actualCount = -1L;
        boolean sampleOk = false;
        String mismatch = null;

        while (System.nanoTime() < deadline) {
            actualCount = repo.count();
            SampleCheckResult sampleResult = validateSamples(repo, sampleIds);
            sampleOk = sampleResult.ok;
            mismatch = sampleResult.mismatch;
            if (actualCount == expectedCount && sampleOk) {
                return new PollResult(actualCount, sampleOk, false, mismatch);
            }
            Thread.sleep(POLL_INTERVAL.toMillis());
        }

        return new PollResult(actualCount, sampleOk, true, mismatch);
    }

    private SampleCheckResult validateSamples(TestEntityRepository repo, long[] sampleIds) {
        for (long id : sampleIds) {
            boolean deleted = id % DELETE_DIVISOR == 0;
            Optional<TestEntity> found = repo.findById(id);
            if (deleted) {
                if (found.isPresent()) {
                    return new SampleCheckResult(false, "expected deleted id " + id + " to be absent");
                }
                continue;
            }

            if (found.isEmpty()) {
                return new SampleCheckResult(false, "expected id " + id + " to be present");
            }

            TestEntity entity = found.orElseThrow();
            boolean updated = id % UPDATE_DIVISOR == 0 && id % DELETE_DIVISOR != 0;
            String expectedName = updated ? "updated-" + id : "seed-" + id;
            int expectedAge = updated ? (int) (id % 100) + 1000 : (int) (id % 100);
            String expectedDepartment = "dept-" + (id % 10);

            if (!expectedName.equals(entity.name)) {
                return new SampleCheckResult(false, "name mismatch for id " + id + ": " + entity.name);
            }
            if (expectedAge != entity.age) {
                return new SampleCheckResult(false, "age mismatch for id " + id + ": " + entity.age);
            }
            if (!expectedDepartment.equals(entity.department)) {
                return new SampleCheckResult(false, "department mismatch for id " + id + ": " + entity.department);
            }
        }

        return new SampleCheckResult(true, null);
    }

    private long[] buildSampleIds(long totalRows, int sampleSize) {
        int actualSample = (int) Math.min(sampleSize, totalRows);
        long[] samples = new long[actualSample];
        long step = Math.max(1L, totalRows / actualSample);
        long id = 1L;
        for (int i = 0; i < actualSample; i++) {
            samples[i] = id;
            id += step;
            if (id > totalRows) {
                id = totalRows;
            }
        }
        return samples;
    }

    private long countDivisible(long total, int divisor) {
        return total / divisor;
    }

    private long resolveTotalRows() {
        var configured = Long.getLong(TOTAL_ROWS_PROPERTY, -1L);
        if (configured > 0) {
            return configured;
        }
        var maxHeapBytes = Runtime.getRuntime().maxMemory();
        var heapBound = maxHeapBytes / ESTIMATED_BYTES_PER_ROW;
        return Math.max(MIN_TOTAL_ROWS, Math.min(Math.min(TARGET_TOTAL_ROWS, heapBound), MAX_TOTAL_ROWS));
    }

    private record StressSnapshot(
            boolean insertsCompleted,
            boolean modificationsCompleted,
            long actualCount,
            long expectedCount,
            boolean sampleOk,
            boolean timedOut,
            String mismatch
    ) {
    }

    private record PollResult(long actualCount, boolean sampleOk, boolean timedOut, String mismatch) {
    }

    private record SampleCheckResult(boolean ok, String mismatch) {
    }
}
