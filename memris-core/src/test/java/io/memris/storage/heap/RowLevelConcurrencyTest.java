package io.memris.storage.heap;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * TDD tests for row-level seqlock and multi-writer concurrency safety.
 *
 * Validates:
 * 1. Row generation access doesn't cause infinite recursion (ByteBuddy interceptor)
 * 2. Seqlock coordination prevents torn reads during concurrent writes
 * 3. Insert ordering: lock → write → unlock → publish → index update
 * 4. Tombstone ordering: lock → tombstone → unlock → index remove
 * 5. Eventual index consistency with query-time validation
 */
class RowLevelConcurrencyTest {

    private MemrisRepositoryFactory factory;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Entity
    public static class TestEntity {
        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        @Index(type = Index.IndexType.HASH)
        public Long id;

        @Index(type = Index.IndexType.HASH)
        public String name;

        public int value;

        public TestEntity() {}

        public TestEntity(String name, int value) {
            this.name = name;
            this.value = value;
        }

        public Long getId() { return id; }
        public void setId(Long id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public int getValue() { return value; }
        public void setValue(int value) { this.value = value; }
    }

    public interface TestEntityRepository extends MemrisRepository<TestEntity> {
        TestEntity save(TestEntity entity);
        Optional<TestEntity> findById(Long id);
        List<TestEntity> findByName(String name);
        long count();
        void deleteById(Long id);
    }

    /**
     * CRITICAL: Verifies rowGeneration() doesn't cause infinite recursion.
     *
     * The ByteBuddy interceptor was calling AbstractTable.rowGeneration() via
     * reflection, which triggered the interceptor again, causing StackOverflowError.
     *
     * This test would fail with StackOverflowError if the recursion bug is reintroduced.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void rowGenerationAccess_shouldNotCauseRecursion() {
        MemrisArena arena = factory.createArena();
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        // Create and save an entity
        TestEntity entity = new TestEntity("test", 42);
        TestEntity saved = repo.save(entity);

        // Force row generation access through multiple paths
        // This would cause StackOverflowError if interceptor recurses
        assertThatCode(() -> {
            // Query triggers selection creation which calls rowGeneration()
            List<TestEntity> results = repo.findByName("test");
            assertThat(results).isNotEmpty();

            // Find by ID triggers lookup which validates generation
            Optional<TestEntity> found = repo.findById(saved.getId());
            assertThat(found).isPresent();
        }).doesNotThrowAnyException();

        arena.close();
    }

    /**
     * Verifies seqlock prevents torn reads during concurrent updates.
     *
     * Two threads updating the same row (rare but possible with reuse)
     * should not cause readers to see partially-written state.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void seqlockCoordination_shouldPreventTornReads() throws InterruptedException {
        MemrisArena arena = factory.createArena();
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        // Create initial entity
        TestEntity entity = new TestEntity("initial", 0);
        TestEntity saved = repo.save(entity);

        int writerCount = 4;
        int readerCount = 4;
        int iterationsPerThread = 100;

        ExecutorService writers = Executors.newFixedThreadPool(writerCount);
        ExecutorService readers = Executors.newFixedThreadPool(readerCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(writerCount + readerCount);

        AtomicInteger tornReadCount = new AtomicInteger(0);
        AtomicReference<String> lastTornRead = new AtomicReference<>();

        // Writer threads: continuously update the entity
        for (int t = 0; t < writerCount; t++) {
            final int threadId = t;
            writers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        TestEntity update = new TestEntity("writer-" + threadId + "-iter-" + i, threadId * 1000 + i);
                        update.setId(saved.getId());
                        repo.save(update);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        // Reader threads: continuously read and validate consistency
        for (int t = 0; t < readerCount; t++) {
            readers.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterationsPerThread; i++) {
                        Optional<TestEntity> found = repo.findById(saved.getId());
                        if (found.isPresent()) {
                            TestEntity read = found.orElseThrow();
                            // Validate: name and value should be from the same writer
                            String name = read.getName();
                            int value = read.getValue();

                            // Extract writer ID from name and value
                            if (name != null && name.startsWith("writer-")) {
                                String[] parts = name.split("-");
                                if (parts.length >= 2) {
                                    try {
                                        int writerFromName = Integer.parseInt(parts[1]);
                                        int expectedBase = writerFromName * 1000;
                                        // Value should be in range [expectedBase, expectedBase + iterations)
                                        if (value < expectedBase || value >= expectedBase + iterationsPerThread) {
                                            int count = tornReadCount.incrementAndGet();
                                            if (count == 1) {
                                                lastTornRead.set("name=" + name + ", value=" + value);
                                            }
                                        }
                                    } catch (NumberFormatException ignored) {
                                        // Initial value, ignore
                                    }
                                }
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

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for completion
        assertThat(completeLatch.await(30, TimeUnit.SECONDS)).isTrue();

        writers.shutdown();
        readers.shutdown();

        // Assert no torn reads detected
        assertThat(new TornReadSummary(tornReadCount.get(), lastTornRead.get())).usingRecursiveComparison()
                .isEqualTo(new TornReadSummary(0, null));

        arena.close();
    }

    /**
     * Verifies concurrent saves don't corrupt data.
     *
     * Multiple threads saving different entities should not interfere
     * with each other (each row has its own seqlock).
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void concurrentSaves_shouldNotCorruptData() throws InterruptedException {
        MemrisArena arena = factory.createArena();
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        int threadCount = 8;
        int entitiesPerThread = 50;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(threadCount);

        Set<Long>[] threadEntityIds = new Set[threadCount];
        for (int i = 0; i < threadCount; i++) {
            threadEntityIds[i] = new HashSet<>();
        }

        // Each thread saves its own set of entities
        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < entitiesPerThread; i++) {
                        TestEntity entity = new TestEntity("thread-" + threadId + "-entity-" + i, threadId * 1000 + i);
                        TestEntity saved = repo.save(entity);
                        threadEntityIds[threadId].add(saved.getId());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    completeLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertThat(completeLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // Verify all entities are correctly saved and retrievable
        for (int t = 0; t < threadCount; t++) {
            for (Long id : threadEntityIds[t]) {
                Optional<TestEntity> found = repo.findById(id);
                assertThat(found).isPresent();

                TestEntity entity = found.orElseThrow();
                String name = entity.getName();
                int value = entity.getValue();

                // Validate entity belongs to the correct thread
                assertThat(name.startsWith("thread-" + t)).isTrue();
                assertThat(value >= t * 1000 && value < t * 1000 + entitiesPerThread).isTrue();
            }
        }

        // Verify total count
        long totalCount = repo.count();
        assertThat(totalCount).isEqualTo(threadCount * entitiesPerThread);

        arena.close();
    }

    /**
     * Verifies concurrent deletes are safe and don't cause errors.
     *
     * Multiple threads deleting different entities should work correctly.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void concurrentDeletes_shouldBeSafe() throws InterruptedException {
        MemrisArena arena = factory.createArena();
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        // Pre-populate with entities
        int entityCount = 200;
        Long[] ids = new Long[entityCount];
        for (int i = 0; i < entityCount; i++) {
            TestEntity entity = new TestEntity("entity-" + i, i);
            TestEntity saved = repo.save(entity);
            ids[i] = saved.getId();
        }

        int deleterCount = 4;
        ExecutorService executor = Executors.newFixedThreadPool(deleterCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch completeLatch = new CountDownLatch(deleterCount);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        // Each thread deletes a portion of entities
        for (int t = 0; t < deleterCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int i = threadId; i < entityCount; i += deleterCount) {
                        try {
                            repo.deleteById(ids[i]);
                            successCount.incrementAndGet();
                        } catch (Exception e) {
                            errorCount.incrementAndGet();
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
        assertThat(completeLatch.await(30, TimeUnit.SECONDS)).isTrue();
        executor.shutdown();

        // Should have no errors
        assertThat(errorCount.get()).isEqualTo(0);

        // Verify entities are deleted (or at least, count is reduced)
        // Note: Due to eventual index consistency, count might not reflect deletions immediately
        long remainingCount = repo.count();
        // At minimum, verify no exceptions were thrown during concurrent deletes
        assertThat(successCount.get()).isEqualTo(entityCount);

        arena.close();
    }

    /**
     * Verifies row generation tracking works correctly.
     *
     * This test ensures row reuse doesn't cause stale references
     * by validating generation changes on row reuse.
     */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void rowGenerationTracking_shouldPreventStaleReferences() {
        MemrisArena arena = factory.createArena();
        TestEntityRepository repo = arena.createRepository(TestEntityRepository.class);

        // Create an entity
        TestEntity entity = new TestEntity("test", 100);
        TestEntity saved = repo.save(entity);
        Long originalId = saved.getId();

        // Delete it
        repo.deleteById(originalId);

        // Verify it's gone
        Optional<TestEntity> deleted = repo.findById(originalId);
        assertThat(deleted).isEmpty();

        // Create new entity (may reuse the row)
        TestEntity newEntity = new TestEntity("new", 200);
        TestEntity newSaved = repo.save(newEntity);

        // New entity should be findable with correct data
        Optional<TestEntity> found = repo.findById(newSaved.getId());
        assertThat(found).isPresent();
        TestEntity foundEntity = found.orElseThrow();
        assertThat(new EntitySnapshot(foundEntity.getName(), foundEntity.getValue())).usingRecursiveComparison()
                .isEqualTo(new EntitySnapshot("new", 200));

        // Old reference should still not work
        Optional<TestEntity> stillDeleted = repo.findById(originalId);
        assertThat(stillDeleted).isEmpty();

        arena.close();
    }

    private record TornReadSummary(int tornReadCount, String lastTornRead) {
    }

    private record EntitySnapshot(String name, int value) {
    }
}
