package io.memris.core;

import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemrisArenaConcurrencyTest {

    @Test
    void shouldCreateSameRepositoryOnlyOnceUnderConcurrentCalls() throws Exception {
        try (var factory = new MemrisRepositoryFactory()) {
            var arena = factory.createArena();
            int threads = 10;
            var ready = new CountDownLatch(threads);
            var start = new CountDownLatch(1);
            var done = new CountDownLatch(threads);
            var instances = ConcurrentHashMap.newKeySet();

            try (var executor = Executors.newFixedThreadPool(threads)) {
                for (int i = 0; i < threads; i++) {
                    executor.submit(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            instances.add(arena.createRepository(PrimaryRepository.class));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }
                ready.await();
                start.countDown();
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            }

            assertThat((Set<?>) instances).hasSize(1);
        }
    }

    @Test
    void shouldCreateDifferentRepositoriesConcurrently() throws Exception {
        try (var factory = new MemrisRepositoryFactory()) {
            var arena = factory.createArena();
            int threads = 12;
            var ready = new CountDownLatch(threads);
            var start = new CountDownLatch(1);
            var done = new CountDownLatch(threads);
            var primaryInstances = ConcurrentHashMap.newKeySet();
            var secondaryInstances = ConcurrentHashMap.newKeySet();

            try (var executor = Executors.newFixedThreadPool(threads)) {
                for (int i = 0; i < threads; i++) {
                    final boolean usePrimary = i % 2 == 0;
                    executor.submit(() -> {
                        ready.countDown();
                        try {
                            start.await();
                            if (usePrimary) {
                                primaryInstances.add(arena.createRepository(PrimaryRepository.class));
                            } else {
                                secondaryInstances.add(arena.createRepository(SecondaryRepository.class));
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            done.countDown();
                        }
                    });
                }
                ready.await();
                start.countDown();
                assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
            }

            assertThat((Set<?>) primaryInstances).hasSize(1);
            assertThat((Set<?>) secondaryInstances).hasSize(1);
        }
    }

    @Test
    void shouldFailFastAfterArenaClose() {
        var factory = new MemrisRepositoryFactory();
        var arena = factory.createArena();
        arena.close();

        assertThatThrownBy(() -> arena.createRepository(PrimaryRepository.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> arena.getOrCreateTable(PrimaryEntity.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(() -> arena.getOrCreateIndexes(PrimaryEntity.class))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("closed");

        factory.close();
    }

    public interface PrimaryRepository extends MemrisRepository<PrimaryEntity> {
    }

    public interface SecondaryRepository extends MemrisRepository<SecondaryEntity> {
    }

    @Entity
    public static class PrimaryEntity {
        @Id
        public Long id;
        public String name;
    }

    @Entity
    public static class SecondaryEntity {
        @Id
        public Long id;
        public String name;
    }
}
