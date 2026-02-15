package io.memris.runtime.codegen;

import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class RuntimeExecutorGeneratorConcurrencyTest {

    @Test
    void shouldReuseFieldReaderForSameKeyUnderConcurrency() throws Exception {
        var generator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        generator.clearCache();

        int threads = 12;
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
                        instances.add(generator.generateFieldValueReader(2, TypeCodes.TYPE_INT, null));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        }

        assertThat((Set<?>) instances).hasSize(1);
    }

    @Test
    void shouldNotShareReadersAcrossGeneratorInstances() {
        var first = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        var second = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());

        var readerA = first.generateFieldValueReader(1, TypeCodes.TYPE_LONG, null);
        var readerB = second.generateFieldValueReader(1, TypeCodes.TYPE_LONG, null);

        assertThat(readerA).isNotSameAs(readerB);
    }
}
