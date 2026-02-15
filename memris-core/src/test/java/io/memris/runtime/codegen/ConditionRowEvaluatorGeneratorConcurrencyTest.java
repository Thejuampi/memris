package io.memris.runtime.codegen;

import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionRowEvaluatorGeneratorConcurrencyTest {

    @Test
    void shouldReuseEvaluatorForSameConditionUnderConcurrency() throws Exception {
        var runtimeGenerator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        var generator = new ConditionRowEvaluatorGenerator(runtimeGenerator);
        generator.clearCache();
        var condition = CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.EQ, 0);

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
                        instances.add(generator.generate(condition, false));
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
    void shouldNotShareEvaluatorAcrossGeneratorInstances() {
        var condition = CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.EQ, 0);

        var runtimeGeneratorA = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        var runtimeGeneratorB = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        var evaluatorGeneratorA = new ConditionRowEvaluatorGenerator(runtimeGeneratorA);
        var evaluatorGeneratorB = new ConditionRowEvaluatorGenerator(runtimeGeneratorB);

        var first = evaluatorGeneratorA.generate(condition, false);
        var second = evaluatorGeneratorB.generate(condition, false);

        assertThat(first).isNotSameAs(second);
    }
}
