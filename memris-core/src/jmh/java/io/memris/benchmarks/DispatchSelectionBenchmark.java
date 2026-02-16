package io.memris.benchmarks;

import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class DispatchSelectionBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private MemrisRepositoryFactory factory;
        private MemrisArena arena;
        private TestEntityRepository repository;

        @Setup(Level.Trial)
        public void setUp() {
            factory = new MemrisRepositoryFactory();
            arena = factory.createArena();
            repository = arena.createRepository(TestEntityRepository.class);
            for (var i = 0; i < 10_000; i++) {
                repository.save(new TestEntity(null, "name-" + (i % 100), i % 100, "dep-" + (i % 10)));
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (factory != null) {
                factory.close();
            }
        }
    }

    @Benchmark
    public int findByName(BenchmarkState state) {
        return state.repository.findByName("name-42").size();
    }

    @Benchmark
    public long countByName(BenchmarkState state) {
        return state.repository.countByName("name-42");
    }

    @Benchmark
    public boolean existsByName(BenchmarkState state) {
        return state.repository.existsByName("name-42");
    }

    @Benchmark
    public int findByAgeIn(BenchmarkState state) {
        return state.repository.findByAgeIn(new int[] { 3, 7, 11, 15 }).size();
    }
}
