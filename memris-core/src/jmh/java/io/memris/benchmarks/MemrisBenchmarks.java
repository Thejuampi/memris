package io.memris.benchmarks;

import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class MemrisBenchmarks {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private TestEntityRepository repository;
    private long[] ids;

    @Setup(Level.Trial)
    public void setup() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
        repository = arena.createRepository(TestEntityRepository.class);
        ids = new long[100_000];

        // Pre-populate with 100k rows for lookup benchmarks
        for (int i = 0; i < ids.length; i++) {
            TestEntity saved = repository.save(new TestEntity(null, "user" + i, i % 100));
            ids[i] = saved.id;
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    // Insert benchmark with isolated state per invocation
    @State(Scope.Thread)
    public static class InsertState {
        MemrisRepositoryFactory factory;
        MemrisArena arena;
        TestEntityRepository repository;
        TestEntity[] entities;

        @Setup(Level.Invocation)
        public void setup() {
            factory = new MemrisRepositoryFactory();
            arena = factory.createArena();
            repository = arena.createRepository(TestEntityRepository.class);

            entities = new TestEntity[100_000];
            for (int i = 0; i < entities.length; i++) {
                entities[i] = new TestEntity(null, "user" + i, i % 100);
            }
        }

        @TearDown(Level.Invocation)
        public void tearDown() throws Exception {
            if (factory != null) {
                factory.close();
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void insert_100k_rows(InsertState state, Blackhole blackhole) {
        for (TestEntity entity : state.entities) {
            blackhole.consume(state.repository.save(entity));
        }
    }

    @Benchmark
    @OperationsPerInvocation(100000)
    public void lookup_by_id(Blackhole blackhole) {
        for (long id : ids) {
            blackhole.consume(repository.findById(id));
        }
    }

    @Benchmark
    public void scan_all_rows(Blackhole blackhole) {
        blackhole.consume(repository.findAll());
    }

    @Benchmark
    public void count_rows(Blackhole blackhole) {
        blackhole.consume(repository.count());
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
