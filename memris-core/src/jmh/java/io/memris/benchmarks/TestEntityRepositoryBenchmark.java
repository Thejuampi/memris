package io.memris.benchmarks;

import io.memris.repository.MemrisArena;
import io.memris.core.MemrisConfiguration;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.ThreadParams;

import java.util.concurrent.TimeUnit;
import java.util.SplittableRandom;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class TestEntityRepositoryBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        @Param({ "750000" })
        public int initialRows;

        @Param({ "750000" })
        public int opsPerInvocation;

        @Param({ "40" })
        public int savePct;

        @Param({ "40" })
        public int updatePct;

        @Param({ "20" })
        public int deletePct;

        private MemrisRepositoryFactory factory;
        private MemrisArena arena;
        private TestEntityRepository repository;
        private long[] prefilledIds;

        @Setup(Level.Trial)
        public void setup() {
            if (savePct + updatePct + deletePct != 100) {
                throw new IllegalStateException("save/update/delete percentages must sum to 100");
            }
            int tablePageSize = 1024;
            int tableMaxPages = 4096;
            int tableInitialPages = 256;
            MemrisConfiguration configuration = MemrisConfiguration.builder()
                    .pageSize(tablePageSize)
                    .maxPages(tableMaxPages)
                    .initialPages(tableInitialPages)
                    .build();
            factory = new MemrisRepositoryFactory(configuration);
            arena = factory.createArena();
            repository = arena.createRepository(TestEntityRepository.class);
            prefilledIds = new long[initialRows];

            for (int i = 0; i < initialRows; i++) {
                TestEntity saved = repository.save(new TestEntity(null, "seed-" + i, i % 100, "dept-" + (i % 10)));
                prefilledIds[i] = saved.id;
            }
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (factory != null) {
                factory.close();
            }
        }
    }

    @State(Scope.Thread)
    public static class ThreadState {
        private long[] idPool;
        private int cursor;
        private SplittableRandom random;

        @Setup(Level.Iteration)
        public void setup(ThreadParams params, BenchmarkState state) {
            int threads = params.getThreadCount();
            int threadIndex = params.getThreadIndex();
            int total = state.prefilledIds.length;
            int chunk = total / threads;
            int start = threadIndex * chunk;
            int end = threadIndex == threads - 1 ? total : start + chunk;
            int size = Math.max(1, end - start);
            idPool = new long[size];
            System.arraycopy(state.prefilledIds, start, idPool, 0, size);
            cursor = 0;
            random = new SplittableRandom(0x9E3779B97F4A7C15L + threadIndex);
        }
    }

    @Benchmark
    public void mixedSaveUpdateDelete(BenchmarkState state, ThreadState threadState) {
        TestEntityRepository repository = state.repository;
        long[] pool = threadState.idPool;
        int poolSize = pool.length;
        int cursor = threadState.cursor;
        SplittableRandom random = threadState.random;

        for (int i = 0; i < state.opsPerInvocation; i++) {
            int roll = random.nextInt(100);
            int slot = cursor++ % poolSize;
            long id = pool[slot];

            if (roll < state.savePct) {
                TestEntity saved = repository.save(new TestEntity(null,
                        "name-" + random.nextInt(),
                        random.nextInt(100),
                        "dept-" + (random.nextInt(10))));
                pool[slot] = saved.id;
                continue;
            }

            if (roll < state.savePct + state.updatePct) {
                if (id == 0L) {
                    TestEntity saved = repository.save(new TestEntity(null,
                            "name-" + random.nextInt(),
                            random.nextInt(100),
                            "dept-" + (random.nextInt(10))));
                    pool[slot] = saved.id;
                } else {
                    repository.save(new TestEntity(id,
                            "name-" + random.nextInt(),
                            random.nextInt(100),
                            "dept-" + (random.nextInt(10))));
                }
                continue;
            }

            if (id == 0L) {
                TestEntity saved = repository.save(new TestEntity(null,
                        "name-" + random.nextInt(),
                        random.nextInt(100),
                        "dept-" + (random.nextInt(10))));
                pool[slot] = saved.id;
            } else {
                repository.deleteById(id);
                pool[slot] = 0L;
            }
        }

        threadState.cursor = cursor;
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
