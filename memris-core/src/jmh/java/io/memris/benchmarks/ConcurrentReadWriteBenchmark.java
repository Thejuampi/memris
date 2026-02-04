package io.memris.benchmarks;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
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
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 5, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class ConcurrentReadWriteBenchmark {

    @Param({"100000"})
    public int initialRows;

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private TestEntityRepository repository;
    private long[] ids;
    private final AtomicLong nextId = new AtomicLong(0);

    @Setup(Level.Trial)
    public void setup() {
        int tablePageSize = 4096;
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
        ids = new long[initialRows];

        // Pre-populate
        for (int i = 0; i < initialRows; i++) {
            TestEntity saved = repository.save(new TestEntity(null, "user" + i, i % 100));
            ids[i] = saved.id;
        }
        nextId.set(initialRows);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    // Group: 1 Writer + 1 Reader
    @Group("write1_read1")
    @GroupThreads(1)
    @Benchmark
    public void writer_1thread(Blackhole blackhole) {
        doWrite(blackhole);
    }

    @Group("write1_read1")
    @GroupThreads(1)
    @Benchmark
    public void reader_1thread(Blackhole blackhole) {
        doRead(blackhole);
    }

    // Group: 2 Writers + 2 Readers
    @Group("write2_read2")
    @GroupThreads(2)
    @Benchmark
    public void writer_2threads(Blackhole blackhole) {
        doWrite(blackhole);
    }

    @Group("write2_read2")
    @GroupThreads(2)
    @Benchmark
    public void reader_2threads(Blackhole blackhole) {
        doRead(blackhole);
    }

    // Group: 4 Writers + 4 Readers
    @Group("write4_read4")
    @GroupThreads(4)
    @Benchmark
    public void writer_4threads(Blackhole blackhole) {
        doWrite(blackhole);
    }

    @Group("write4_read4")
    @GroupThreads(4)
    @Benchmark
    public void reader_4threads(Blackhole blackhole) {
        doRead(blackhole);
    }

    // Group: 8 Writers + 8 Readers
    @Group("write8_read8")
    @GroupThreads(8)
    @Benchmark
    public void writer_8threads(Blackhole blackhole) {
        doWrite(blackhole);
    }

    @Group("write8_read8")
    @GroupThreads(8)
    @Benchmark
    public void reader_8threads(Blackhole blackhole) {
        doRead(blackhole);
    }

    // Group: 4 Writers + 8 Readers (read-heavy)
    @Group("write4_read8")
    @GroupThreads(4)
    @Benchmark
    public void writer_4threads_read8(Blackhole blackhole) {
        doWrite(blackhole);
    }

    @Group("write4_read8")
    @GroupThreads(8)
    @Benchmark
    public void reader_8threads_write4(Blackhole blackhole) {
        doRead(blackhole);
    }

    // Group: 8 Writers + 4 Readers (write-heavy)
    @Group("write8_read4")
    @GroupThreads(8)
    @Benchmark
    public void writer_8threads_read4(Blackhole blackhole) {
        doWrite(blackhole);
    }

    @Group("write8_read4")
    @GroupThreads(4)
    @Benchmark
    public void reader_4threads_write8(Blackhole blackhole) {
        doRead(blackhole);
    }

    // Writer workload: 50% insert, 30% update, 20% delete
    private void doWrite(Blackhole blackhole) {
        long id = nextId.getAndIncrement();
        int operation = (int) (id % 100);

        if (operation < 50) {
            // Insert (50%)
            TestEntity saved = repository.save(new TestEntity(null, "writer-" + id, (int)(id % 100)));
            blackhole.consume(saved);
        } else if (operation < 80) {
            // Update (30%)
            int idx = (int) (id % initialRows);
            long existingId = ids[idx];
            if (existingId != 0) {
                TestEntity updated = repository.save(new TestEntity(existingId, "updated-" + id, (int)(id % 100)));
                blackhole.consume(updated);
            }
        } else {
            // Delete and re-insert (20%)
            int idx = (int) (id % initialRows);
            long existingId = ids[idx];
            if (existingId != 0) {
                repository.deleteById(existingId);
                TestEntity saved = repository.save(new TestEntity(null, "repl-" + id, (int)(id % 100)));
                ids[idx] = saved.id;
                blackhole.consume(saved);
            }
        }
    }

    // Reader workload: 60% lookup by ID, 30% count, 10% scan all
    private void doRead(Blackhole blackhole) {
        var threadId = Thread.currentThread().threadId();
        int operation = (int) (threadId % 100);

        if (operation < 60) {
            // Lookup by ID (60%)
            int idx = (int) ((threadId + System.nanoTime()) % initialRows);
            if (idx < 0) idx = -idx;
            long id = ids[idx];
            if (id != 0) {
                blackhole.consume(repository.findById(id));
            }
        } else if (operation < 90) {
            // Count (30%)
            blackhole.consume(repository.count());
        } else {
            // Scan all (10%)
            blackhole.consume(repository.findAll());
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.Main.main(args);
    }
}
