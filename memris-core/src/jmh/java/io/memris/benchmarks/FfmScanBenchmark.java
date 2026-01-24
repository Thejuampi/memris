package io.memris.benchmarks;

import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.storage.ffm.FfmTable;

import java.lang.foreign.Arena;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Thread)
@Fork(1)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class FfmScanBenchmark {

    @Param({"100000", "1000000", "10000000"})
    int rowCount;

    private FfmTable table;
    private SelectionVectorFactory factory;

    @Setup(Level.Iteration)
    public void setup() {
        Arena arena = Arena.ofConfined();
        table = new FfmTable("test", arena, List.of(
                new FfmTable.ColumnSpec("id", int.class),
                new FfmTable.ColumnSpec("value", int.class)
        ), rowCount);

        for (int i = 0; i < rowCount; i++) {
            table.insert(i, i % 100);
        }
        factory = SelectionVectorFactory.defaultFactory();
    }

    @Benchmark
    public void scanAll(Blackhole blackhole) {
        SelectionVector result = table.scanAll(factory);
        blackhole.consume(result);
    }

    @Benchmark
    public void scanValue42(Blackhole blackhole) {
        SelectionVector result = table.scanComparison(
                new io.memris.kernel.Predicate.Comparison("value", io.memris.kernel.Predicate.Operator.EQ, 42),
                factory);
        blackhole.consume(result);
    }
}
