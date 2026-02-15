package io.memris.runtime;

import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class HeapRuntimeKernelCoverageTest {

    @Test
    void shouldRejectNullRuntimeExecutorGenerator() {
        assertThatThrownBy(() -> new HeapRuntimeKernel(new StubTable(), TypeHandlerRegistry.getDefault(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("runtimeExecutorGenerator");
    }

    @Test
    void shouldExposeTableMetadataAndCustomRegistry() {
        var registry = TypeHandlerRegistry.empty();
        var table = new StubTable()
                .withCounts(11, 7)
                .withColumnType(TypeCodes.TYPE_LONG);
        var kernel = new HeapRuntimeKernel(table, registry);

        assertThat(kernel.rowCount()).isEqualTo(7);
        assertThat(kernel.allocatedCount()).isEqualTo(11);
        assertThat(kernel.columnCount()).isEqualTo(1);
        assertThat(kernel.typeCodeAt(0)).isEqualTo(TypeCodes.TYPE_LONG);
        assertThat(kernel.handlerRegistry()).isSameAs(registry);
    }

    @Test
    void shouldDelegateInAndNotInToConfiguredRuntimeExecutor() {
        long gen0 = 10;
        long gen1 = 11;
        long gen2 = 12;
        var table = new StubTable()
                .withScanAll(new int[] { 0, 1, 2 })
                .withGenerations(new long[] { gen0, gen1, gen2 })
                .withInRows(new int[] { 1 });
        var generator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        var kernel = new HeapRuntimeKernel(table, TypeHandlerRegistry.getDefault(), generator);

        var inCondition = CompiledQuery.CompiledCondition.of(
                0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.IN,
                0);
        var inResult = kernel.executeCondition(inCondition, new Object[] { new int[] { 1, 2 } });
        assertThat(inResult.toRefArray()).containsExactly(Selection.pack(1, gen1));
        assertThat(table.inCalls).isEqualTo(1);
        assertThat(table.lastInColumn).isEqualTo(0);
        assertThat(table.lastInValues).containsExactly(1, 2);

        var notInCondition = CompiledQuery.CompiledCondition.of(
                0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.NOT_IN,
                0);
        var notInResult = kernel.executeCondition(notInCondition, new Object[] { new int[] { 1, 2 } });
        assertThat(notInResult.toIntArray()).containsExactly(0, 2);
        assertThat(table.inCalls).isEqualTo(2);
    }

    @Test
    void shouldDelegateBetweenToConfiguredRuntimeExecutor() {
        long rowGeneration = 30;
        var table = new StubTable()
                .withBetweenRows(new int[] { 2 })
                .withGenerations(new long[] { 20, 25, rowGeneration });
        var generator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        var kernel = new HeapRuntimeKernel(table, TypeHandlerRegistry.getDefault(), generator);
        var condition = CompiledQuery.CompiledCondition.of(
                0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.BETWEEN,
                0);

        var result = kernel.executeCondition(condition, new Object[] { 10, 20 });

        assertThat(result.toRefArray()).containsExactly(Selection.pack(2, rowGeneration));
        assertThat(table.betweenCalls).isEqualTo(1);
        assertThat(table.lastBetweenColumn).isEqualTo(0);
        assertThat(table.lastBetweenMin).isEqualTo(10);
        assertThat(table.lastBetweenMax).isEqualTo(20);
    }

    @Test
    void shouldThrowWhenNoHandlerIsRegisteredForTypeCode() {
        var kernel = new HeapRuntimeKernel(
                new StubTable(),
                TypeHandlerRegistry.empty(),
                new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build()));
        var condition = CompiledQuery.CompiledCondition.of(
                0,
                TypeCodes.TYPE_INT,
                LogicalQuery.Operator.EQ,
                0);

        assertThatThrownBy(() -> kernel.executeCondition(condition, new Object[] { 42 }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No handler registered");
    }

    private static final class StubTable implements GeneratedTable {
        private byte columnType = TypeCodes.TYPE_INT;
        private long allocatedCount = 0;
        private long liveCount = 0;
        private int[] scanAllRows = new int[0];
        private int[] inRows = new int[0];
        private int[] betweenRows = new int[0];
        private long[] generations = new long[0];
        private int inCalls = 0;
        private int lastInColumn = -1;
        private int[] lastInValues = new int[0];
        private int betweenCalls = 0;
        private int lastBetweenColumn = -1;
        private int lastBetweenMin;
        private int lastBetweenMax;

        private StubTable withColumnType(byte typeCode) {
            this.columnType = typeCode;
            return this;
        }

        private StubTable withCounts(long allocated, long live) {
            this.allocatedCount = allocated;
            this.liveCount = live;
            return this;
        }

        private StubTable withScanAll(int[] rows) {
            this.scanAllRows = rows;
            return this;
        }

        private StubTable withInRows(int[] rows) {
            this.inRows = rows;
            return this;
        }

        private StubTable withBetweenRows(int[] rows) {
            this.betweenRows = rows;
            return this;
        }

        private StubTable withGenerations(long[] generations) {
            this.generations = generations;
            return this;
        }

        @Override
        public int columnCount() {
            return 1;
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return columnType;
        }

        @Override
        public long allocatedCount() {
            return allocatedCount;
        }

        @Override
        public long liveCount() {
            return liveCount;
        }

        @Override
        public <T> T readWithSeqLock(int rowIndex, Supplier<T> reader) {
            return reader.get();
        }

        @Override
        public long lookupById(long id) {
            return -1;
        }

        @Override
        public long lookupByIdString(String id) {
            return -1;
        }

        @Override
        public void removeById(long id) {
        }

        @Override
        public long insertFrom(Object[] values) {
            return -1;
        }

        @Override
        public void tombstone(long ref) {
        }

        @Override
        public boolean isLive(long ref) {
            return true;
        }

        @Override
        public long currentGeneration() {
            return 0;
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return rowIndex >= 0 && rowIndex < generations.length ? generations[rowIndex] : 0;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            betweenCalls++;
            lastBetweenColumn = columnIndex;
            lastBetweenMin = min;
            lastBetweenMax = max;
            return betweenRows;
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            return new int[0];
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            inCalls++;
            lastInColumn = columnIndex;
            lastInValues = values;
            return inRows;
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return new int[0];
        }

        @Override
        public int[] scanAll() {
            return scanAllRows;
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            return 0;
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return 0;
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            return null;
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return true;
        }
    }
}
