package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NumericHandlerRangeCoverageTest {

    @Test
    void intTypeHandlerCoversComparisonAndSingleInPaths() {
        IntTypeHandler handler = new IntTypeHandler();
        IntTable table = new IntTable(new int[] { 5, 10, 15, 20 });

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 10, false).toIntArray())
                .containsExactly(2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, 10, false).toIntArray())
                .containsExactly(1, 2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, 15, false).toIntArray())
                .containsExactly(0, 1);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, 15, false).toIntArray())
                .containsExactly(0, 1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, 10, false).toIntArray())
                .containsExactly(1);

        assertThatThrownBy(() -> handler.convertValue("bad")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shortTypeHandlerCoversComparisonAndSingleInPaths() {
        ShortTypeHandler handler = new ShortTypeHandler();
        IntTable table = new IntTable(new int[] { 2, 4, 6, 8 });

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, (short) 4, false).toIntArray())
                .containsExactly(2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, (short) 4, false).toIntArray())
                .containsExactly(1, 2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, (short) 6, false).toIntArray())
                .containsExactly(0, 1);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, (short) 6, false).toIntArray())
                .containsExactly(0, 1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, (short) 4, false).toIntArray())
                .containsExactly(1);
    }

    @Test
    void charTypeHandlerCoversComparisonAndSingleInPaths() {
        CharTypeHandler handler = new CharTypeHandler();
        IntTable table = new IntTable(new int[] { 'a', 'c', 'e', 'g' });

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 'c', false).toIntArray())
                .containsExactly(2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, 'c', false).toIntArray())
                .containsExactly(1, 2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, 'e', false).toIntArray())
                .containsExactly(0, 1);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, 'e', false).toIntArray())
                .containsExactly(0, 1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, 'c', false).toIntArray())
                .containsExactly(1);
    }

    @Test
    void floatTypeHandlerCoversComparisonAndSingleInPaths() {
        FloatTypeHandler handler = new FloatTypeHandler();
        IntTable table = new IntTable(new int[] {
                FloatEncoding.floatToSortableInt(1.0f),
                FloatEncoding.floatToSortableInt(2.0f),
                FloatEncoding.floatToSortableInt(3.0f),
                FloatEncoding.floatToSortableInt(4.0f)
        });

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 2.0f, false).toIntArray())
                .containsExactly(2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, 2.0f, false).toIntArray())
                .containsExactly(1, 2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, 3.0f, false).toIntArray())
                .containsExactly(0, 1);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, 3.0f, false).toIntArray())
                .containsExactly(0, 1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, 2.0f, false).toIntArray())
                .containsExactly(1);
    }

    @Test
    void doubleTypeHandlerCoversComparisonAndSingleInPaths() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        LongTable table = new LongTable(new long[] {
                FloatEncoding.doubleToSortableLong(1.0d),
                FloatEncoding.doubleToSortableLong(2.0d),
                FloatEncoding.doubleToSortableLong(3.0d),
                FloatEncoding.doubleToSortableLong(4.0d)
        });

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 2.0d, false).toIntArray())
                .containsExactly(2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, 2.0d, false).toIntArray())
                .containsExactly(1, 2, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, 3.0d, false).toIntArray())
                .containsExactly(0, 1);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, 3.0d, false).toIntArray())
                .containsExactly(0, 1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, 2.0d, false).toIntArray())
                .containsExactly(1);
    }

    @Test
    void localDateTimeTypeHandlerCoversInclusiveComparisons() {
        LocalDateTimeTypeHandler handler = new LocalDateTimeTypeHandler();
        LocalDateTime t1 = LocalDateTime.of(2024, 1, 1, 0, 0);
        LocalDateTime t2 = LocalDateTime.of(2024, 1, 2, 0, 0);
        LocalDateTime t3 = LocalDateTime.of(2024, 1, 3, 0, 0);
        LongTable table = new LongTable(new long[] {
                t1.toInstant(ZoneOffset.UTC).toEpochMilli(),
                t2.toInstant(ZoneOffset.UTC).toEpochMilli(),
                t3.toInstant(ZoneOffset.UTC).toEpochMilli()
        });

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, t2, false).toIntArray())
                .containsExactly(1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, t2, false).toIntArray())
                .containsExactly(0, 1);
    }

    static class IntTable implements GeneratedTable {
        private final int[] values;

        IntTable(int[] values) {
            this.values = values;
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return values.length; }
        @Override public long liveCount() { return values.length; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) { }
        @Override public long insertFrom(Object[] values) { throw new UnsupportedOperationException(); }
        @Override public void tombstone(long ref) { }
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return scanBetweenInt(columnIndex, value, value);
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            int[] tmp = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] >= min && values[i] <= max) {
                    tmp[count++] = i;
                }
            }
            return java.util.Arrays.copyOf(tmp, count);
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] targets) {
            java.util.Set<Integer> set = new java.util.HashSet<>();
            for (int target : targets) {
                set.add(target);
            }
            int[] tmp = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (set.contains(values[i])) {
                    tmp[count++] = i;
                }
            }
            return java.util.Arrays.copyOf(tmp, count);
        }

        @Override
        public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                rows[i] = i;
            }
            return rows;
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return values[rowIndex];
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return true;
        }
    }

    static class LongTable implements GeneratedTable {
        private final long[] values;

        LongTable(long[] values) {
            this.values = values;
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return values.length; }
        @Override public long liveCount() { return values.length; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) { }
        @Override public long insertFrom(Object[] values) { throw new UnsupportedOperationException(); }
        @Override public void tombstone(long ref) { }
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return scanBetweenLong(columnIndex, value, value);
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            int[] tmp = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] >= min && values[i] <= max) {
                    tmp[count++] = i;
                }
            }
            return java.util.Arrays.copyOf(tmp, count);
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] targets) {
            java.util.Set<Long> set = new java.util.HashSet<>();
            for (long target : targets) {
                set.add(target);
            }
            int[] tmp = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (set.contains(values[i])) {
                    tmp[count++] = i;
                }
            }
            return java.util.Arrays.copyOf(tmp, count);
        }

        @Override
        public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) {
                rows[i] = i;
            }
            return rows;
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            return values[rowIndex];
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return true;
        }
    }
}
