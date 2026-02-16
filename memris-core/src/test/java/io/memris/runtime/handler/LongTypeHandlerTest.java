package io.memris.runtime.handler;

import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LongTypeHandlerTest {

    @Test
    void basic_long_handler_behaviour() {
        LongTypeHandler handler = new LongTypeHandler();

        GeneratedTable table = new GeneratedTable() {
            @Override public int columnCount() { return 1; }
            @Override public byte typeCodeAt(int columnIndex) { return 0; }
            @Override public long allocatedCount() { return 0; }
            @Override public long liveCount() { return 0; }
            @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
            @Override public long lookupById(long id) { return -1; }
            @Override public long lookupByIdString(String id) { return -1; }
            @Override public void removeById(long id) {}
            @Override public long insertFrom(Object[] values) { return 0; }
            @Override public void tombstone(long ref) {}
            @Override public boolean isLive(long ref) { return false; }
            @Override public long currentGeneration() { return 0; }
            @Override public long rowGeneration(int rowIndex) { return 0L; }
            @Override public int[] scanEqualsLong(int columnIndex, long value) { return value==99L ? new int[]{7} : new int[0]; }
            @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
            @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
            @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return (min<=50L && max>=50L) ? new int[]{8} : new int[0]; }
            @Override public int[] scanInLong(int columnIndex, long[] values) { return (values.length==2 && values[0]==1L) ? new int[]{9,10} : new int[0]; }
            @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
            @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
            @Override public int[] scanAll() { return new int[]{5,6,7}; }
            @Override public long readLong(int columnIndex, int rowIndex) { return 50L; }
            @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
            @Override public String readString(int columnIndex, int rowIndex) { return null; }
            @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
        };

        Selection eq = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, 99L, false);
        assertThat(eq.toIntArray()).containsExactly(7);

        Selection between = handler.executeBetweenRange(table, 0, 10L, 90L);
        // our stub returns {8} when range includes 50
        assertThat(between.toIntArray()).containsExactly(8);

        Selection in = handler.executeIn(table, 0, new long[]{1L,2L});
        assertThat(in.toIntArray()).containsExactly(9,10);
    }

    @Test
    void shouldCoverComparisonAndNullOperators() {
        LongTypeHandler handler = new LongTypeHandler();

        GeneratedTable table = new GeneratedTable() {
            @Override public int columnCount() { return 1; }
            @Override public byte typeCodeAt(int columnIndex) { return 0; }
            @Override public long allocatedCount() { return 0; }
            @Override public long liveCount() { return 0; }
            @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
            @Override public long lookupById(long id) { return -1; }
            @Override public long lookupByIdString(String id) { return -1; }
            @Override public void removeById(long id) {}
            @Override public long insertFrom(Object[] values) { return 0; }
            @Override public void tombstone(long ref) {}
            @Override public boolean isLive(long ref) { return true; }
            @Override public long currentGeneration() { return 0; }
            @Override public long rowGeneration(int rowIndex) { return 1L; }
            @Override public int[] scanEqualsLong(int columnIndex, long value) { return value == 10L ? new int[]{0} : new int[0]; }
            @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
            @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
            @Override public int[] scanBetweenLong(int columnIndex, long min, long max) {
                if (min == 11L && max == Long.MAX_VALUE) return new int[] {1, 2};
                if (min == 10L && max == Long.MAX_VALUE) return new int[] {0, 1, 2};
                if (min == Long.MIN_VALUE && max == 9L) return new int[] {3};
                if (min == Long.MIN_VALUE && max == 10L) return new int[] {0, 3};
                return new int[0];
            }
            @Override public int[] scanInLong(int columnIndex, long[] values) { return values.length == 1 && values[0] == 10L ? new int[] {0} : new int[0]; }
            @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
            @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
            @Override public int[] scanAll() { return new int[] {0, 1, 2, 3}; }
            @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
            @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
            @Override public String readString(int columnIndex, int rowIndex) { return null; }
            @Override public boolean isPresent(int columnIndex, int rowIndex) { return rowIndex != 3; }
        };

        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 10L, false).toIntArray())
                .containsExactlyInAnyOrder(1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.GTE, 10L, false).toIntArray())
                .containsExactlyInAnyOrder(0, 1, 2);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LT, 10L, false).toIntArray())
                .containsExactly(3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.LTE, 10L, false).toIntArray())
                .containsExactlyInAnyOrder(0, 3);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IN, 10L, false).toIntArray())
                .containsExactly(0);
        assertThat(handler.executeCondition(table, 0, LogicalQuery.Operator.IS_NULL, null, false).toIntArray())
                .containsExactly(3);

        assertThat(handler.convertValue(42)).isEqualTo(42L);
        assertThatThrownBy(() -> handler.convertValue("bad")).isInstanceOf(IllegalArgumentException.class);
    }
}
