package io.memris.runtime.handler;

import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IntTypeHandlerTest {
    // Ownership: int-specific operator dispatch behavior not covered by parameterized int/long semantics.
    // Out-of-scope: full equals/between/in semantic matrix (owned by IntAndLongTypeHandlerTest).

    @Test
    void shouldRouteComparisonAndNullOperatorsThroughExecuteCondition() {
        IntTypeHandler handler = new IntTypeHandler();

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
            @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
            @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
            @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanBetweenInt(int columnIndex, int min, int max) {
                if (min == 8 && max == Integer.MAX_VALUE) {
                    return new int[] {3, 4};
                }
                return new int[0];
            }
            @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
            @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
            @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
            @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
            @Override public int[] scanAll() { return new int[]{0, 1, 2, 3, 4}; }
            @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
            @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
            @Override public String readString(int columnIndex, int rowIndex) { return null; }
            @Override public boolean isPresent(int columnIndex, int rowIndex) { return rowIndex != 1 && rowIndex != 4; }
        };

        Selection gt = handler.executeCondition(table, 0, LogicalQuery.Operator.GT, 7, false);
        assertThat(gt.toIntArray()).containsExactly(3, 4);

        Selection isNull = handler.executeCondition(table, 0, LogicalQuery.Operator.IS_NULL, null, false);
        assertThat(isNull.toIntArray()).containsExactly(1, 4);
    }
}
