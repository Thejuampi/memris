package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class BooleanTypeHandlerTest {
    private final BooleanTypeHandler handler = new BooleanTypeHandler();

    @Test
    void testMetadata() {
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_BOOLEAN);
        assertThat(handler.getJavaType()).isEqualTo(Boolean.class);
    }

    @Test
    void testConvertValue() {
        assertThat(handler.convertValue(true)).isTrue();
        assertThat(handler.convertValue(false)).isFalse();
        assertThat(handler.convertValue(1)).isTrue();
        assertThat(handler.convertValue(0)).isFalse();
        assertThat(handler.convertValue(-5)).isTrue();
        assertThatThrownBy(() -> handler.convertValue("not a bool")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEqualsSelection() {
        var table = new BooleanTable(new int[]{1, 0, 1, 0, 1});
        Selection sel = handler.executeEquals(table, 0, true, false);
        assertThat(sel.toIntArray()).containsExactly(0, 2, 4);
        sel = handler.executeEquals(table, 0, false, false);
        assertThat(sel.toIntArray()).containsExactly(1, 3);
    }

    @Test
    void testInSelection() {
        var table = new BooleanTable(new int[]{1, 0, 1, 0, 1});
        Selection sel = handler.executeIn(table, 0, true);
        assertThat(sel.toIntArray()).containsExactly(0, 2, 4);
        sel = handler.executeIn(table, 0, false);
        assertThat(sel.toIntArray()).containsExactly(1, 3);
    }

    @Test
    void testIsTrueIsFalse() {
        var table = new BooleanTable(new int[]{1, 0, 1, 0, 1});
        Selection sel = handler.executeIsTrue(table, 0);
        assertThat(sel.toIntArray()).containsExactly(0, 2, 4);
        sel = handler.executeIsFalse(table, 0);
        assertThat(sel.toIntArray()).containsExactly(1, 3);
    }

    @Test
    void testUnsupportedOps() {
        var table = new BooleanTable(new int[]{1, 0});
        assertThatThrownBy(() -> handler.executeGreaterThan(table, 0, true)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> handler.executeLessThan(table, 0, false)).isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> handler.executeBetween(table, 0, true)).isInstanceOf(UnsupportedOperationException.class);
    }

    static class BooleanTable implements GeneratedTable {
        private final int[] values; // 1 = true, 0 = false
        BooleanTable(int[] values) { this.values = values; }
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_BOOLEAN; }
        @Override public long allocatedCount() { return values.length; }
        @Override public long liveCount() { return values.length; }
        @Override public long lookupById(long id) { return -1L; }
        @Override public long lookupByIdString(String id) { return -1L; }
        @Override public void removeById(long id) { }
        @Override public long insertFrom(Object[] values) { throw new UnsupportedOperationException(); }
        @Override public void tombstone(long ref) { }
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0L; }
        @Override public long rowGeneration(int rowIndex) { return 0L; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsInt(int columnIndex, int value) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) if (values[i] == value) matches.add(i);
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanEqualsString(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInLong(int columnIndex, long[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInInt(int columnIndex, int[] targets) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                for (int t : targets) {
                    if (values[i] == t) {
                        matches.add(i);
                        break;
                    }
                }
            }
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanInString(int columnIndex, String[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) rows[i] = i;
            return rows;
        }
        @Override public long readLong(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public int readInt(int columnIndex, int rowIndex) { return values[rowIndex]; }
        @Override public String readString(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
    }
}
