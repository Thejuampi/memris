package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

class DoubleTypeHandlerTest {
    static class TestTable implements GeneratedTable {
        private final double[] data;
        TestTable(double... data) { this.data = data; }
        // Dummy scanEqualsLong implementation for test
        @Override
        public int[] scanEqualsLong(int col, long value) {
            java.util.List<Integer> rows = new java.util.ArrayList<>();
            for (int i = 0; i < data.length; i++) {
                if (Double.doubleToLongBits(data[i]) == value) rows.add(i);
            }
            return rows.stream().mapToInt(Integer::intValue).toArray();
        }
        // Dummy scanInLong implementation for test
        @Override
        public int[] scanInLong(int col, long[] values) {
            java.util.List<Long> valueList = new java.util.ArrayList<>();
            for (long v : values) valueList.add(v);
            java.util.List<Integer> rows = new java.util.ArrayList<>();
            for (int i = 0; i < data.length; i++) {
                long bits = Double.doubleToLongBits(data[i]);
                if (valueList.contains(bits)) rows.add(i);
            }
            return rows.stream().mapToInt(Integer::intValue).toArray();
        }
        // Stubs for other abstract methods
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return 0; }
        @Override public long liveCount() { return 0; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return null; }
        @Override public long lookupById(long id) { return 0; }
        @Override public long lookupByIdString(String id) { return 0; }
        @Override public void removeById(long id) {}
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) {}
        @Override public boolean isLive(long ref) { return false; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { return new int[0]; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return false; }
    }

    @Test
    @DisplayName("getTypeCode returns TYPE_DOUBLE")
    void getTypeCode() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_DOUBLE);
    }

    @Test
    @DisplayName("getJavaType returns Double.class")
    void getJavaType() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Double.class);
    }

    @Test
    @DisplayName("convertValue handles valid types")
    void convertValue() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.convertValue(1.5d)).isEqualTo(1.5d);
        assertThat(handler.convertValue(2)).isEqualTo(2.0d);
        assertThat(handler.convertValue(3L)).isEqualTo(3.0d);
        assertThat(handler.convertValue("4.5")).isEqualTo(4.5d);
    }

    @Test
    @DisplayName("convertValue throws for invalid")
    void convertValueThrows() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("bad"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot convert");
        assertThatThrownBy(() -> handler.convertValue(new Object())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("executeEquals matches correct values")
    void executeEquals() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        TestTable table = new TestTable(1.0d, 2.0d, 3.0d, 2.0d);
        Selection sel = handler.executeEquals(table, 0, Double.valueOf(2.0d), false);
        assertThat(sel.size()).isEqualTo(2);
        int[] rows = sel.toIntArray();
        assertThat(rows[0]).isEqualTo(1);
        assertThat(rows[1]).isEqualTo(3);
    }

    @Test
    @DisplayName("executeIn matches set values")
    void executeIn() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        TestTable table = new TestTable(1.0d, 2.0d, 3.0d, 2.0d);
        Selection sel = handler.executeIn(table, 0, Arrays.asList(1.0d, 3.0d));
        assertThat(sel.size()).isEqualTo(2);
        int[] rows2 = sel.toIntArray();
        assertThat(rows2[0]).isEqualTo(0);
        assertThat(rows2[1]).isEqualTo(2);
    }

    @Test
    @DisplayName("executeIn with empty set returns none")
    void executeInEmpty() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        TestTable table = new TestTable(1.0d, 2.0d);
        Selection sel = handler.executeIn(table, 0, Collections.emptyList());
        assertThat(sel.size()).isZero();
    }

    @Test
    @DisplayName("executeEquals with null returns none")
    void executeEqualsNull() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        TestTable table = new TestTable(1.0d, 2.0d);
        Selection sel = handler.executeEquals(table, 0, null, false);
        assertThat(sel.size()).isZero();
    }
}
