package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

class ShortTypeHandlerTest {
    static class TestTable implements GeneratedTable {
        private final short[] data;
        TestTable(short... data) { this.data = data; }
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return data.length; }
        @Override public long liveCount() { return data.length; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) {}
        @Override public long insertFrom(Object[] values) { throw new UnsupportedOperationException(); }
        @Override public void tombstone(long ref) {}
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0L; }
        @Override public long rowGeneration(int row) { return 0L; }
        @Override
        public int[] scanEqualsInt(int col, int bits) {
            int[] tmp = new int[data.length];
            int count = 0;
            for (int i = 0; i < data.length; i++) {
                if (Short.valueOf(data[i]).hashCode() == bits) {
                    tmp[count++] = i;
                }
            }
            return java.util.Arrays.copyOf(tmp, count);
        }
        @Override
        public int[] scanInInt(int col, int[] bits) {
            java.util.Set<Integer> bitSet = new java.util.HashSet<>();
            for (int b : bits) bitSet.add(b);
            int[] tmp = new int[data.length];
            int count = 0;
            for (int i = 0; i < data.length; i++) {
                if (bitSet.contains(Short.valueOf(data[i]).hashCode())) {
                    tmp[count++] = i;
                }
            }
            return java.util.Arrays.copyOf(tmp, count);
        }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() {
            int[] rows = new int[data.length];
            for (int i = 0; i < data.length; i++) {
                rows[i] = i;
            }
            return rows;
        }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public int readInt(int columnIndex, int rowIndex) { return data[rowIndex]; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
    }

    @Test
    @DisplayName("getTypeCode returns TYPE_SHORT")
    void getTypeCode() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_SHORT);
    }

    @Test
    @DisplayName("getJavaType returns Short.class")
    void getJavaType() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Short.class);
    }

    @Test
    @DisplayName("convertValue handles valid types")
    void convertValue() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.convertValue((short)1)).isEqualTo((short)1);
        assertThat(handler.convertValue(2)).isEqualTo((short)2);
        assertThat(handler.convertValue(3L)).isEqualTo((short)3);
        // String is not supported, should throw
        assertThatThrownBy(() -> handler.convertValue("4")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("convertValue throws for invalid")
    void convertValueThrows() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("bad")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> handler.convertValue(new Object())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("executeEquals matches correct values")
    void executeEquals() {
        ShortTypeHandler handler = new ShortTypeHandler();
        TestTable table = new TestTable((short)1, (short)2, (short)3, (short)2);
        Selection sel = handler.executeEquals(table, 0, (short)2, false);
        assertThat(sel.size()).isEqualTo(2);
        int[] rows = sel.toIntArray();
        assertThat(rows).containsExactly(1, 3);
    }

    @Test
    @DisplayName("executeIn matches set values")
    void executeIn() {
        ShortTypeHandler handler = new ShortTypeHandler();
        TestTable table = new TestTable((short)1, (short)2, (short)3, (short)2);
        Selection sel = handler.executeIn(table, 0, new short[] {(short)1, (short)3});
        assertThat(sel.size()).isEqualTo(2);
        int[] rows = sel.toIntArray();
        assertThat(rows).containsExactly(0, 2);
    }

    @Test
    @DisplayName("executeIn with empty set returns none")
    void executeInEmpty() {
        ShortTypeHandler handler = new ShortTypeHandler();
        TestTable table = new TestTable((short)1, (short)2);
        Selection sel = handler.executeIn(table, 0, new short[]{});
        assertThat(sel.size()).isZero();
    }

    @Test
    @DisplayName("executeEquals with null throws NPE")
    void executeEqualsNull() {
        ShortTypeHandler handler = new ShortTypeHandler();
        TestTable table = new TestTable((short)1, (short)2);
        assertThatThrownBy(() -> handler.executeEquals(table, 0, null, false)).isInstanceOf(NullPointerException.class);
    }
}
