package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.*;

class ByteTypeHandlerTest {
    static class TestTable implements GeneratedTable {
        private final byte[] data;
        TestTable(byte... data) { this.data = data; }
        // Dummy scanEqualsInt implementation for test
        @Override
        public int[] scanEqualsInt(int col, int value) {
            java.util.List<Integer> rows = new java.util.ArrayList<>();
            for (int i = 0; i < data.length; i++) {
                if (data[i] == (byte)value) rows.add(i);
            }
            return rows.stream().mapToInt(Integer::intValue).toArray();
        }
        // Dummy scanInInt implementation for test
        @Override
        public int[] scanInInt(int col, int[] values) {
            java.util.List<Integer> valueList = new java.util.ArrayList<>();
            for (int v : values) valueList.add(v);
            java.util.List<Integer> rows = new java.util.ArrayList<>();
            for (int i = 0; i < data.length; i++) {
                if (valueList.contains((int)data[i])) rows.add(i);
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
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { return new int[0]; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return false; }
    }

    @Test
    @DisplayName("getTypeCode returns TYPE_BYTE")
    void getTypeCode() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_BYTE);
    }

    @Test
    @DisplayName("getJavaType returns Byte.class")
    void getJavaType() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Byte.class);
    }

    @Test
    @DisplayName("convertValue handles valid types")
    void convertValue() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.convertValue((byte)1)).isEqualTo((byte)1);
        assertThat(handler.convertValue(2)).isEqualTo((byte)2);
        assertThat(handler.convertValue(3L)).isEqualTo((byte)3);
        assertThat(handler.convertValue("4")).isEqualTo((byte)4);
    }

    @Test
    @DisplayName("convertValue throws for invalid")
    void convertValueThrows() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("bad"))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot convert");
        assertThatThrownBy(() -> handler.convertValue(new Object())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> handler.convertValue(null))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Cannot convert null");
    }

    @Test
    @DisplayName("executeEquals matches correct values")
    void executeEquals() {
        ByteTypeHandler handler = new ByteTypeHandler();
        TestTable table = new TestTable((byte)1, (byte)2, (byte)3, (byte)2);
        Selection sel = handler.executeEquals(table, 0, Byte.valueOf((byte)2), false);
        assertThat(sel.size()).isEqualTo(2);
        int[] rows = sel.toIntArray();
        assertThat(rows[0]).isEqualTo(1);
        assertThat(rows[1]).isEqualTo(3);
    }

    @Test
    @DisplayName("executeIn matches set values")
    void executeIn() {
        ByteTypeHandler handler = new ByteTypeHandler();
        TestTable table = new TestTable((byte)1, (byte)2, (byte)3, (byte)2);
        Selection sel = handler.executeIn(table, 0, Arrays.asList((byte)1, (byte)3));
        assertThat(sel.size()).isEqualTo(2);
        int[] rows2 = sel.toIntArray();
        assertThat(rows2[0]).isEqualTo(0);
        assertThat(rows2[1]).isEqualTo(2);
    }

    @Test
    @DisplayName("executeIn list rejects incompatible element types")
    void executeInRejectsIncompatibleElements() {
        ByteTypeHandler handler = new ByteTypeHandler();
        TestTable table = new TestTable((byte)1, (byte)2);
        assertThatThrownBy(() -> handler.executeIn(table, 0, Arrays.asList("1", (byte)2)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be java.lang.Byte");
    }

    @Test
    @DisplayName("executeIn with empty set returns none")
    void executeInEmpty() {
        ByteTypeHandler handler = new ByteTypeHandler();
        TestTable table = new TestTable((byte)1, (byte)2);
        Selection sel = handler.executeIn(table, 0, Collections.emptyList());
        assertThat(sel.size()).isZero();
    }

    @Test
    @DisplayName("executeIn list rejects null elements with clear message")
    void executeInRejectsNullElements() {
        ByteTypeHandler handler = new ByteTypeHandler();
        TestTable table = new TestTable((byte)1, (byte)2);
        assertThatThrownBy(() -> handler.executeIn(table, 0, Arrays.asList((byte)1, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot contain nulls");
    }

    @Test
    @DisplayName("executeEquals with null returns none")
    void executeEqualsNull() {
        ByteTypeHandler handler = new ByteTypeHandler();
        TestTable table = new TestTable((byte)1, (byte)2);
        Selection sel = handler.executeEquals(table, 0, null, false);
        assertThat(sel.size()).isZero();
    }
}
