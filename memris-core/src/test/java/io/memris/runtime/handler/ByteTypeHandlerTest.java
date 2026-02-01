package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for ByteTypeHandler.
 */
class ByteTypeHandlerTest {

    @Test
    @DisplayName("getTypeCode should return TYPE_BYTE")
    void getTypeCode() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_BYTE);
    }

    @Test
    @DisplayName("getJavaType should return Byte.class")
    void getJavaType() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Byte.class);
    }

    @Test
    @DisplayName("convertValue should convert Byte to Byte")
    void convertByteValue() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.convertValue((byte) 42)).isEqualTo((byte) 42);
    }

    @Test
    @DisplayName("convertValue should convert Number to Byte")
    void convertNumberToByte() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThat(handler.convertValue(42)).isEqualTo((byte) 42);
        assertThat(handler.convertValue(42L)).isEqualTo((byte) 42);
        assertThat(handler.convertValue(42.5)).isEqualTo((byte) 42);
    }

    @Test
    @DisplayName("convertValue should throw for non-numeric types")
    void convertNonNumericThrows() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("not a number"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot convert");
    }

    @Test
    @DisplayName("executeEquals should find matching bytes")
    void executeEquals() {
        ByteTypeHandler handler = new ByteTypeHandler();
        byte[] values = {10, 20, 10, 30, 10};
        GeneratedTable table = new FakeByteTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, (byte) 10, false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(0, 2, 4);
    }

    @Test
    @DisplayName("executeGreaterThan should find bytes greater than target")
    void executeGreaterThan() {
        ByteTypeHandler handler = new ByteTypeHandler();
        byte[] values = {10, 20, 30, 40, 50};
        GeneratedTable table = new FakeByteTable(values);

        Selection selection = handler.executeBetweenRange(table, 0, (byte) 30, (byte) 50);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(2, 3, 4);
    }

    @Test
    @DisplayName("executeBetweenRange should find bytes within range")
    void executeBetweenRange() {
        ByteTypeHandler handler = new ByteTypeHandler();
        byte[] values = {10, 20, 30, 40, 50};
        GeneratedTable table = new FakeByteTable(values);

        Selection selection = handler.executeBetweenRange(table, 0, (byte) 25, (byte) 45);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(2, 3);
    }

    @Test
    @DisplayName("executeIn with byte array should find multiple values")
    void executeInWithByteArray() {
        ByteTypeHandler handler = new ByteTypeHandler();
        byte[] values = {10, 20, 30, 40, 50};
        GeneratedTable table = new FakeByteTable(values);

        Selection selection = handler.executeIn(table, 0, new byte[]{20, 40});

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    @DisplayName("executeBetween should throw UnsupportedOperationException")
    void executeBetweenThrows() {
        ByteTypeHandler handler = new ByteTypeHandler();
        assertThatThrownBy(() -> handler.executeBetween(null, 0, (byte) 1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("executeBetweenRange");
    }

    private static final class FakeByteTable implements GeneratedTable {
        private final int[] values;

        private FakeByteTable(byte[] bytes) {
            this.values = new int[bytes.length];
            for (int i = 0; i < bytes.length; i++) {
                values[i] = bytes[i];
            }
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_BYTE; }
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
        
        @Override public int[] scanEqualsLong(int columnIndex, long value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsInt(int columnIndex, int value) {
            int[] matches = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] == value) matches[count++] = i;
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }
        @Override public int[] scanEqualsString(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) {
            int[] matches = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] >= min && values[i] <= max) matches[count++] = i;
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { throw new UnsupportedOperationException(); }
        
        @Override public int[] scanInLong(int columnIndex, long[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInInt(int columnIndex, int[] targets) {
            int[] matches = new int[this.values.length];
            int count = 0;
            for (int i = 0; i < this.values.length; i++) {
                for (int target : targets) {
                    if (this.values[i] == target) {
                        matches[count++] = i;
                        break;
                    }
                }
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
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
