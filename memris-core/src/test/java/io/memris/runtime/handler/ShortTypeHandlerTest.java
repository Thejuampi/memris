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
 * Tests for ShortTypeHandler.
 */
class ShortTypeHandlerTest {

    @Test
    @DisplayName("getTypeCode should return TYPE_SHORT")
    void getTypeCode() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_SHORT);
    }

    @Test
    @DisplayName("getJavaType should return Short.class")
    void getJavaType() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Short.class);
    }

    @Test
    @DisplayName("convertValue should convert Short to Short")
    void convertShortValue() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.convertValue((short) 42)).isEqualTo((short) 42);
    }

    @Test
    @DisplayName("convertValue should convert Number to Short")
    void convertNumberToShort() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThat(handler.convertValue(42)).isEqualTo((short) 42);
        assertThat(handler.convertValue(42L)).isEqualTo((short) 42);
        assertThat(handler.convertValue(42.7)).isEqualTo((short) 42);
    }

    @Test
    @DisplayName("convertValue should throw for non-numeric types")
    void convertNonNumericThrows() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("not a number"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot convert");
    }

    @Test
    @DisplayName("executeEquals should find matching shorts")
    void executeEquals() {
        ShortTypeHandler handler = new ShortTypeHandler();
        short[] values = {100, 200, 100, 300, 100};
        GeneratedTable table = new FakeShortTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, (short) 100, false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(0, 2, 4);
    }

    @Test
    @DisplayName("executeBetweenRange should find shorts within range")
    void executeBetweenRange() {
        ShortTypeHandler handler = new ShortTypeHandler();
        short[] values = {100, 200, 300, 400, 500};
        GeneratedTable table = new FakeShortTable(values);

        Selection selection = handler.executeBetweenRange(table, 0, (short) 200, (short) 400);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    @DisplayName("executeIn with short array should find multiple values")
    void executeInWithShortArray() {
        ShortTypeHandler handler = new ShortTypeHandler();
        short[] values = {100, 200, 300, 400, 500};
        GeneratedTable table = new FakeShortTable(values);

        Selection selection = handler.executeIn(table, 0, new short[]{200, 400});

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    @DisplayName("executeBetween should throw UnsupportedOperationException")
    void executeBetweenThrows() {
        ShortTypeHandler handler = new ShortTypeHandler();
        assertThatThrownBy(() -> handler.executeBetween(null, 0, (short) 1))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("executeBetweenRange");
    }

    @Test
    @DisplayName("boundary values with Short.MIN_VALUE and Short.MAX_VALUE")
    void boundaryValues() {
        ShortTypeHandler handler = new ShortTypeHandler();
        short[] values = {Short.MIN_VALUE, 0, 100, Short.MAX_VALUE};
        GeneratedTable table = new FakeShortTable(values);

        Selection minSelection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, Short.MIN_VALUE, false);
        Selection maxSelection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, Short.MAX_VALUE, false);

        assertThat(minSelection.toIntArray()).containsExactlyInAnyOrder(0);
        assertThat(maxSelection.toIntArray()).containsExactlyInAnyOrder(3);
    }

    private static final class FakeShortTable implements GeneratedTable {
        private final int[] values;

        private FakeShortTable(short[] shorts) {
            this.values = new int[shorts.length];
            for (int i = 0; i < shorts.length; i++) {
                values[i] = shorts[i];
            }
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_SHORT; }
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
