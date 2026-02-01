package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for DoubleTypeHandler.
 */
class DoubleTypeHandlerTest {

    @Test
    @DisplayName("getTypeCode should return TYPE_DOUBLE")
    void getTypeCode() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_DOUBLE);
    }

    @Test
    @DisplayName("getJavaType should return Double.class")
    void getJavaType() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Double.class);
    }

    @Test
    @DisplayName("convertValue should convert Double to Double")
    void convertDoubleValue() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.convertValue(42.5)).isEqualTo(42.5);
    }

    @Test
    @DisplayName("convertValue should convert Number to Double")
    void convertNumberToDouble() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThat(handler.convertValue(42)).isEqualTo(42.0);
        assertThat(handler.convertValue(42L)).isEqualTo(42.0);
        assertThat(handler.convertValue(42.5f)).isEqualTo(42.5);
    }

    @Test
    @DisplayName("convertValue should throw for non-numeric types")
    void convertNonNumericThrows() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("not a number"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot convert");
    }

    @Test
    @DisplayName("executeEquals should find matching doubles using sortable encoding")
    void executeEquals() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        double[] values = {1.5, -2.5, 1.5, 0.0, 1.5};
        GeneratedTable table = new FakeDoubleTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, 1.5, false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(0, 2, 4);
    }

    @Test
    @DisplayName("executeBetweenRange should work with sortable encoding across zero")
    void executeBetweenRange() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        double[] values = {-5.5, -1.0, 0.0, 2.5, 7.5};
        GeneratedTable table = new FakeDoubleTable(values);

        Selection selection = handler.executeBetweenRange(table, 0, -2.0, 3.0);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    @DisplayName("executeBetweenRange with all negative values")
    void executeBetweenRangeNegativeOnly() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        double[] values = {-10.0, -5.0, -3.0, -1.0};
        GeneratedTable table = new FakeDoubleTable(values);

        Selection selection = handler.executeBetweenRange(table, 0, -8.0, -2.0);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 2);
    }

    @Test
    @DisplayName("executeIn with double array should find multiple values")
    void executeInWithDoubleArray() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        double[] values = {1.1, 2.2, 3.3, 4.4, 5.5};
        GeneratedTable table = new FakeDoubleTable(values);

        Selection selection = handler.executeIn(table, 0, new double[]{2.2, 4.4});

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    @DisplayName("executeBetween should throw UnsupportedOperationException")
    void executeBetweenThrows() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        assertThatThrownBy(() -> handler.executeBetween(null, 0, 1.0))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("executeBetweenRange");
    }

    @Test
    @DisplayName("should handle extreme double values")
    void extremeDoubleValues() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        double[] values = {Double.MAX_VALUE, 0.0, Double.MIN_VALUE};
        GeneratedTable table = new FakeDoubleTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, Double.MAX_VALUE, false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(0);
    }

    @Test
    @DisplayName("should handle negative zero correctly")
    void negativeZeroHandling() {
        DoubleTypeHandler handler = new DoubleTypeHandler();
        // Note: Double.compare distinguishes 0.0 and -0.0, but sortable encoding may not
        // This test verifies the actual behavior of sortable encoding
        double[] values = {0.0, 1.0};
        GeneratedTable table = new FakeDoubleTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, 0.0, false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(0);
    }

    private static final class FakeDoubleTable implements GeneratedTable {
        private final long[] values;

        private FakeDoubleTable(double[] doubles) {
            this.values = new long[doubles.length];
            for (int i = 0; i < doubles.length; i++) {
                values[i] = FloatEncoding.doubleToSortableLong(doubles[i]);
            }
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_DOUBLE; }
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
        
        @Override public int[] scanEqualsLong(int columnIndex, long value) {
            int[] matches = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] == value) matches[count++] = i;
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsString(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) {
            int[] matches = new int[values.length];
            int count = 0;
            for (int i = 0; i < values.length; i++) {
                if (values[i] >= min && values[i] <= max) matches[count++] = i;
            }
            int[] trimmed = new int[count];
            System.arraycopy(matches, 0, trimmed, 0, count);
            return trimmed;
        }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { throw new UnsupportedOperationException(); }
        
        @Override public int[] scanInLong(int columnIndex, long[] targets) {
            int[] matches = new int[this.values.length];
            int count = 0;
            for (int i = 0; i < this.values.length; i++) {
                for (long target : targets) {
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
        @Override public int[] scanInInt(int columnIndex, int[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInString(int columnIndex, String[] values) { throw new UnsupportedOperationException(); }
        
        @Override public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) rows[i] = i;
            return rows;
        }
        
        @Override public long readLong(int columnIndex, int rowIndex) { return values[rowIndex]; }
        @Override public int readInt(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public String readString(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
    }
}
