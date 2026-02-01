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
 * Tests for CharTypeHandler.
 */
class CharTypeHandlerTest {

    @Test
    @DisplayName("getTypeCode should return TYPE_CHAR")
    void getTypeCode() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_CHAR);
    }

    @Test
    @DisplayName("getJavaType should return Character.class")
    void getJavaType() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThat(handler.getJavaType()).isEqualTo(Character.class);
    }

    @Test
    @DisplayName("convertValue should convert Character to Character")
    void convertCharacterValue() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThat(handler.convertValue('A')).isEqualTo('A');
    }

    @Test
    @DisplayName("convertValue should convert Number to Character")
    void convertNumberToChar() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThat(handler.convertValue(65)).isEqualTo('A');
        assertThat(handler.convertValue(97L)).isEqualTo('a');
    }

    @Test
    @DisplayName("convertValue should convert single-character String to Character")
    void convertStringToChar() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThat(handler.convertValue("A")).isEqualTo('A');
        assertThat(handler.convertValue("z")).isEqualTo('z');
    }

    @Test
    @DisplayName("convertValue should throw for invalid types")
    void convertInvalidThrows() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThatThrownBy(() -> handler.convertValue("AB"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot convert");
    }

    @Test
    @DisplayName("executeEquals should find matching characters")
    void executeEquals() {
        CharTypeHandler handler = new CharTypeHandler();
        char[] values = {'A', 'B', 'A', 'C', 'A'};
        GeneratedTable table = new FakeCharTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, 'A', false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(0, 2, 4);
    }

    @Test
    @DisplayName("executeBetweenRange should find characters within range")
    void executeBetweenRange() {
        CharTypeHandler handler = new CharTypeHandler();
        char[] values = {'A', 'D', 'G', 'M', 'Z'};
        GeneratedTable table = new FakeCharTable(values);

        Selection selection = handler.executeBetweenRange(table, 0, 'B', 'N');

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    @DisplayName("executeIn with char array should find multiple values")
    void executeInWithCharArray() {
        CharTypeHandler handler = new CharTypeHandler();
        char[] values = {'A', 'B', 'C', 'D', 'E'};
        GeneratedTable table = new FakeCharTable(values);

        Selection selection = handler.executeIn(table, 0, new char[]{'B', 'D'});

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1, 3);
    }

    @Test
    @DisplayName("executeBetween should throw UnsupportedOperationException")
    void executeBetweenThrows() {
        CharTypeHandler handler = new CharTypeHandler();
        assertThatThrownBy(() -> handler.executeBetween(null, 0, 'A'))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining("executeBetweenRange");
    }

    @Test
    @DisplayName("unicode character handling")
    void unicodeCharacterHandling() {
        CharTypeHandler handler = new CharTypeHandler();
        char[] values = {'A', '\u3042', '\u3044', '\u3046', 'Z'};
        GeneratedTable table = new FakeCharTable(values);

        Selection selection = handler.executeCondition(table, 0, LogicalQuery.Operator.EQ, '\u3042', false);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(1);
    }

    private static final class FakeCharTable implements GeneratedTable {
        private final int[] values;

        private FakeCharTable(char[] chars) {
            this.values = new int[chars.length];
            for (int i = 0; i < chars.length; i++) {
                values[i] = chars[i];
            }
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_CHAR; }
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
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) {
            return reader.get();
        }
        
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
