package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class StringTypeHandlerTest {
    private final StringTypeHandler handler = new StringTypeHandler();

    @Test
    void testMetadata() {
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_STRING);
        assertThat(handler.getJavaType()).isEqualTo(String.class);
    }

    @Test
    void testConvertValue() {
        assertThat(handler.convertValue("foo")).isEqualTo("foo");
        assertThatThrownBy(() -> handler.convertValue(123)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEqualsSelection() {
        var table = new StringTable(new String[]{"a", "b", "A", null, "b"});
        Selection sel = handler.executeEquals(table, 0, "a", false);
        assertThat(sel.toIntArray()).containsExactly(0);
        sel = handler.executeEquals(table, 0, "A", true);
        assertThat(sel.toIntArray()).containsExactly(0, 2);
    }

    @Test
    void testInSelection() {
        var table = new StringTable(new String[]{"a", "b", "c", "b", null});
        Selection sel = handler.executeIn(table, 0, new String[]{"b", "c"});
        assertThat(sel.toIntArray()).containsExactly(1, 2, 3);
    }

    @Test
    void testIsNull() {
        var table = new StringTable(new String[]{"a", null, "b"});
        Selection sel = handler.executeIsNull(table, 0);
        assertThat(sel.toIntArray()).containsExactly(1);
    }

    @Test
    void testLike() {
        var table = new StringTable(new String[]{"foo", "foobar", "barfoo", "FOO", null});
        Selection sel = handler.executeLike(table, 0, "%foo%", false);
        assertThat(sel.toIntArray()).containsExactly(0, 1, 2);
        sel = handler.executeLike(table, 0, "%foo%", true);
        assertThat(sel.toIntArray()).containsExactly(0, 1, 2, 3);
    }

    @Test
    void testContaining() {
        var table = new StringTable(new String[]{"hello", "world", "HELLO", null});
        Selection sel = handler.executeContaining(table, 0, "ell", false);
        assertThat(sel.toIntArray()).containsExactly(0);
        sel = handler.executeContaining(table, 0, "ell", true);
        assertThat(sel.toIntArray()).containsExactly(0, 2);
    }

    @Test
    void testStartingWith() {
        var table = new StringTable(new String[]{"abc", "abcd", "bcd", "ABC", null});
        Selection sel = handler.executeStartingWith(table, 0, "ab", false);
        assertThat(sel.toIntArray()).containsExactly(0, 1);
        sel = handler.executeStartingWith(table, 0, "ab", true);
        assertThat(sel.toIntArray()).containsExactly(0, 1, 3);
    }

    @Test
    void testEndingWith() {
        var table = new StringTable(new String[]{"abc", "xbc", "ABC", null});
        Selection sel = handler.executeEndingWith(table, 0, "bc", false);
        assertThat(sel.toIntArray()).containsExactly(0, 1);
        sel = handler.executeEndingWith(table, 0, "bc", true);
        assertThat(sel.toIntArray()).containsExactly(0, 1, 2);
    }

    static class StringTable implements GeneratedTable {
        private final String[] values;
        StringTable(String[] values) { this.values = values; }
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_STRING; }
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
        @Override public int[] scanEqualsInt(int columnIndex, int value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsString(int columnIndex, String value) {
            return scan((v, t) -> v != null && v.equals(t), value);
        }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return scan((v, t) -> v != null && v.equalsIgnoreCase(t), value);
        }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInLong(int columnIndex, long[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInInt(int columnIndex, int[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInString(int columnIndex, String[] targets) {
            return scan((v, arr) -> v != null && java.util.Arrays.asList(arr).contains(v), targets);
        }
        @Override public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) rows[i] = i;
            return rows;
        }
        @Override public long readLong(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public int readInt(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public String readString(int columnIndex, int rowIndex) { return values[rowIndex]; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return values[rowIndex] != null; }
        private int[] scan(java.util.function.BiPredicate<String, String> pred, String target) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) if (pred.test(values[i], target)) matches.add(i);
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
        private int[] scan(java.util.function.BiPredicate<String, String[]> pred, String[] targets) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) if (pred.test(values[i], targets)) matches.add(i);
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
    }
}
