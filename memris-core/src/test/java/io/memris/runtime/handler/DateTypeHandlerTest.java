package io.memris.runtime.handler;

import io.memris.runtime.handler.DateTypeHandler;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.*;

class DateTypeHandlerTest {
    private static final DateTypeHandler HANDLER = new DateTypeHandler();

    // Minimal GeneratedTable stub for long column
    static class TestTable implements io.memris.storage.GeneratedTable {
        private final List<Long> values;
        TestTable(List<Long> values) { this.values = values; }
        @Override public long readLong(int columnIndex, int rowIndex) { return values.get(rowIndex); }
        @Override public int[] scanEqualsLong(int columnIndex, long value) {
            List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) if (values.get(i) == value) idx.add(i);
            return idx.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) {
            List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) {
                long v = values.get(i);
                if (v >= min && v <= max) idx.add(i);
            }
            return idx.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanInLong(int columnIndex, long[] in) {
            Set<Long> set = new HashSet<>();
            for (long l : in) set.add(l);
            List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) if (set.contains(values.get(i))) idx.add(i);
            return idx.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return values.size(); }
        @Override public long liveCount() { return values.size(); }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) {}
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) {}
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { int[] arr = new int[values.size()]; for (int i = 0; i < arr.length; i++) arr[i] = i; return arr; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
    }

    @Test void typeCodeAndJavaType() {
        assertThat(HANDLER.getTypeCode()).isNotZero();
        assertThat(HANDLER.getJavaType()).isEqualTo(Date.class);
    }

    @Test void convertValueAcceptsDate() {
        Date d = new Date(1234L);
        assertThat(HANDLER.convertValue(d)).isSameAs(d);
    }

    @Test void convertValueRejectsNonDate() {
        assertThatThrownBy(() -> HANDLER.convertValue("not a date")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void executeEqualsFindsMatching() {
        Date d = new Date(1000L);
        TestTable t = new TestTable(List.of(500L, 1000L, 2000L));
        Selection sel = HANDLER.executeEquals(t, 0, d, false);
        assertThat(sel.size()).isEqualTo(1);
        assertThat(sel.toIntArray()).containsExactly(1);
    }

    @Test void executeGreaterThan() {
        Date d = new Date(1000L);
        TestTable t = new TestTable(List.of(500L, 1000L, 1500L, 2000L));
        Selection sel = HANDLER.executeGreaterThan(t, 0, d);
        assertThat(sel.size()).isEqualTo(2);
        assertThat(sel.toIntArray()).contains(2, 3);
    }

    @Test void executeGreaterThanOrEqual() {
        Date d = new Date(1000L);
        TestTable t = new TestTable(List.of(500L, 1000L, 1500L));
        Selection sel = HANDLER.executeGreaterThanOrEqual(t, 0, d);
        assertThat(sel.size()).isEqualTo(2);
        assertThat(sel.toIntArray()).contains(1, 2);
    }

    @Test void executeLessThan() {
        Date d = new Date(1500L);
        TestTable t = new TestTable(List.of(500L, 1000L, 1500L, 2000L));
        Selection sel = HANDLER.executeLessThan(t, 0, d);
        assertThat(sel.size()).isEqualTo(2);
        assertThat(sel.toIntArray()).contains(0, 1);
    }

    @Test void executeLessThanOrEqual() {
        Date d = new Date(1500L);
        TestTable t = new TestTable(List.of(500L, 1000L, 1500L, 2000L));
        Selection sel = HANDLER.executeLessThanOrEqual(t, 0, d);
        assertThat(sel.size()).isEqualTo(3);
        assertThat(sel.toIntArray()).contains(0, 1, 2);
    }

    @Test void executeIn() {
        Date d = new Date(1000L);
        TestTable t = new TestTable(List.of(500L, 1000L, 2000L));
        Selection sel = HANDLER.executeIn(t, 0, d);
        assertThat(sel.size()).isEqualTo(1);
        assertThat(sel.toIntArray()).containsExactly(1);
    }

    @Test void executeBetweenThrows() {
        Date d = new Date(1000L);
        TestTable t = new TestTable(List.of(500L, 1000L, 2000L));
        assertThatThrownBy(() -> HANDLER.executeBetween(t, 0, d)).isInstanceOf(UnsupportedOperationException.class);
    }
}
