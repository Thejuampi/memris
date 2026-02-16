package io.memris.runtime.handler;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

class BigDecimalTypeHandlerTest {
    private static final BigDecimalTypeHandler HANDLER = new BigDecimalTypeHandler();

    static class TestTable implements GeneratedTable {
        private final List<String> values;
        TestTable(List<String> values) { this.values = values; }
        @Override public String readString(int columnIndex, int rowIndex) { return values.get(rowIndex); }
        @Override public int[] scanEqualsString(int columnIndex, String value) {
            List<Integer> idx = new ArrayList<>();
            for (int i = 0; i < values.size(); i++) if (values.get(i).equals(value)) idx.add(i);
            return idx.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanInString(int columnIndex, String[] in) {
            Set<String> set = new HashSet<>(Arrays.asList(in));
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
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanAll() { int[] arr = new int[values.size()]; for (int i = 0; i < arr.length; i++) arr[i] = i; return arr; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
    }

    @Test void typeCodeAndJavaType() {
        assertThat(HANDLER.getTypeCode()).isNotZero();
        assertThat(HANDLER.getJavaType()).isEqualTo(BigDecimal.class);
    }

    @Test void convertValueAcceptsBigDecimal() {
        BigDecimal b = new BigDecimal("1234567890.1234567890");
        assertThat(HANDLER.convertValue(b)).isSameAs(b);
    }

    @Test void convertValueRejectsNonBigDecimal() {
        assertThatThrownBy(() -> HANDLER.convertValue("not a bigdecimal")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test void executeEqualsFindsMatching() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        Selection sel = HANDLER.executeEquals(t, 0, b, false);
        assertThat(sel.size()).isEqualTo(1);
        assertThat(sel.toIntArray()).containsExactly(1);
    }

    @Test void executeIn() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        Selection sel = HANDLER.executeIn(t, 0, b);
        assertThat(sel.size()).isEqualTo(1);
        assertThat(sel.toIntArray()).containsExactly(1);
    }

    @Test void executeGreaterThanThrows() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        assertThatThrownBy(() -> HANDLER.executeGreaterThan(t, 0, b)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void executeGreaterThanOrEqualThrows() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        assertThatThrownBy(() -> HANDLER.executeGreaterThanOrEqual(t, 0, b)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void executeLessThanThrows() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        assertThatThrownBy(() -> HANDLER.executeLessThan(t, 0, b)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void executeLessThanOrEqualThrows() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        assertThatThrownBy(() -> HANDLER.executeLessThanOrEqual(t, 0, b)).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test void executeBetweenThrows() {
        BigDecimal b = new BigDecimal("123.45");
        TestTable t = new TestTable(List.of("1.0", "123.45", "999.99"));
        assertThatThrownBy(() -> HANDLER.executeBetween(t, 0, b)).isInstanceOf(UnsupportedOperationException.class);
    }
}
