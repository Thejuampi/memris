package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import static org.assertj.core.api.Assertions.*;

class LocalDateTimeTypeHandlerTest {
    private final LocalDateTimeTypeHandler handler = new LocalDateTimeTypeHandler();

    @Test
    void testMetadata() {
        assertThat(handler.getTypeCode()).isEqualTo(TypeCodes.TYPE_LOCAL_DATE_TIME);
        assertThat(handler.getJavaType()).isEqualTo(LocalDateTime.class);
    }

    @Test
    void testConvertValue() {
        var now = LocalDateTime.now();
        assertThat(handler.convertValue(now)).isEqualTo(now);
        assertThatThrownBy(() -> handler.convertValue("not a date")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testEqualsSelection() {
        var t1 = LocalDateTime.of(2020, 1, 1, 0, 0);
        var t2 = LocalDateTime.of(2021, 1, 1, 0, 0);
        var t3 = LocalDateTime.of(2022, 1, 1, 0, 0);
        var table = new LocalDateTimeTable(new LocalDateTime[]{t1, t2, t1, t3});
        Selection sel = handler.executeEquals(table, 0, t1, false);
        assertThat(sel.toIntArray()).containsExactly(0, 2);
    }

    @Test
    void testInSelection() {
        var t1 = LocalDateTime.of(2020, 1, 1, 0, 0);
        var t2 = LocalDateTime.of(2021, 1, 1, 0, 0);
        var t3 = LocalDateTime.of(2022, 1, 1, 0, 0);
        var table = new LocalDateTimeTable(new LocalDateTime[]{t1, t2, t3, t2});
        Selection sel = handler.executeIn(table, 0, t2);
        assertThat(sel.toIntArray()).containsExactly(1, 3);
    }

    @Test
    void testGreaterLess() {
        var t1 = LocalDateTime.of(2020, 1, 1, 0, 0);
        var t2 = LocalDateTime.of(2021, 1, 1, 0, 0);
        var t3 = LocalDateTime.of(2022, 1, 1, 0, 0);
        var table = new LocalDateTimeTable(new LocalDateTime[]{t1, t2, t3});
        Selection sel = handler.executeGreaterThan(table, 0, t1);
        assertThat(sel.toIntArray()).containsExactly(1, 2);
        sel = handler.executeLessThan(table, 0, t3);
        assertThat(sel.toIntArray()).containsExactly(0, 1);
    }

    @Test
    void testBetweenThrows() {
        var t1 = LocalDateTime.of(2020, 1, 1, 0, 0);
        var table = new LocalDateTimeTable(new LocalDateTime[]{t1});
        assertThatThrownBy(() -> handler.executeBetween(table, 0, t1)).isInstanceOf(UnsupportedOperationException.class);
    }

    static class LocalDateTimeTable implements GeneratedTable {
        private final LocalDateTime[] values;
        LocalDateTimeTable(LocalDateTime[] values) { this.values = values; }
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return TypeCodes.TYPE_LOCAL_DATE_TIME; }
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
        @Override public int[] scanEqualsLong(int columnIndex, long value) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                LocalDateTime v = values[i];
                if (v != null && v.toInstant(ZoneOffset.UTC).toEpochMilli() == value) {
                    matches.add(i);
                }
            }
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsString(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                LocalDateTime v = values[i];
                if (v != null) {
                    long epoch = v.toInstant(ZoneOffset.UTC).toEpochMilli();
                    if (epoch >= min && epoch <= max) {
                        matches.add(i);
                    }
                }
            }
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanInLong(int columnIndex, long[] targets) {
            java.util.List<Integer> matches = new java.util.ArrayList<>();
            for (int i = 0; i < values.length; i++) {
                LocalDateTime v = values[i];
                if (v != null) {
                    long epoch = v.toInstant(ZoneOffset.UTC).toEpochMilli();
                    for (long t : targets) {
                        if (epoch == t) {
                            matches.add(i);
                            break;
                        }
                    }
                }
            }
            return matches.stream().mapToInt(Integer::intValue).toArray();
        }
        @Override public int[] scanInInt(int columnIndex, int[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInString(int columnIndex, String[] targets) { throw new UnsupportedOperationException(); }
        @Override public int[] scanAll() {
            int[] rows = new int[values.length];
            for (int i = 0; i < values.length; i++) rows[i] = i;
            return rows;
        }
        @Override public long readLong(int columnIndex, int rowIndex) { return values[rowIndex].toInstant(ZoneOffset.UTC).toEpochMilli(); }
        @Override public int readInt(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public String readString(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return values[rowIndex] != null; }
    }
}
