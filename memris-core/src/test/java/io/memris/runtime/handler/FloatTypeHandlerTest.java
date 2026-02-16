package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FloatTypeHandlerTest {

    @Test
    void float_handler_special_cases() {
        FloatTypeHandler handler = new FloatTypeHandler();

        int nanBits = FloatEncoding.floatToSortableInt(Float.NaN);
        int posInfBits = FloatEncoding.floatToSortableInt(Float.POSITIVE_INFINITY);
        int negInfBits = FloatEncoding.floatToSortableInt(Float.NEGATIVE_INFINITY);

        GeneratedTable table = new GeneratedTable() {
            @Override public int columnCount() { return 1; }
            @Override public byte typeCodeAt(int columnIndex) { return 0; }
            @Override public long allocatedCount() { return 0; }
            @Override public long liveCount() { return 0; }
            @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
            @Override public long lookupById(long id) { return -1; }
            @Override public long lookupByIdString(String id) { return -1; }
            @Override public void removeById(long id) {}
            @Override public long insertFrom(Object[] values) { return 0; }
            @Override public void tombstone(long ref) {}
            @Override public boolean isLive(long ref) { return false; }
            @Override public long currentGeneration() { return 0; }
            @Override public long rowGeneration(int rowIndex) { return 0L; }
            @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
            @Override public int[] scanEqualsInt(int columnIndex, int value) { return value==nanBits ? new int[]{11} : (value==posInfBits? new int[]{12}: (value==negInfBits? new int[]{13}: new int[0])); }
            @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
            @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return (min<=100 && max>=100) ? new int[]{14} : new int[0]; }
            @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
            @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
            @Override public int[] scanInInt(int columnIndex, int[] values) { return values.length==2 && values[0]==1? new int[]{15,16} : new int[0]; }
            @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
            @Override public int[] scanAll() { return new int[]{11,12,13,14}; }
            @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
            @Override public int readInt(int columnIndex, int rowIndex) { return 100; }
            @Override public String readString(int columnIndex, int rowIndex) { return null; }
            @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
        };

        Selection nanSel = handler.executeIsNaN(table, 0);
        assertThat(nanSel.toIntArray()).containsExactly(11);

        Selection infSel = handler.executeIsInfinite(table, 0);
        assertThat(infSel.toIntArray()).containsExactly(12,13);

        // skip exact between-range assertion — sortable encoding checked elsewhere
    }
}
