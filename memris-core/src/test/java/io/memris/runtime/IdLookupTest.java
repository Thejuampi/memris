package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class IdLookupTest {

    static class TestTable implements GeneratedTable {
        private final long longResult;
        private final long stringResult;
        TestTable(long longResult, long stringResult) { this.longResult = longResult; this.stringResult = stringResult; }
        @Override public int columnCount() { return 0; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return 0; }
        @Override public long liveCount() { return 0; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return longResult; }
        @Override public long lookupByIdString(String id) { return stringResult; }
        @Override public void removeById(long id) {}
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) {}
        @Override public boolean isLive(long ref) { return true; }
        @Override public long currentGeneration() { return 0; }
        @Override public long rowGeneration(int rowIndex) { return 0; }
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return new int[0]; }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return new int[0]; }
        @Override public int[] scanEqualsString(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { return new int[0]; }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return new int[0]; }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return new int[0]; }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return new int[0]; }
        @Override public int[] scanInInt(int columnIndex, int[] values) { return new int[0]; }
        @Override public int[] scanInString(int columnIndex, String[] values) { return new int[0]; }
        @Override public int[] scanAll() { return new int[0]; }
        @Override public int readInt(int columnIndex, int rowIndex) { return 0; }
        @Override public long readLong(int columnIndex, int rowIndex) { return 0; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
    }

    @Test void forTypeCode_string_callsLookupByIdString() {
        TestTable t = new TestTable(111L, 222L);
        IdLookup lookup = IdLookup.forTypeCode(TypeCodes.TYPE_STRING);
        long res = lookup.lookup(t, "abc");
        assertThat(res).isEqualTo(222L);
    }

    @Test void forTypeCode_default_usesLongLookup() {
        TestTable t = new TestTable(333L, 444L);
        IdLookup lookup = IdLookup.forTypeCode((byte)0);
        long res = lookup.lookup(t, 99L);
        assertThat(res).isEqualTo(333L);
    }
}
