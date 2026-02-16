package io.memris.runtime.codegen;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class BetweenLongMatcherTest {

    @Test
    void matches_between_long_behavior() throws Exception {
        int columnIndex = 2;
        int rowIndex = 0;
        long actualValue = 5L;

        GeneratedTable table = new GeneratedTable() {
            @Override public int columnCount() { return 0; }
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
            @Override public long readLong(int columnIndex, int rowIndex) { return actualValue; }
            @Override public int readInt(int columnIndex, int rowIndex) { return (int) actualValue; }
            @Override public String readString(int columnIndex, int rowIndex) { return String.valueOf(actualValue); }
            @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }
        };

        Class<?> cls = ConditionRowEvaluatorGenerator.BetweenLongMatcher.class;
        Constructor<?> ctor = cls.getDeclaredConstructor(int.class, int.class, byte.class, boolean.class);
        ctor.setAccessible(true);
        Object matcher = ctor.newInstance(columnIndex, 0, (byte)0, true);

        Method matches = cls.getMethod("matches", GeneratedTable.class, int.class, Object[].class);

        // within range
        boolean inRange = (Boolean) matches.invoke(matcher, table, rowIndex, new Object[]{1L, 10L});
        assertThat(inRange).isTrue();

        // out of range
        boolean outRange = (Boolean) matches.invoke(matcher, table, rowIndex, new Object[]{6L, 7L});
        assertThat(outRange).isFalse();

        // null argument -> false
        boolean nullArg = (Boolean) matches.invoke(matcher, table, rowIndex, new Object[]{null, 10L});
        assertThat(nullArg).isFalse();
    }
}
