package io.memris.runtime.codegen;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionRowMatcherCoverageTest {

    @Test
    void eqIntMatcherNullPresenceAndEqualityPaths() throws Exception {
        GeneratedTable presentTable = new StubTable(true, 7, 70L);
        GeneratedTable nullTable = new StubTable(false, 7, 70L);
        Object matcher = newInstance(
                ConditionRowEvaluatorGenerator.EqIntMatcher.class,
                int.class, int.class, byte.class, boolean.class,
                0, 0, TypeCodes.TYPE_INT, false);
        Method matches = matcher.getClass().getMethod("matches", GeneratedTable.class, int.class, Object[].class);

        assertThat((boolean) matches.invoke(matcher, nullTable, 0, new Object[] { null })).isTrue();
        assertThat((boolean) matches.invoke(matcher, nullTable, 0, new Object[] { 7 })).isFalse();
        assertThat((boolean) matches.invoke(matcher, presentTable, 0, new Object[] { 7 })).isTrue();
    }

    @Test
    void rangeIntMatcherCoversOperatorsAndFallbacks() throws Exception {
        Object matcherGt = newMatcher(ConditionRowEvaluatorGenerator.RangeIntMatcher.class, LogicalQuery.Operator.GT);
        Object matcherGte = newMatcher(ConditionRowEvaluatorGenerator.RangeIntMatcher.class, LogicalQuery.Operator.GTE);
        Object matcherLt = newMatcher(ConditionRowEvaluatorGenerator.RangeIntMatcher.class, LogicalQuery.Operator.LT);
        Object matcherLte = newMatcher(ConditionRowEvaluatorGenerator.RangeIntMatcher.class, LogicalQuery.Operator.LTE);
        Object matcherDefault = newMatcher(ConditionRowEvaluatorGenerator.RangeIntMatcher.class, LogicalQuery.Operator.EQ);
        Method matches = matcherGt.getClass().getMethod("matches", GeneratedTable.class, int.class, Object[].class);
        GeneratedTable table = new StubTable(true, 7, 70L);

        assertThat((boolean) matches.invoke(matcherGt, table, 0, new Object[] { 6 })).isTrue();
        assertThat((boolean) matches.invoke(matcherGte, table, 0, new Object[] { 7 })).isTrue();
        assertThat((boolean) matches.invoke(matcherLt, table, 0, new Object[] { 8 })).isTrue();
        assertThat((boolean) matches.invoke(matcherLte, table, 0, new Object[] { 7 })).isTrue();
        assertThat((boolean) matches.invoke(matcherDefault, table, 0, new Object[] { 7 })).isFalse();
        assertThat((boolean) matches.invoke(matcherGt, table, 0, new Object[] { null })).isFalse();
        assertThat((boolean) matches.invoke(matcherGt, new StubTable(false, 7, 70L), 0, new Object[] { 1 })).isFalse();
    }

    @Test
    void rangeLongMatcherCoversOperatorsAndFallbacks() throws Exception {
        Object matcherGt = newMatcher(ConditionRowEvaluatorGenerator.RangeLongMatcher.class, LogicalQuery.Operator.GT);
        Object matcherGte = newMatcher(ConditionRowEvaluatorGenerator.RangeLongMatcher.class, LogicalQuery.Operator.GTE);
        Object matcherLt = newMatcher(ConditionRowEvaluatorGenerator.RangeLongMatcher.class, LogicalQuery.Operator.LT);
        Object matcherLte = newMatcher(ConditionRowEvaluatorGenerator.RangeLongMatcher.class, LogicalQuery.Operator.LTE);
        Object matcherDefault = newMatcher(ConditionRowEvaluatorGenerator.RangeLongMatcher.class, LogicalQuery.Operator.EQ);
        Method matches = matcherGt.getClass().getMethod("matches", GeneratedTable.class, int.class, Object[].class);
        GeneratedTable table = new StubTable(true, 7, 70L);

        assertThat((boolean) matches.invoke(matcherGt, table, 0, new Object[] { 60L })).isTrue();
        assertThat((boolean) matches.invoke(matcherGte, table, 0, new Object[] { 70L })).isTrue();
        assertThat((boolean) matches.invoke(matcherLt, table, 0, new Object[] { 80L })).isTrue();
        assertThat((boolean) matches.invoke(matcherLte, table, 0, new Object[] { 70L })).isTrue();
        assertThat((boolean) matches.invoke(matcherDefault, table, 0, new Object[] { 70L })).isFalse();
        assertThat((boolean) matches.invoke(matcherGt, table, 0, new Object[] { null })).isFalse();
        assertThat((boolean) matches.invoke(matcherGt, new StubTable(false, 7, 70L), 0, new Object[] { 1L })).isFalse();
    }

    private static Object newMatcher(Class<?> type, LogicalQuery.Operator operator) throws Exception {
        return newInstance(type, int.class, int.class, byte.class, LogicalQuery.Operator.class, boolean.class,
                0, 0, TypeCodes.TYPE_LONG, operator, false);
    }

    private static Object newInstance(Class<?> type, Class<?> a, Class<?> b, Class<?> c, Class<?> d,
            Object av, Object bv, Object cv, Object dv) throws Exception {
        Constructor<?> ctor = type.getDeclaredConstructor(a, b, c, d);
        ctor.setAccessible(true);
        return ctor.newInstance(av, bv, cv, dv);
    }

    private static Object newInstance(Class<?> type, Class<?> a, Class<?> b, Class<?> c, Class<?> d, Class<?> e,
            Object av, Object bv, Object cv, Object dv, Object ev) throws Exception {
        Constructor<?> ctor = type.getDeclaredConstructor(a, b, c, d, e);
        ctor.setAccessible(true);
        return ctor.newInstance(av, bv, cv, dv, ev);
    }

    private record StubTable(boolean present, int intValue, long longValue) implements GeneratedTable {
        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return 0; }
        @Override public long allocatedCount() { return 1; }
        @Override public long liveCount() { return 1; }
        @Override public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) { return reader.get(); }
        @Override public long lookupById(long id) { return -1; }
        @Override public long lookupByIdString(String id) { return -1; }
        @Override public void removeById(long id) { }
        @Override public long insertFrom(Object[] values) { return 0; }
        @Override public void tombstone(long ref) { }
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
        @Override public int[] scanAll() { return new int[] { 0 }; }
        @Override public long readLong(int columnIndex, int rowIndex) { return longValue; }
        @Override public int readInt(int columnIndex, int rowIndex) { return intValue; }
        @Override public String readString(int columnIndex, int rowIndex) { return null; }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return present; }
    }
}
