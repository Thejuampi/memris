package io.memris.runtime.codegen;

import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ConditionRowEvaluatorGeneratorCoverageTest {
    private RuntimeExecutorGenerator runtimeGenerator;
    private ConditionRowEvaluatorGenerator conditionGenerator;

    @AfterEach
    void tearDown() {
        runtimeGenerator = new RuntimeExecutorGenerator();
        conditionGenerator = new ConditionRowEvaluatorGenerator(runtimeGenerator);
        runtimeGenerator.clearCache();
        conditionGenerator.clearCache();
    }

    @Test
    void shouldGenerateEqNeAndNullMatchers() {
        runtimeGenerator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        conditionGenerator = new ConditionRowEvaluatorGenerator(runtimeGenerator);
        var table = new StubTable();
        table.present(0, true);
        table.stringValue(0, "Alice");
        table.intValue(1, 33);
        table.longValue(2, 44L);

        var eqIgnoreCase = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_STRING, LogicalQuery.Operator.IGNORE_CASE_EQ, 0),
                false);
        var neInt = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(1, TypeCodes.TYPE_INT, LogicalQuery.Operator.NE, 0),
                false);
        var isNull = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_STRING, LogicalQuery.Operator.IS_NULL, 0),
                false);
        var notNullPrimitive = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_LONG, LogicalQuery.Operator.NOT_NULL, 0),
                true);

        assertThat(eqIgnoreCase.matches(table, 0, new Object[] { "alice" })).isTrue();
        assertThat(neInt.matches(table, 0, new Object[] { 99 })).isTrue();

        table.present(0, false);
        assertThat(isNull.matches(table, 0, new Object[0])).isTrue();
        assertThat(notNullPrimitive.matches(table, 0, new Object[0])).isTrue();
    }

    @Test
    void shouldGenerateRangeBetweenAndInMatchersAcrossTypes() {
        runtimeGenerator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        conditionGenerator = new ConditionRowEvaluatorGenerator(runtimeGenerator);
        var table = new StubTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.intValue(0, 50);
        table.longValue(1, 1_000L);
        table.stringValue(2, "x");

        var gt = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.GT, 0),
                false);
        var before = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(1, TypeCodes.TYPE_LONG, LogicalQuery.Operator.BEFORE, 0),
                false);
        var between = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.BETWEEN, 0),
                false);
        var inString = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_STRING, LogicalQuery.Operator.IN, 0),
                false);
        var inLong = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(1, TypeCodes.TYPE_LONG, LogicalQuery.Operator.IN, 0),
                false);
        var notInInt = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.NOT_IN, 0),
                false);

        assertThat(gt.matches(table, 0, new Object[] { 40 })).isTrue();
        assertThat(before.matches(table, 0, new Object[] { 1_200L })).isTrue();
        assertThat(between.matches(table, 0, new Object[] { 45, 55 })).isTrue();
        assertThat(inString.matches(table, 0, new Object[] { List.of("a", "x", "z") })).isTrue();
        assertThat(inLong.matches(table, 0, new Object[] { new Object[] { 5L, 1_000L } })).isTrue();
        assertThat(notInInt.matches(table, 0, new Object[] { new int[] { 1, 2, 3 } })).isTrue();
    }

    @Test
    void shouldHandleUnsupportedOperatorCachingAndCodegenPath() {
        runtimeGenerator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(true).build());
        conditionGenerator = new ConditionRowEvaluatorGenerator(runtimeGenerator);
        var table = new StubTable();
        table.present(0, true);
        table.stringValue(0, "A");

        var unsupported = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_STRING, LogicalQuery.Operator.CONTAINING, 0),
                false);
        assertThat(unsupported).isNull();

        var keyCondition = CompiledQuery.CompiledCondition.of(0,
                TypeCodes.TYPE_STRING,
                LogicalQuery.Operator.EQ,
                0,
                false,
                LogicalQuery.Combinator.AND);
        var first = conditionGenerator.generate(keyCondition, false);
        var second = conditionGenerator.generate(keyCondition, false);

        assertThat(first).isSameAs(second);
        assertThat(first.matches(table, 0, new Object[] { "A" })).isTrue();
        assertThat(first.matches(table, 0, new Object[0])).isFalse();
    }

    @Test
    void shouldDecodeInArgumentsAcrossArrayAndTemporalShapes() {
        runtimeGenerator = new RuntimeExecutorGenerator(MemrisConfiguration.builder().codegenEnabled(false).build());
        conditionGenerator = new ConditionRowEvaluatorGenerator(runtimeGenerator);
        var table = new StubTable();
        table.present(0, true);
        table.present(1, true);
        table.present(2, true);
        table.present(3, true);
        table.intValue(0, 1);
        table.intValue(1, (int) 'C');
        table.longValue(2, 10L);
        table.stringValue(3, "a");

        var inBoolean = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_BOOLEAN, LogicalQuery.Operator.IN, 0),
                false);
        var inChar = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(1, TypeCodes.TYPE_CHAR, LogicalQuery.Operator.IN, 0),
                false);
        var inInstant = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_INSTANT, LogicalQuery.Operator.IN, 0),
                false);
        var inDate = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_DATE, LogicalQuery.Operator.IN, 0),
                false);
        var inDouble = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_DOUBLE, LogicalQuery.Operator.IN, 0),
                false);
        var inString = conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(3, TypeCodes.TYPE_STRING, LogicalQuery.Operator.IN, 0),
                false);

        assertThat(inBoolean.matches(table, 0, new Object[] { new Object[] { true, false } })).isTrue();
        assertThat(inChar.matches(table, 0, new Object[] { List.of('A', "C") })).isTrue();
        assertThat(inInstant.matches(table, 0, new Object[] { new Object[] { Instant.ofEpochMilli(10L) } })).isTrue();
        assertThat(inDate.matches(table, 0, new Object[] { List.of(new Date(10L)) })).isTrue();

        table.longValue(2, io.memris.core.FloatEncoding.doubleToSortableLong(2.5d));
        assertThat(inDouble.matches(table, 0, new Object[] { new Object[] { 2.5d } })).isTrue();

        assertThat(conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_LOCAL_DATE, LogicalQuery.Operator.IN, 0),
                false).matches(table, 0, new Object[] { new Object[] { LocalDate.ofEpochDay(3) } })).isFalse();
        table.longValue(2, LocalDate.ofEpochDay(3).toEpochDay());
        assertThat(conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_LOCAL_DATE, LogicalQuery.Operator.IN, 0),
                false).matches(table, 0, new Object[] { new Object[] { LocalDate.ofEpochDay(3) } })).isTrue();

        LocalDateTime ldt = LocalDateTime.ofEpochSecond(2, 0, ZoneOffset.UTC);
        table.longValue(2, ldt.toInstant(ZoneOffset.UTC).toEpochMilli());
        assertThat(conditionGenerator.generate(
                CompiledQuery.CompiledCondition.of(2, TypeCodes.TYPE_LOCAL_DATE_TIME, LogicalQuery.Operator.IN, 0),
                false).matches(table, 0, new Object[] { new Object[] { ldt } })).isTrue();

        assertThat(inString.matches(table, 0, new Object[] { new String[] { "a" } })).isTrue();
        assertThat(inString.matches(table, 0, new Object[] { new Object[] { "a", null } })).isTrue();
    }

    private static final class StubTable implements GeneratedTable {
        private final Map<Integer, Boolean> present = new HashMap<>();
        private final Map<Integer, Integer> ints = new HashMap<>();
        private final Map<Integer, Long> longs = new HashMap<>();
        private final Map<Integer, String> strings = new HashMap<>();

        void present(int column, boolean value) {
            present.put(column, value);
        }

        void intValue(int column, int value) {
            ints.put(column, value);
        }

        void longValue(int column, long value) {
            longs.put(column, value);
        }

        void stringValue(int column, String value) {
            strings.put(column, value);
        }

        @Override
        public int columnCount() {
            return 0;
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return 0;
        }

        @Override
        public long allocatedCount() {
            return 0;
        }

        @Override
        public long liveCount() {
            return 0;
        }

        @Override
        public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) {
            return reader.get();
        }

        @Override
        public long lookupById(long id) {
            return -1;
        }

        @Override
        public long lookupByIdString(String id) {
            return -1;
        }

        @Override
        public void removeById(long id) {
        }

        @Override
        public long insertFrom(Object[] values) {
            return 0;
        }

        @Override
        public void tombstone(long ref) {
        }

        @Override
        public boolean isLive(long ref) {
            return true;
        }

        @Override
        public long currentGeneration() {
            return 0;
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return 1;
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            return new int[0];
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            return new int[0];
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            return new int[0];
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return new int[0];
        }

        @Override
        public int[] scanAll() {
            return new int[0];
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            return longs.getOrDefault(columnIndex, 0L);
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return ints.getOrDefault(columnIndex, 0);
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            return strings.get(columnIndex);
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return present.getOrDefault(columnIndex, false);
        }
    }
}


