package io.memris.runtime.handler;

import io.memris.core.TypeCodes;
import io.memris.query.LogicalQuery;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IntAndLongTypeHandlerTest {
    // Ownership: canonical int/long handler semantic matrix (metadata/convert/equals/between/in).
    // Out-of-scope: single-operator dispatch smoke tests in type-specific test classes.

    @ParameterizedTest(name = "metadata for {0}")
    @MethodSource("specs")
    void metadata(HandlerSpec spec) {
        assertThat(spec.metadata()).usingRecursiveComparison().isEqualTo(spec.expectedMetadata());
    }

    @ParameterizedTest(name = "convert value for {0}")
    @MethodSource("specs")
    void convertValue(HandlerSpec spec) {
        assertThat(spec.convert(spec.convertInput())).isEqualTo(spec.convertExpected());
    }

    @ParameterizedTest(name = "convert rejects non-numeric for {0}")
    @MethodSource("specs")
    void convertRejectsNonNumeric(HandlerSpec spec) {
        assertThatThrownBy(() -> spec.convert("not a number"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot convert");
    }

    @ParameterizedTest(name = "equals selection for {0}")
    @MethodSource("specs")
    void executeEquals(HandlerSpec spec) {
        assertThat(spec.executeEqualsSelection().toIntArray()).containsExactlyInAnyOrder(spec.expectedEqualsRows());
    }

    @ParameterizedTest(name = "between range selection for {0}")
    @MethodSource("specs")
    void executeBetweenRange(HandlerSpec spec) {
        assertThat(spec.executeBetweenRangeSelection().toIntArray()).containsExactlyInAnyOrder(spec.expectedBetweenRows());
    }

    @ParameterizedTest(name = "in selection for {0}")
    @MethodSource("specs")
    void executeIn(HandlerSpec spec) {
        assertThat(spec.executeInSelection().toIntArray()).containsExactlyInAnyOrder(spec.expectedInRows());
    }

    @ParameterizedTest(name = "between operator routing for {0}")
    @MethodSource("specs")
    void executeBetweenThrows(HandlerSpec spec) {
        assertThatThrownBy(spec::executeBetween)
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessageContaining(spec.expectedBetweenMessage());
    }

    private static Stream<HandlerSpec> specs() {
        return Stream.of(
                HandlerSpec.intSpec(),
                HandlerSpec.longSpec()
        );
    }

    private record Metadata(byte typeCode, Class<?> javaType) {}

    private record HandlerSpec(
            String name,
            Supplier<Object> factory,
            Metadata expectedMetadata,
            Object convertInput,
            Object convertExpected,
            String expectedBetweenMessage,
            GeneratedTable equalsTable,
            Object equalsValue,
            int[] expectedEqualsRows,
            GeneratedTable betweenTable,
            Object betweenMin,
            Object betweenMax,
            int[] expectedBetweenRows,
            GeneratedTable inTable,
            Object inValues,
            int[] expectedInRows
    ) {
        static HandlerSpec intSpec() {
            return new HandlerSpec(
                    "int",
                    IntTypeHandler::new,
                    new Metadata(TypeCodes.TYPE_INT, Integer.class),
                    42L,
                    42,
                    "executeBetweenRange",
                    new IntBackedTable(TypeCodes.TYPE_INT, new int[]{10, 20, 10, 30, 10}),
                    10,
                    new int[]{0, 2, 4},
                    new IntBackedTable(TypeCodes.TYPE_INT, new int[]{10, 20, 30, 40, 50}),
                    25,
                    45,
                    new int[]{2, 3},
                    new IntBackedTable(TypeCodes.TYPE_INT, new int[]{10, 20, 30, 40, 50}),
                    new int[]{20, 40},
                    new int[]{1, 3}
            );
        }

        static HandlerSpec longSpec() {
            return new HandlerSpec(
                    "long",
                    LongTypeHandler::new,
                    new Metadata(TypeCodes.TYPE_LONG, Long.class),
                    42,
                    42L,
                    "executeBetweenRange",
                    new LongBackedTable(TypeCodes.TYPE_LONG, new long[]{100L, 200L, 100L, 300L, 100L}),
                    100L,
                    new int[]{0, 2, 4},
                    new LongBackedTable(TypeCodes.TYPE_LONG, new long[]{100L, 200L, 300L, 400L, 500L}),
                    200L,
                    400L,
                    new int[]{1, 2, 3},
                    new LongBackedTable(TypeCodes.TYPE_LONG, new long[]{100L, 200L, 300L, 400L, 500L}),
                    new long[]{200L, 400L},
                    new int[]{1, 3}
            );
        }

        Metadata metadata() {
            var handler = factory.get();
            if (handler instanceof IntTypeHandler intHandler) {
                return new Metadata(intHandler.getTypeCode(), intHandler.getJavaType());
            }
            var longHandler = (LongTypeHandler) handler;
            return new Metadata(longHandler.getTypeCode(), longHandler.getJavaType());
        }

        Object convert(Object value) {
            var handler = factory.get();
            if (handler instanceof IntTypeHandler intHandler) {
                return intHandler.convertValue(value);
            }
            return ((LongTypeHandler) handler).convertValue(value);
        }

        Selection executeEqualsSelection() {
            var handler = factory.get();
            if (handler instanceof IntTypeHandler intHandler) {
                return intHandler.executeEquals(equalsTable, 0, (int) equalsValue, false);
            }
            return ((LongTypeHandler) handler).executeEquals(equalsTable, 0, (long) equalsValue, false);
        }

        Selection executeBetweenRangeSelection() {
            var handler = factory.get();
            if (handler instanceof IntTypeHandler intHandler) {
                return intHandler.executeBetweenRange(betweenTable, 0, (int) betweenMin, (int) betweenMax);
            }
            return ((LongTypeHandler) handler).executeBetweenRange(betweenTable, 0, (long) betweenMin, (long) betweenMax);
        }

        Selection executeInSelection() {
            var handler = factory.get();
            if (handler instanceof IntTypeHandler intHandler) {
                return intHandler.executeIn(inTable, 0, (int[]) inValues);
            }
            return ((LongTypeHandler) handler).executeIn(inTable, 0, (long[]) inValues);
        }

        void executeBetween() {
            var handler = factory.get();
            if (handler instanceof IntTypeHandler intHandler) {
                intHandler.executeBetween(null, 0, 1);
                return;
            }
            ((LongTypeHandler) handler).executeBetween(null, 0, 1L);
        }
    }

    private static final class IntBackedTable implements GeneratedTable {
        private final byte typeCode;
        private final int[] values;

        private IntBackedTable(byte typeCode, int[] values) {
            this.typeCode = typeCode;
            this.values = values;
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return typeCode; }
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
        @Override public int[] scanEqualsInt(int columnIndex, int value) { return scanEquals(value); }
        @Override public int[] scanEqualsString(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { return scanBetween(min, max); }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInLong(int columnIndex, long[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInInt(int columnIndex, int[] targets) { return scanIn(targets); }
        @Override public int[] scanInString(int columnIndex, String[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanAll() { return allRows(values.length); }
        @Override public long readLong(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public int readInt(int columnIndex, int rowIndex) { return values[rowIndex]; }
        @Override public String readString(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }

        private int[] scanEquals(int target) {
            var matches = new int[values.length];
            var count = 0;
            for (var index = 0; index < values.length; index++) {
                if (values[index] == target) {
                    matches[count++] = index;
                }
            }
            return trim(matches, count);
        }

        private int[] scanBetween(int min, int max) {
            var matches = new int[values.length];
            var count = 0;
            for (var index = 0; index < values.length; index++) {
                var value = values[index];
                if (value >= min && value <= max) {
                    matches[count++] = index;
                }
            }
            return trim(matches, count);
        }

        private int[] scanIn(int[] targets) {
            var matches = new int[values.length];
            var count = 0;
            for (var index = 0; index < values.length; index++) {
                var candidate = values[index];
                for (var target : targets) {
                    if (candidate == target) {
                        matches[count++] = index;
                        break;
                    }
                }
            }
            return trim(matches, count);
        }
    }

    private static final class LongBackedTable implements GeneratedTable {
        private final byte typeCode;
        private final long[] values;

        private LongBackedTable(byte typeCode, long[] values) {
            this.typeCode = typeCode;
            this.values = values;
        }

        @Override public int columnCount() { return 1; }
        @Override public byte typeCodeAt(int columnIndex) { return typeCode; }
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
        @Override public int[] scanEqualsLong(int columnIndex, long value) { return scanEquals(value); }
        @Override public int[] scanEqualsInt(int columnIndex, int value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsString(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenInt(int columnIndex, int min, int max) { throw new UnsupportedOperationException(); }
        @Override public int[] scanBetweenLong(int columnIndex, long min, long max) { return scanBetween(min, max); }
        @Override public int[] scanInLong(int columnIndex, long[] values) { return scanIn(values); }
        @Override public int[] scanInInt(int columnIndex, int[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanInString(int columnIndex, String[] values) { throw new UnsupportedOperationException(); }
        @Override public int[] scanAll() { return allRows(values.length); }
        @Override public long readLong(int columnIndex, int rowIndex) { return values[rowIndex]; }
        @Override public int readInt(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public String readString(int columnIndex, int rowIndex) { throw new UnsupportedOperationException(); }
        @Override public boolean isPresent(int columnIndex, int rowIndex) { return true; }

        private int[] scanEquals(long target) {
            var matches = new int[values.length];
            var count = 0;
            for (var index = 0; index < values.length; index++) {
                if (values[index] == target) {
                    matches[count++] = index;
                }
            }
            return trim(matches, count);
        }

        private int[] scanBetween(long min, long max) {
            var matches = new int[values.length];
            var count = 0;
            for (var index = 0; index < values.length; index++) {
                var value = values[index];
                if (value >= min && value <= max) {
                    matches[count++] = index;
                }
            }
            return trim(matches, count);
        }

        private int[] scanIn(long[] targets) {
            var matches = new int[values.length];
            var count = 0;
            for (var index = 0; index < values.length; index++) {
                var candidate = values[index];
                for (var target : targets) {
                    if (candidate == target) {
                        matches[count++] = index;
                        break;
                    }
                }
            }
            return trim(matches, count);
        }
    }

    private static int[] allRows(int size) {
        var rows = new int[size];
        for (var index = 0; index < size; index++) {
            rows[index] = index;
        }
        return rows;
    }

    private static int[] trim(int[] values, int count) {
        var trimmed = new int[count];
        System.arraycopy(values, 0, trimmed, 0, count);
        return trimmed;
    }
}
