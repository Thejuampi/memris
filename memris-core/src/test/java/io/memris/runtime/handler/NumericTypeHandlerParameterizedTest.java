package io.memris.runtime.handler;

import io.memris.core.FloatEncoding;
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

class NumericTypeHandlerParameterizedTest {

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
                HandlerSpec.byteSpec(),
                HandlerSpec.shortSpec(),
                HandlerSpec.floatSpec(),
                HandlerSpec.doubleSpec()
        );
    }

    private record Metadata(byte typeCode, Class<?> javaType) {
    }

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
        static HandlerSpec byteSpec() {
            return new HandlerSpec(
                    "byte",
                    ByteTypeHandler::new,
                    new Metadata(TypeCodes.TYPE_BYTE, Byte.class),
                    42,
                    (byte) 42,
                    "executeBetweenRange",
                    new IntBackedTable(TypeCodes.TYPE_BYTE, new int[]{10, 20, 10, 30, 10}),
                    (byte) 10,
                    new int[]{0, 2, 4},
                    new IntBackedTable(TypeCodes.TYPE_BYTE, new int[]{10, 20, 30, 40, 50}),
                    (byte) 25,
                    (byte) 45,
                    new int[]{2, 3},
                    new IntBackedTable(TypeCodes.TYPE_BYTE, new int[]{10, 20, 30, 40, 50}),
                    new byte[]{20, 40},
                    new int[]{1, 3}
            );
        }

        static HandlerSpec shortSpec() {
            return new HandlerSpec(
                    "short",
                    ShortTypeHandler::new,
                    new Metadata(TypeCodes.TYPE_SHORT, Short.class),
                    42L,
                    (short) 42,
                    "executeBetweenRange",
                    new IntBackedTable(TypeCodes.TYPE_SHORT, new int[]{100, 200, 100, 300, 100}),
                    (short) 100,
                    new int[]{0, 2, 4},
                    new IntBackedTable(TypeCodes.TYPE_SHORT, new int[]{100, 200, 300, 400, 500}),
                    (short) 200,
                    (short) 400,
                    new int[]{1, 2, 3},
                    new IntBackedTable(TypeCodes.TYPE_SHORT, new int[]{100, 200, 300, 400, 500}),
                    new short[]{200, 400},
                    new int[]{1, 3}
            );
        }

        static HandlerSpec floatSpec() {
            return new HandlerSpec(
                    "float",
                    FloatTypeHandler::new,
                    new Metadata(TypeCodes.TYPE_FLOAT, Float.class),
                    42.5d,
                    42.5f,
                    "HeapRuntimeKernel",
                    new IntBackedTable(TypeCodes.TYPE_FLOAT, encodedFloats(new float[]{1.5f, -2.5f, 1.5f, 0f, 1.5f})),
                    1.5f,
                    new int[]{0, 2, 4},
                    new IntBackedTable(TypeCodes.TYPE_FLOAT, encodedFloats(new float[]{-5.5f, -1.0f, 0.0f, 2.5f, 7.5f})),
                    -2.0f,
                    3.0f,
                    new int[]{1, 2, 3},
                    new IntBackedTable(TypeCodes.TYPE_FLOAT, encodedFloats(new float[]{1.1f, 2.2f, 3.3f, 4.4f, 5.5f})),
                    new float[]{2.2f, 4.4f},
                    new int[]{1, 3}
            );
        }

        static HandlerSpec doubleSpec() {
            return new HandlerSpec(
                    "double",
                    DoubleTypeHandler::new,
                    new Metadata(TypeCodes.TYPE_DOUBLE, Double.class),
                    42,
                    42.0d,
                    "executeBetweenRange",
                    new LongBackedTable(TypeCodes.TYPE_DOUBLE, encodedDoubles(new double[]{1.5d, -2.5d, 1.5d, 0d, 1.5d})),
                    1.5d,
                    new int[]{0, 2, 4},
                    new LongBackedTable(TypeCodes.TYPE_DOUBLE, encodedDoubles(new double[]{-5.5d, -1.0d, 0.0d, 2.5d, 7.5d})),
                    -2.0d,
                    3.0d,
                    new int[]{1, 2, 3},
                    new LongBackedTable(TypeCodes.TYPE_DOUBLE, encodedDoubles(new double[]{1.1d, 2.2d, 3.3d, 4.4d, 5.5d})),
                    new double[]{2.2d, 4.4d},
                    new int[]{1, 3}
            );
        }

        Metadata metadata() {
            var handler = factory.get();
            if (handler instanceof ByteTypeHandler byteHandler) {
                return new Metadata(byteHandler.getTypeCode(), byteHandler.getJavaType());
            }
            if (handler instanceof ShortTypeHandler shortHandler) {
                return new Metadata(shortHandler.getTypeCode(), shortHandler.getJavaType());
            }
            if (handler instanceof FloatTypeHandler floatHandler) {
                return new Metadata(floatHandler.getTypeCode(), floatHandler.getJavaType());
            }
            var doubleHandler = (DoubleTypeHandler) handler;
            return new Metadata(doubleHandler.getTypeCode(), doubleHandler.getJavaType());
        }

        Object convert(Object value) {
            var handler = factory.get();
            if (handler instanceof ByteTypeHandler byteHandler) {
                return byteHandler.convertValue(value);
            }
            if (handler instanceof ShortTypeHandler shortHandler) {
                return shortHandler.convertValue(value);
            }
            if (handler instanceof FloatTypeHandler floatHandler) {
                return floatHandler.convertValue(value);
            }
            return ((DoubleTypeHandler) handler).convertValue(value);
        }

        Selection executeEqualsSelection() {
            var handler = factory.get();
            if (handler instanceof ByteTypeHandler byteHandler) {
                return byteHandler.executeCondition(equalsTable, 0, LogicalQuery.Operator.EQ, (byte) equalsValue, false);
            }
            if (handler instanceof ShortTypeHandler shortHandler) {
                return shortHandler.executeCondition(equalsTable, 0, LogicalQuery.Operator.EQ, (short) equalsValue, false);
            }
            if (handler instanceof FloatTypeHandler floatHandler) {
                return floatHandler.executeCondition(equalsTable, 0, LogicalQuery.Operator.EQ, (float) equalsValue, false);
            }
            return ((DoubleTypeHandler) handler).executeCondition(equalsTable, 0, LogicalQuery.Operator.EQ, (double) equalsValue, false);
        }

        Selection executeBetweenRangeSelection() {
            var handler = factory.get();
            if (handler instanceof ByteTypeHandler byteHandler) {
                return byteHandler.executeBetweenRange(betweenTable, 0, (byte) betweenMin, (byte) betweenMax);
            }
            if (handler instanceof ShortTypeHandler shortHandler) {
                return shortHandler.executeBetweenRange(betweenTable, 0, (short) betweenMin, (short) betweenMax);
            }
            if (handler instanceof FloatTypeHandler floatHandler) {
                return floatHandler.executeBetweenRange(betweenTable, 0, (float) betweenMin, (float) betweenMax);
            }
            return ((DoubleTypeHandler) handler).executeBetweenRange(betweenTable, 0, (double) betweenMin, (double) betweenMax);
        }

        Selection executeInSelection() {
            var handler = factory.get();
            if (handler instanceof ByteTypeHandler byteHandler) {
                return byteHandler.executeIn(inTable, 0, (byte[]) inValues);
            }
            if (handler instanceof ShortTypeHandler shortHandler) {
                return shortHandler.executeIn(inTable, 0, (short[]) inValues);
            }
            if (handler instanceof FloatTypeHandler floatHandler) {
                return floatHandler.executeIn(inTable, 0, (float[]) inValues);
            }
            return ((DoubleTypeHandler) handler).executeIn(inTable, 0, (double[]) inValues);
        }

        void executeBetween() {
            var handler = factory.get();
            if (handler instanceof ByteTypeHandler byteHandler) {
                byteHandler.executeBetween(null, 0, (byte) 1);
                return;
            }
            if (handler instanceof ShortTypeHandler shortHandler) {
                shortHandler.executeBetween(null, 0, (short) 1);
                return;
            }
            if (handler instanceof FloatTypeHandler floatHandler) {
                floatHandler.executeCondition(new IntBackedTable(TypeCodes.TYPE_FLOAT, encodedFloats(new float[]{1.0f})),
                        0, LogicalQuery.Operator.BETWEEN, 1.0f, false);
                return;
            }
            ((DoubleTypeHandler) handler).executeBetween(null, 0, 1.0d);
        }

        private static int[] encodedFloats(float[] values) {
            var encoded = new int[values.length];
            for (var index = 0; index < values.length; index++) {
                encoded[index] = FloatEncoding.floatToSortableInt(values[index]);
            }
            return encoded;
        }

        private static long[] encodedDoubles(double[] values) {
            var encoded = new long[values.length];
            for (var index = 0; index < values.length; index++) {
                encoded[index] = FloatEncoding.doubleToSortableLong(values[index]);
            }
            return encoded;
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
