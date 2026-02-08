package io.memris.storage.heap;

import java.util.stream.Stream;

final class ColumnSpecs {

    static final ColumnSpec<Integer> INT = new ColumnSpec<>() {
        @Override
        public String name() {
            return "int";
        }

        @Override
        public void set(Object column, int index, Integer value) {
            ((PageColumnInt) column).set(index, value);
        }

        @Override
        public void setNull(Object column, int index) {
            ((PageColumnInt) column).setNull(index);
        }

        @Override
        public Object createColumn(int pageSize) {
            return new PageColumnInt(pageSize);
        }

        @Override
        public int[] scanEquals(Object column, Integer value, int count) {
            return ((PageColumnInt) column).scanEquals(value, count);
        }

        @Override
        public int[] scanGreaterThan(Object column, Integer value, int count) {
            return ((PageColumnInt) column).scanGreaterThan(value, count);
        }

        @Override
        public int[] scanLessThan(Object column, Integer value, int count) {
            return ((PageColumnInt) column).scanLessThan(value, count);
        }

        @Override
        public int[] scanBetween(Object column, Integer min, Integer max, int count) {
            return ((PageColumnInt) column).scanBetween(min, max, count);
        }

        @Override
        public int[] scanIn(Object column, Integer[] values, int count) {
            if (values == null || values.length == 0) {
                return ((PageColumnInt) column).scanIn(new int[0], count);
            }
            var primitiveValues = new int[values.length];
            for (var index = 0; index < values.length; index++) {
                primitiveValues[index] = values[index];
            }
            return ((PageColumnInt) column).scanIn(primitiveValues, count);
        }

        @Override
        public Integer[] sampleValues() {
            return new Integer[]{10, 20, 30, 40, 50};
        }

        @Override
        public Integer[] inTargets() {
            return new Integer[]{20, 40};
        }

        @Override
        public Integer[] noMatchTargets() {
            return new Integer[]{99, 999};
        }

        @Override
        public Integer[] emptyTargets() {
            return new Integer[0];
        }

        @Override
        public Integer[] extremeTargets() {
            return new Integer[]{Integer.MAX_VALUE, Integer.MIN_VALUE};
        }
    };

    static final ColumnSpec<Long> LONG = new ColumnSpec<>() {
        @Override
        public String name() {
            return "long";
        }

        @Override
        public void set(Object column, int index, Long value) {
            ((PageColumnLong) column).set(index, value);
        }

        @Override
        public void setNull(Object column, int index) {
            ((PageColumnLong) column).setNull(index);
        }

        @Override
        public Object createColumn(int pageSize) {
            return new PageColumnLong(pageSize);
        }

        @Override
        public int[] scanEquals(Object column, Long value, int count) {
            return ((PageColumnLong) column).scanEquals(value, count);
        }

        @Override
        public int[] scanGreaterThan(Object column, Long value, int count) {
            return ((PageColumnLong) column).scanGreaterThan(value, count);
        }

        @Override
        public int[] scanLessThan(Object column, Long value, int count) {
            return ((PageColumnLong) column).scanLessThan(value, count);
        }

        @Override
        public int[] scanBetween(Object column, Long min, Long max, int count) {
            return ((PageColumnLong) column).scanBetween(min, max, count);
        }

        @Override
        public int[] scanIn(Object column, Long[] values, int count) {
            if (values == null || values.length == 0) {
                return ((PageColumnLong) column).scanIn(new long[0], count);
            }
            var primitiveValues = new long[values.length];
            for (var index = 0; index < values.length; index++) {
                primitiveValues[index] = values[index];
            }
            return ((PageColumnLong) column).scanIn(primitiveValues, count);
        }

        @Override
        public Long[] sampleValues() {
            return new Long[]{10L, 20L, 30L, 40L, 50L};
        }

        @Override
        public Long[] inTargets() {
            return new Long[]{20L, 40L};
        }

        @Override
        public Long[] noMatchTargets() {
            return new Long[]{99L, 999L};
        }

        @Override
        public Long[] emptyTargets() {
            return new Long[0];
        }

        @Override
        public Long[] extremeTargets() {
            return new Long[]{Long.MAX_VALUE, Long.MIN_VALUE};
        }
    };

    static final ColumnSpec<String> STRING = new ColumnSpec<>() {
        @Override
        public String name() {
            return "string";
        }

        @Override
        public void set(Object column, int index, String value) {
            ((PageColumnString) column).set(index, value);
        }

        @Override
        public void setNull(Object column, int index) {
            ((PageColumnString) column).setNull(index);
        }

        @Override
        public Object createColumn(int pageSize) {
            return new PageColumnString(pageSize);
        }

        @Override
        public int[] scanEquals(Object column, String value, int count) {
            return ((PageColumnString) column).scanEquals(value, count);
        }

        @Override
        public int[] scanGreaterThan(Object column, String value, int count) {
            throw new UnsupportedOperationException("greaterThan not supported for string");
        }

        @Override
        public int[] scanLessThan(Object column, String value, int count) {
            throw new UnsupportedOperationException("lessThan not supported for string");
        }

        @Override
        public int[] scanBetween(Object column, String min, String max, int count) {
            throw new UnsupportedOperationException("between not supported for string");
        }

        @Override
        public int[] scanIn(Object column, String[] values, int count) {
            return ((PageColumnString) column).scanIn(values, count);
        }

        @Override
        public String[] sampleValues() {
            return new String[]{"red", "green", "blue", "yellow", "purple"};
        }

        @Override
        public String[] inTargets() {
            return new String[]{"green", "yellow"};
        }

        @Override
        public String[] noMatchTargets() {
            return new String[]{"grape", "pear"};
        }

        @Override
        public String[] emptyTargets() {
            return new String[0];
        }

        @Override
        public String[] extremeTargets() {
            return new String[]{""};
        }

        @Override
        public int[] scanEqualsIgnoreCase(Object column, String value, int count) {
            return ((PageColumnString) column).scanEqualsIgnoreCase(value, count);
        }
    };

    private ColumnSpecs() {
    }

    static Stream<ColumnSpec<?>> all() {
        return Stream.of(INT, LONG, STRING);
    }

    static Stream<ColumnSpec<?>> numeric() {
        return Stream.of(INT, LONG);
    }
}
