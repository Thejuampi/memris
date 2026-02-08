package io.memris.storage.heap;

interface ColumnSpec<T> {
    String name();

    void set(Object column, int index, T value);

    void setNull(Object column, int index);

    Object createColumn(int pageSize);

    int[] scanEquals(Object column, T value, int count);

    int[] scanGreaterThan(Object column, T value, int count);

    int[] scanLessThan(Object column, T value, int count);

    int[] scanBetween(Object column, T min, T max, int count);

    int[] scanIn(Object column, T[] values, int count);

    T[] sampleValues();

    T[] inTargets();

    T[] noMatchTargets();

    T[] emptyTargets();

    T[] extremeTargets();

    default int[] scanEqualsIgnoreCase(Object column, T value, int count) {
        throw new UnsupportedOperationException("scanEqualsIgnoreCase not supported for " + name());
    }
}
