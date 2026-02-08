package io.memris.storage.heap;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class PageColumnScanTest {

    private static final int PAGE_SIZE = 64;

    @ParameterizedTest(name = "scanEquals matches values for {0}")
    @MethodSource("numericSpecs")
    void scanEqualsFindsMatchingValues(ColumnSpec<?> spec) {
        assertThat(scanEqualsMatches(spec)).containsExactly(0, 2);
    }

    @ParameterizedTest(name = "scanGreaterThan works for {0}")
    @MethodSource("numericSpecs")
    void scanGreaterThan(ColumnSpec<?> spec) {
        assertThat(scanGreaterThanMatches(spec)).containsExactly(2, 3);
    }

    @ParameterizedTest(name = "scanLessThan works for {0}")
    @MethodSource("numericSpecs")
    void scanLessThan(ColumnSpec<?> spec) {
        assertThat(scanLessThanMatches(spec)).containsExactly(0, 1);
    }

    @ParameterizedTest(name = "scanBetween works for {0}")
    @MethodSource("numericSpecs")
    void scanBetween(ColumnSpec<?> spec) {
        assertThat(scanBetweenMatches(spec)).containsExactly(1, 2, 3);
    }

    @ParameterizedTest(name = "scanIn works for {0}")
    @MethodSource("allSpecs")
    void scanIn(ColumnSpec<?> spec) {
        assertThat(scanInMatches(spec)).containsExactly(1, 3);
    }

    @ParameterizedTest(name = "scanIn empty targets returns empty for {0}")
    @MethodSource("allSpecs")
    void scanInWithEmptyTargets(ColumnSpec<?> spec) {
        assertThat(scanInEmptyTargets(spec)).isEmpty();
    }

    @ParameterizedTest(name = "scanIn null targets returns empty for {0}")
    @MethodSource("allSpecs")
    void scanInWithNullTargets(ColumnSpec<?> spec) {
        assertThat(scanInNullTargets(spec)).isEmpty();
    }

    @ParameterizedTest(name = "scan respects published count for {0}")
    @MethodSource("allSpecs")
    void scanRespectsPublishedCount(ColumnSpec<?> spec) {
        assertThat(scanRespectsPublished(spec)).containsExactly(0, 1);
    }

    @ParameterizedTest(name = "scan no matches returns empty for {0}")
    @MethodSource("allSpecs")
    void scanNoMatchesReturnsEmpty(ColumnSpec<?> spec) {
        assertThat(scanNoMatches(spec)).isEmpty();
    }

    @ParameterizedTest(name = "scan handles null/present markers for {0}")
    @MethodSource("allSpecs")
    void scanSkipsNullValues(ColumnSpec<?> spec) {
        assertThat(scanSkipsNullValuesMatches(spec)).containsExactly(0, 2);
    }

    @ParameterizedTest(name = "string case-insensitive equals works")
    @MethodSource("stringSpecs")
    void scanEqualsIgnoreCase(ColumnSpec<?> spec) {
        assertThat(scanIgnoreCaseMatches(spec)).containsExactly(0, 1, 3);
    }

    @ParameterizedTest(name = "string null equals works")
    @MethodSource("stringSpecs")
    void scanEqualsNullTarget(ColumnSpec<?> spec) {
        assertThat(scanNullTargetMatches(spec)).containsExactly(1, 3);
    }

    @ParameterizedTest(name = "extreme boundaries work for {0}")
    @MethodSource("numericSpecs")
    void extremeBoundaries(ColumnSpec<?> spec) {
        assertThat(scanExtremeBounds(spec)).containsExactly(0, 2);
    }

    private static Stream<ColumnSpec<?>> allSpecs() {
        return ColumnSpecs.all();
    }

    private static Stream<ColumnSpec<?>> numericSpecs() {
        return ColumnSpecs.numeric();
    }

    private static Stream<ColumnSpec<?>> stringSpecs() {
        return Stream.of(ColumnSpecs.STRING);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanEqualsMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        spec.set(column, 2, values[0]);
        spec.set(column, 3, values[3]);
        publish(column, 4);
        return spec.scanEquals(column, values[0], 4);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanGreaterThanMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        spec.set(column, 2, values[2]);
        spec.set(column, 3, values[3]);
        publish(column, 4);
        return spec.scanGreaterThan(column, values[1], 4);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanLessThanMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        spec.set(column, 2, values[2]);
        spec.set(column, 3, values[3]);
        publish(column, 4);
        return spec.scanLessThan(column, values[2], 4);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanBetweenMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        spec.set(column, 2, values[2]);
        spec.set(column, 3, values[3]);
        spec.set(column, 4, values[4]);
        publish(column, 5);
        return spec.scanBetween(column, values[1], values[3], 5);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanInMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        spec.set(column, 2, values[2]);
        spec.set(column, 3, values[3]);
        publish(column, 4);
        return spec.scanIn(column, spec.inTargets(), 4);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanInEmptyTargets(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        publish(column, 2);
        return spec.scanIn(column, spec.emptyTargets(), 2);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanInNullTargets(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        publish(column, 2);
        return spec.scanIn(column, (T[]) null, 2);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanRespectsPublished(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[0]);
        spec.set(column, 2, values[0]);
        publish(column, 2);
        return spec.scanEquals(column, values[0], 4);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanNoMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.set(column, 1, values[1]);
        publish(column, 2);
        return spec.scanIn(column, spec.noMatchTargets(), 2);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanSkipsNullValuesMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        var values = spec.sampleValues();
        spec.set(column, 0, values[0]);
        spec.setNull(column, 1);
        spec.set(column, 2, values[0]);
        publish(column, 4);
        return spec.scanEquals(column, values[0], 4);
    }

    private static int[] scanIgnoreCaseMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<String>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        spec.set(column, 0, "Apple");
        spec.set(column, 1, "APPLE");
        spec.set(column, 2, "banana");
        spec.set(column, 3, "aPPle");
        publish(column, 4);
        return spec.scanEqualsIgnoreCase(column, "apple", 4);
    }

    private static int[] scanNullTargetMatches(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<String>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        spec.set(column, 0, "apple");
        spec.setNull(column, 1);
        spec.set(column, 2, "banana");
        spec.setNull(column, 3);
        publish(column, 4);
        return spec.scanEquals(column, null, 4);
    }

    @SuppressWarnings("unchecked")
    private static <T> int[] scanExtremeBounds(ColumnSpec<?> rawSpec) {
        var spec = (ColumnSpec<T>) rawSpec;
        var column = spec.createColumn(PAGE_SIZE);
        if (spec == ColumnSpecs.INT) {
            spec.set(column, 0, (T) Integer.valueOf(Integer.MAX_VALUE));
            spec.set(column, 1, (T) Integer.valueOf(0));
            spec.set(column, 2, (T) Integer.valueOf(Integer.MIN_VALUE));
            publish(column, 3);
            return spec.scanIn(column, spec.extremeTargets(), 3);
        }
        spec.set(column, 0, (T) Long.valueOf(Long.MAX_VALUE));
        spec.set(column, 1, (T) Long.valueOf(0L));
        spec.set(column, 2, (T) Long.valueOf(Long.MIN_VALUE));
        publish(column, 3);
        return spec.scanIn(column, spec.extremeTargets(), 3);
    }

    private static void publish(Object column, int count) {
        if (column instanceof PageColumnInt intColumn) {
            intColumn.publish(count);
        } else if (column instanceof PageColumnLong longColumn) {
            longColumn.publish(count);
        } else {
            ((PageColumnString) column).publish(count);
        }
    }
}
