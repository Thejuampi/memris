package io.memris.runtime.dispatch;

import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class InIndexSelectorTest {

    @Test
    @DisplayName("should union rows for iterable values")
    void shouldUnionRowsForIterableValues() {
        var selection = InIndexSelector.select(
                List.of("a", "b"),
                value -> switch ((String) value) {
                    case "a" -> new int[] { 10 };
                    case "b" -> new int[] { 20 };
                    default -> null;
                },
                InIndexSelectorTest::selectionFromRows);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(10, 20);
    }

    @Test
    @DisplayName("should union rows for long array values")
    void shouldUnionRowsForLongArrayValues() {
        var selection = InIndexSelector.select(
                new long[] { 2L, 4L },
                value -> ((Number) value).longValue() == 2L ? new int[] { 2 } : new int[] { 4 },
                InIndexSelectorTest::selectionFromRows);

        assertThat(selection.toIntArray()).containsExactlyInAnyOrder(2, 4);
    }

    @Test
    @DisplayName("should return null when lookup misses")
    void shouldReturnNullWhenLookupMisses() {
        var selection = InIndexSelector.select(
                new int[] { 1, 3 },
                value -> ((Number) value).intValue() == 1 ? new int[] { 1 } : null,
                InIndexSelectorTest::selectionFromRows);

        assertThat(selection).isNull();
    }

    private static Selection selectionFromRows(int[] rows) {
        var refs = new long[rows.length];
        for (var i = 0; i < rows.length; i++) {
            refs[i] = Selection.pack(rows[i], 1);
        }
        return new SelectionImpl(refs);
    }
}
