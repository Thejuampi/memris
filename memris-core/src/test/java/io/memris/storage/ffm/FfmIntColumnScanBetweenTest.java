package io.memris.storage.ffm;

import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.assertj.core.api.Assertions.assertThat;

class FfmIntColumnScanBetweenTest {

    @Test
    void scanBetween_should_return_rows_in_range() {
        try (Arena arena = Arena.ofConfined()) {
            FfmIntColumn column = new FfmIntColumn("test", arena, 100);
            for (int i = 0; i < 100; i++) {
                column.set(i, i);
            }
            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = column.scanBetween(25, 75, 100, factory);
            assertThat(result.size()).isEqualTo(51);
            int expected = 25;
            for (int idx : result.toIntArray()) {
                assertThat(idx).isEqualTo(expected++);
            }
        }
    }

    @Test
    void scanBetween_with_no_matches_should_return_empty() {
        try (Arena arena = Arena.ofConfined()) {
            FfmIntColumn column = new FfmIntColumn("test", arena, 100);
            for (int i = 0; i < 100; i++) {
                column.set(i, i);
            }
            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = column.scanBetween(200, 300, 100, factory);
            assertThat(result.size()).isZero();
        }
    }

    @Test
    void scanBetween_with_all_matching_should_return_all_rows() {
        try (Arena arena = Arena.ofConfined()) {
            FfmIntColumn column = new FfmIntColumn("test", arena, 100);
            for (int i = 0; i < 100; i++) {
                column.set(i, 50);
            }
            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = column.scanBetween(0, 100, 100, factory);
            assertThat(result.size()).isEqualTo(100);
        }
    }

    @Test
    void scanBetween_with_partial_row_count_should_only_scan_up_to_rowCount() {
        try (Arena arena = Arena.ofConfined()) {
            FfmIntColumn column = new FfmIntColumn("test", arena, 1000);
            for (int i = 0; i < 1000; i++) {
                column.set(i, i);
            }
            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = column.scanBetween(0, 99, 50, factory);
            assertThat(result.size()).isEqualTo(50);
        }
    }
}
