package io.memris.storage.ffm;

import io.memris.kernel.Predicate;
import io.memris.kernel.selection.SelectionVector;
import io.memris.kernel.selection.SelectionVectorFactory;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FfmTableScanInTest {

    @Test
    void scanIn_should_return_rows_with_values_in_collection() {
        try (Arena arena = Arena.ofConfined()) {
            FfmTable table = new FfmTable("test", arena, List.of(
                    new FfmTable.ColumnSpec("id", int.class),
                    new FfmTable.ColumnSpec("status", int.class)
            ), 100);

            for (int i = 0; i < 100; i++) {
                table.insert(i, i % 10);
            }

            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = table.scan(
                    new Predicate.In("status", List.of(0, 5, 9)),
                    factory);

            assertThat(result.size()).isEqualTo(30);
        }
    }

    @Test
    void scanIn_with_empty_collection_should_return_empty() {
        try (Arena arena = Arena.ofConfined()) {
            FfmTable table = new FfmTable("test", arena, List.of(
                    new FfmTable.ColumnSpec("id", int.class),
                    new FfmTable.ColumnSpec("status", int.class)
            ), 100);

            for (int i = 0; i < 100; i++) {
                table.insert(i, i % 10);
            }

            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = table.scan(
                    new Predicate.In("status", List.of()),
                    factory);

            assertThat(result.size()).isZero();
        }
    }

    @Test
    void scanIn_with_single_value_should_match_equal_rows() {
        try (Arena arena = Arena.ofConfined()) {
            FfmTable table = new FfmTable("test", arena, List.of(
                    new FfmTable.ColumnSpec("id", int.class),
                    new FfmTable.ColumnSpec("status", int.class)
            ), 100);

            for (int i = 0; i < 100; i++) {
                table.insert(i, i % 10);
            }

            SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
            SelectionVector result = table.scan(
                    new Predicate.In("status", List.of(7)),
                    factory);

            assertThat(result.size()).isEqualTo(10);
        }
    }
}
