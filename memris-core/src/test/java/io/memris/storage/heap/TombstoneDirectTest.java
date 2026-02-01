package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Direct test of tombstone functionality
 */
class TombstoneDirectTest {

    private TableMetadata createPersonMetadata() {
        return new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("age", io.memris.core.TypeCodes.TYPE_INT, false, false)
                )
        );
    }

    @Test
    void tombstoneShouldDecrementLiveCount() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Insert 3 rows
        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        long ref2 = table.insertFrom(new Object[]{2L, "Bob", 101});
        long ref3 = table.insertFrom(new Object[]{3L, "Charlie", 102});

        assertThat(table.liveCount()).isEqualTo(3);

        // Tombstone first row
        table.tombstone(ref1);

        assertThat(table.liveCount()).isEqualTo(2);
        assertThat(table.allocatedCount()).isEqualTo(3); // Still 3 allocated

        // Verify lookup returns -1 for tombstoned row
        assertThat(table.lookupById(1L)).isEqualTo(-1L);
        assertThat(table.lookupById(2L)).isGreaterThanOrEqualTo(0);

        // Verify scanAll returns only 2 rows
        int[] allRows = table.scanAll();
        assertThat(allRows).hasSize(2);
    }
}
