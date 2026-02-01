package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TableGeneratorTombstoneTest {

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
    void tableGeneratorShouldDecrementLiveCountOnTombstone() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(1024, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});
        table.insertFrom(new Object[]{3L, "Charlie", 102});

        assertThat(table.liveCount()).isEqualTo(3);

        table.tombstone(ref1);

        assertThat(table.liveCount()).isEqualTo(2);
        assertThat(table.scanAll()).hasSize(2);
        assertThat(table.lookupById(1L)).isEqualTo(-1L);
    }

    @Test
    void directAbstractTableTombstoneShouldDecrementLiveCount() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(1024, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});

        int rowIndex = io.memris.storage.Selection.index(ref1);
        long generation = io.memris.storage.Selection.generation(ref1);
        int pageId = rowIndex / 1024;
        int offset = rowIndex % 1024;
        io.memris.kernel.RowId rowId = new io.memris.kernel.RowId(pageId, offset);

        boolean tombstoned = abstractTable.tombstone(rowId, generation);

        assertThat(tombstoned).isTrue();
        assertThat(table.liveCount()).isEqualTo(1);
    }
}
