package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class BytecodeImplementationTest {

    @BeforeEach
    void enableBytecodeImpl() {
        System.setProperty("memris.table.impl", "bytecode");
    }

    @AfterEach
    void clearBytecodeImpl() {
        System.clearProperty("memris.table.impl");
    }

    @Test
    void bytecodeTableSupportsInsertScanAndLookup() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        long ref2 = table.insertFrom(new Object[]{2L, "Bob", 101});

        assertThat(new InsertSnapshot(ref1 >= 0, ref2 >= 0, table.liveCount())).usingRecursiveComparison()
                .isEqualTo(new InsertSnapshot(true, true, 2L));

        int[] matches = table.scanEqualsString(1, "Alice");
        assertThat(matches).hasSize(1);

        long found = table.lookupById(1L);
        assertThat(found).isEqualTo(ref1);

        assertThat(table.readString(1, matches[0])).isNotNull();
    }

    private TableMetadata createPersonMetadata() {
        return new TableMetadata(
                "Person",
                "io.memris.test.Person",
                List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("addressId", io.memris.core.TypeCodes.TYPE_INT, false, false)
                )
        );
    }

    private record InsertSnapshot(boolean firstInserted, boolean secondInserted, long liveCount) {
    }
}
