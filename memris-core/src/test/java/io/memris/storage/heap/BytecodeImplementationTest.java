package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

        assertTrue(ref1 >= 0);
        assertTrue(ref2 >= 0);
        assertEquals(2L, table.liveCount());

        int[] matches = table.scanEqualsString(1, "Alice");
        assertEquals(1, matches.length);

        long found = table.lookupById(1L);
        assertEquals(ref1, found);

        assertNotNull(table.readString(1, matches[0]));
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
}
