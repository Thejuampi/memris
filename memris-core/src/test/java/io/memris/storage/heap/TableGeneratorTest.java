package io.memris.storage.heap;

import io.memris.kernel.RowId;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD tests for TableGenerator.
 * Tests that ByteBuddy generates correct table classes from entity metadata.
 */
class TableGeneratorTest {

    @Test
    void generatePersonTableCreatesValidClass() {
        TableMetadata metadata = createPersonMetadata();

        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        assertNotNull(tableClass);
        assertEquals("PersonTable", tableClass.getSimpleName());
        assertTrue(AbstractTable.class.isAssignableFrom(tableClass));
        assertTrue(GeneratedTable.class.isAssignableFrom(tableClass));
    }

    @Test
    void generatedPersonTableHasCorrectColumns() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable genTable = (GeneratedTable) abstractTable;

        // Verify table exists and has correct name
        assertNotNull(abstractTable);
        assertEquals("Person", abstractTable.name());
        
        // Verify GeneratedTable interface methods work
        assertEquals(3, genTable.columnCount()); // id, name, addressId
    }

    @Test
    void generatedPersonTableSupportsInsert() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Insert using GeneratedTable.insertFrom(Object[])
        long ref = table.insertFrom(new Object[]{1L, "Alice", 100});

        assertNotNull(ref);
        assertTrue(ref >= 0);
        assertEquals(1L, table.liveCount());
    }

    @Test
    void generatedPersonTableSupportsFindById() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Insert
        long ref = table.insertFrom(new Object[]{1L, "Alice", 100});
        
        // Find by ID using lookupById
        long foundRef = table.lookupById(1L);
        assertEquals(ref, foundRef);
    }

    @Test
    void generateAddressTableCreatesValidClass() {
        TableMetadata metadata = createAddressMetadata();

        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        assertNotNull(tableClass);
        assertEquals("AddressTable", tableClass.getSimpleName());
    }

    @Test
    void generatedTableHandlesStringId() throws Exception {
        TableMetadata metadata = createAddressMetadata(); // Address has String ID (zip)
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Insert with String ID using insertFrom
        long ref = table.insertFrom(new Object[]{"12345", "Main St", 100});

        assertNotNull(ref);
        assertTrue(ref >= 0);

        // Find by String ID using lookupByIdString
        long foundRef = table.lookupByIdString("12345");
        assertEquals(ref, foundRef);
    }

    @Test
    void generatedTableHasScanMethods() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Insert multiple rows
        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});
        table.insertFrom(new Object[]{3L, "Alice", 102});

        // Scan for rows with name = "Alice" (column index 1 is name)
        int[] results = table.scanEqualsString(1, "Alice");

        assertEquals(2, results.length);
    }

    // Helper methods to create EntityMetadata

    private TableMetadata createPersonMetadata() {
        // Person entity:
        // - id: Long (PK)
        // - name: String
        // - addressId: int (FK)
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

    private TableMetadata createAddressMetadata() {
        // Address entity:
        // - zip: String (PK)
        // - street: String
        // - cityId: int (FK)
        return new TableMetadata(
                "Address",
                "io.memris.test.Address",
                List.of(
                        new FieldMetadata("zip", io.memris.core.TypeCodes.TYPE_STRING, true, true),
                        new FieldMetadata("street", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("cityId", io.memris.core.TypeCodes.TYPE_INT, false, false)
                )
        );
    }

    // Helper classes for metadata are now in TableMetadata and FieldMetadata
}
