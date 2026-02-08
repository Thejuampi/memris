package io.memris.storage.heap;

import io.memris.kernel.RowId;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD tests for TableGenerator.
 * Tests that ByteBuddy generates correct table classes from entity metadata.
 */
class TableGeneratorTest {

    @Test
    void generatePersonTableCreatesValidClass() {
        TableMetadata metadata = createPersonMetadata();

        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        assertThat(new ClassSummary(
                tableClass != null,
                tableClass != null ? tableClass.getSimpleName() : null,
                tableClass != null && AbstractTable.class.isAssignableFrom(tableClass),
                tableClass != null && GeneratedTable.class.isAssignableFrom(tableClass)))
                .usingRecursiveComparison()
                .isEqualTo(new ClassSummary(true, "PersonTable", true, true));
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
        assertThat(new TableSummary(abstractTable != null, abstractTable.name(), genTable.columnCount()))
                .usingRecursiveComparison()
                .isEqualTo(new TableSummary(true, "Person", 3));
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

        assertThat(new InsertSummary(ref >= 0, table.liveCount())).usingRecursiveComparison()
                .isEqualTo(new InsertSummary(true, 1L));
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
        assertThat(foundRef).isEqualTo(ref);
    }

    @Test
    void generateAddressTableCreatesValidClass() {
        TableMetadata metadata = createAddressMetadata();

        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);

        assertThat(new ClassSummary(
                tableClass != null,
                tableClass != null ? tableClass.getSimpleName() : null,
                tableClass != null && AbstractTable.class.isAssignableFrom(tableClass),
                tableClass != null && GeneratedTable.class.isAssignableFrom(tableClass)))
                .usingRecursiveComparison()
                .isEqualTo(new ClassSummary(true, "AddressTable", true, true));
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

        long foundRef = table.lookupByIdString("12345");
        assertThat(new LookupSummary(ref >= 0, foundRef)).usingRecursiveComparison()
                .isEqualTo(new LookupSummary(true, ref));
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

        assertThat(results).hasSize(2);
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

    private record ClassSummary(boolean exists, String simpleName, boolean abstractTableAssignable,
                                boolean generatedTableAssignable) {
    }

    private record TableSummary(boolean exists, String name, int columnCount) {
    }

    private record InsertSummary(boolean nonNegativeRef, long liveCount) {
    }

    private record LookupSummary(boolean nonNegativeRef, long foundRef) {
    }
}
