package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for BytecodeTableGenerator - true bytecode table implementation without MethodHandle.
 *
 * These tests verify that generated table classes:
 * 1. Have direct field access (no MethodHandle.invoke)
 * 2. Support all scan operations (equals, between, in)
 * 3. Handle tombstone filtering inline
 * 4. Use O(1) array access for type codes
 */
class BytecodeTableGeneratorTest {

    @Test
    void generatedTableShouldSupportScanEqualsLong() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        long ref2 = table.insertFrom(new Object[]{2L, "Bob", 101});
        long ref3 = table.insertFrom(new Object[]{3L, "Charlie", 102});

        int[] matches = table.scanEqualsLong(0, 2L);

        assertEquals(1, matches.length);
        assertEquals(1, matches[0]); // Row index 1 (Bob)
    }

    @Test
    void generatedTableShouldSupportScanEqualsString() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});
        table.insertFrom(new Object[]{3L, "Charlie", 102});

        int[] matches = table.scanEqualsString(1, "Bob");

        assertEquals(1, matches.length);
        assertEquals("Bob", table.readString(1, matches[0]));
    }

    @Test
    void generatedTableShouldSupportScanBetweenLong() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 200});
        table.insertFrom(new Object[]{3L, "Charlie", 300});

        int[] matches = table.scanBetweenLong(2, 150L, 250L);

        assertEquals(1, matches.length);
        assertEquals(200, table.readInt(2, matches[0]));
    }

    @Test
    void generatedTableShouldSupportScanInLong() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 200});
        table.insertFrom(new Object[]{3L, "Charlie", 300});

        int[] matches = table.scanInLong(0, new long[]{1L, 3L});

        assertEquals(2, matches.length);
    }

    @Test
    void generatedTableShouldFilterTombstonedRows() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});
        table.insertFrom(new Object[]{3L, "Charlie", 102});

        // Tombstone the first row
        table.tombstone(ref1);

        int[] allMatches = table.scanEqualsLong(0, 1L);
        assertEquals(0, allMatches.length); // Should be filtered out

        int[] remaining = table.scanAll();
        assertEquals(2, remaining.length); // Only 2 live rows
    }

    @Test
    void generatedTableShouldHaveO1TypeCodeAccess() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Type code access should use direct array lookup
        assertEquals(io.memris.spring.TypeCodes.TYPE_LONG, table.typeCodeAt(0));
        assertEquals(io.memris.spring.TypeCodes.TYPE_STRING, table.typeCodeAt(1));
        assertEquals(io.memris.spring.TypeCodes.TYPE_INT, table.typeCodeAt(2));
    }

    @Test
    void generatedTableShouldSupportLookupById() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref = table.insertFrom(new Object[]{42L, "Test", 999});

        long found = table.lookupById(42L);

        assertEquals(ref, found);
    }

    @Test
    void generatedTableShouldNotUseReflection() throws Exception {
        // This test verifies the generated class doesn't have MethodHandle fields
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);

        // Check for no MethodHandle fields
        java.lang.reflect.Field[] fields = tableClass.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                fail("Generated table should not have MethodHandle fields: " + field.getName());
            }
        }
    }

    private TableMetadata createPersonMetadata() {
        return new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.spring.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.spring.TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("age", io.memris.spring.TypeCodes.TYPE_INT, false, false)
                )
        );
    }
}
