package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

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
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        long ref2 = table.insertFrom(new Object[]{2L, "Bob", 101});
        long ref3 = table.insertFrom(new Object[]{3L, "Charlie", 102});

        int[] matches = table.scanEqualsLong(0, 2L);

        assertThat(matches).containsExactly(1);
    }

    @Test
    void generatedTableShouldSupportScanEqualsString() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});
        table.insertFrom(new Object[]{3L, "Charlie", 102});

        int[] matches = table.scanEqualsString(1, "Bob");

        assertThat(new StringMatchSnapshot(matches.length, table.readString(1, matches[0])))
                .usingRecursiveComparison()
                .isEqualTo(new StringMatchSnapshot(1, "Bob"));
    }

    @Test
    void generatedTableShouldSupportScanBetweenLong() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 200});
        table.insertFrom(new Object[]{3L, "Charlie", 300});

        int[] matches = table.scanBetweenLong(2, 150L, 250L);

        assertThat(new IntMatchSnapshot(matches.length, table.readInt(2, matches[0])))
                .usingRecursiveComparison()
                .isEqualTo(new IntMatchSnapshot(1, 200));
    }

    @Test
    void generatedTableShouldSupportScanInLong() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 200});
        table.insertFrom(new Object[]{3L, "Charlie", 300});

        int[] matches = table.scanInLong(0, new long[]{1L, 3L});

        assertThat(matches).hasSize(2);
    }

    @Test
    void generatedTableShouldFilterTombstonedRows() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref1 = table.insertFrom(new Object[]{1L, "Alice", 100});
        table.insertFrom(new Object[]{2L, "Bob", 101});
        table.insertFrom(new Object[]{3L, "Charlie", 102});

        // Tombstone the first row
        table.tombstone(ref1);

        int[] allMatches = table.scanEqualsLong(0, 1L);
        assertThat(allMatches).isEmpty();

        int[] remaining = table.scanAll();
        assertThat(remaining).hasSize(2);
    }

    @Test
    void generatedTableShouldHaveO1TypeCodeAccess() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        // Type code access should use direct array lookup
        assertThat(new byte[]{table.typeCodeAt(0), table.typeCodeAt(1), table.typeCodeAt(2)})
                .containsExactly(io.memris.core.TypeCodes.TYPE_LONG, io.memris.core.TypeCodes.TYPE_STRING,
                        io.memris.core.TypeCodes.TYPE_INT);
    }

    @Test
    void generatedTableShouldSupportLookupById() throws Exception {
        TableMetadata metadata = createPersonMetadata();
        Class<? extends AbstractTable> tableClass = BytecodeTableGenerator.generate(metadata);
        AbstractTable abstractTable = tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        GeneratedTable table = (GeneratedTable) abstractTable;

        long ref = table.insertFrom(new Object[]{42L, "Test", 999});

        long found = table.lookupById(42L);

        assertThat(found).isEqualTo(ref);
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
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("age", io.memris.core.TypeCodes.TYPE_INT, false, false)
                )
        );
    }

    private record StringMatchSnapshot(int size, String value) {
    }

    private record IntMatchSnapshot(int size, int value) {
    }
}
