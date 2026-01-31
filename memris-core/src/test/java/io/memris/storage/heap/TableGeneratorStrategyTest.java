package io.memris.storage.heap;

import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for TableGenerator strategy selection.
 */
class TableGeneratorStrategyTest {

    private String originalImpl;

    @BeforeEach
    void setUp() {
        originalImpl = System.getProperty("memris.table.impl");
    }

    @AfterEach
    void tearDown() {
        if (originalImpl == null) {
            System.clearProperty("memris.table.impl");
        } else {
            System.setProperty("memris.table.impl", originalImpl);
        }
    }

    @Test
    void tableGeneratorShouldUseBytecodeImplementationByDefault() throws Exception {
        // Clear any explicit setting
        System.clearProperty("memris.table.impl");
        
        TableMetadata metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.spring.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.spring.TypeCodes.TYPE_STRING, false, false)
                )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        
        // Verify it's the bytecode implementation (not MethodHandleImplementation)
        assertFalse(tableClass.getName().contains("MethodHandle"), 
                "Default should be bytecode implementation, not MethodHandle");
        
        // Verify generated class has no MethodHandle fields
        java.lang.reflect.Field[] fields = tableClass.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                fail("Bytecode table should not have MethodHandle fields: " + field.getName());
            }
        }
        
        // Verify it works
        GeneratedTable table = (GeneratedTable) tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        
        long ref = table.insertFrom(new Object[]{1L, "Alice"});
        assertTrue(ref >= 0);
    }

    @Test
    void tableGeneratorShouldRespectBytecodeProperty() throws Exception {
        System.setProperty("memris.table.impl", "bytecode");
        
        TableMetadata metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.spring.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.spring.TypeCodes.TYPE_STRING, false, false)
                )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        
        // Verify no MethodHandle fields
        java.lang.reflect.Field[] fields = tableClass.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                fail("Bytecode table should not have MethodHandle fields: " + field.getName());
            }
        }
    }

    @Test
    void tableGeneratorShouldFallbackToMethodHandleWhenRequested() throws Exception {
        System.setProperty("memris.table.impl", "methodhandle");
        
        TableMetadata metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.spring.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.spring.TypeCodes.TYPE_STRING, false, false)
                )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        
        // Verify the table works correctly with methodhandle implementation
        GeneratedTable table = (GeneratedTable) tableClass
                .getConstructor(int.class, int.class)
                .newInstance(32, 4);
        
        long ref = table.insertFrom(new Object[]{1L, "Alice"});
        assertTrue(ref >= 0, "MethodHandle implementation should work correctly");
        
        // Verify we can read back
        String name = table.readString(1, 0);
        assertEquals("Alice", name);
    }
}
