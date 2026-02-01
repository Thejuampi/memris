package io.memris.storage.heap;

import io.memris.core.MemrisConfiguration;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for TableGenerator strategy selection using MemrisConfiguration.
 */
class TableGeneratorStrategyTest {

    @Test
    void tableGeneratorShouldUseBytecodeImplementationByDefault() throws Exception {
        // Use default configuration (BYTECODE)
        MemrisConfiguration config = MemrisConfiguration.builder().build();
        
        TableMetadata metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false)
                )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        
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
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        
        long ref = table.insertFrom(new Object[]{1L, "Alice"});
        assertTrue(ref >= 0);
    }

    @Test
    void tableGeneratorShouldUseBytecodeImplementationWhenConfigured() throws Exception {
        MemrisConfiguration config = MemrisConfiguration.builder()
                .tableImplementation(MemrisConfiguration.TableImplementation.BYTECODE)
                .build();
        
        TableMetadata metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false)
                )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        
        // Verify no MethodHandle fields
        java.lang.reflect.Field[] fields = tableClass.getDeclaredFields();
        for (java.lang.reflect.Field field : fields) {
            if (java.lang.invoke.MethodHandle.class.isAssignableFrom(field.getType())) {
                fail("Bytecode table should not have MethodHandle fields: " + field.getName());
            }
        }
    }

    @Test
    void tableGeneratorShouldUseMethodHandleImplementationWhenConfigured() throws Exception {
        MemrisConfiguration config = MemrisConfiguration.builder()
                .tableImplementation(MemrisConfiguration.TableImplementation.METHOD_HANDLE)
                .build();
        
        TableMetadata metadata = new TableMetadata(
                "Person",
                "io.memris.test.Person",
                java.util.List.of(
                        new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false)
                )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata, config);
        
        // Verify the table works correctly with methodhandle implementation
        GeneratedTable table = (GeneratedTable) tableClass
                .getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
        
        long ref = table.insertFrom(new Object[]{1L, "Alice"});
        assertTrue(ref >= 0, "MethodHandle implementation should work correctly");
        
        // Verify we can read back
        String name = table.readString(1, 0);
        assertEquals("Alice", name);
    }
}
