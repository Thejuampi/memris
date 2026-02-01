package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.MetadataExtractor;
import io.memris.storage.GeneratedTable;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for EntityMaterializerImpl.
 */
class EntityMaterializerImplTest {

    @Test
    @DisplayName("should materialize entity from table row")
    void materializeEntityFromRow() throws Exception {
        // Create table with test data
        TableMetadata tableMetadata = new TableMetadata(
            "TestEntity",
            TestEntity.class.getName(),
            List.of(
                new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                new FieldMetadata("age", io.memris.core.TypeCodes.TYPE_INT, false, false)
            )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(tableMetadata);
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class).newInstance(32, 4);
        
        // Insert test data
        table.insertFrom(new Object[]{1L, "Alice", 30});
        
        // Create metadata and materializer
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(metadata);
        
        // Create kernel with table
        HeapRuntimeKernel kernel = new HeapRuntimeKernel(table);
        
        // Materialize entity
        TestEntity entity = materializer.materialize(kernel, 0);
        
        assertNotNull(entity);
        assertEquals(1L, entity.id);
        assertEquals("Alice", entity.name);
        assertEquals(30, entity.age);
    }
    
    @Test
    @DisplayName("should handle null values correctly")
    void handleNullValues() throws Exception {
        TableMetadata tableMetadata = new TableMetadata(
            "TestEntity",
            TestEntity.class.getName(),
            List.of(
                new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                new FieldMetadata("age", io.memris.core.TypeCodes.TYPE_INT, false, false)
            )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(tableMetadata);
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class).newInstance(32, 4);
        
        // Insert with null name
        table.insertFrom(new Object[]{1L, null, 30});
        
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(metadata);
        HeapRuntimeKernel kernel = new HeapRuntimeKernel(table);
        
        TestEntity entity = materializer.materialize(kernel, 0);
        
        assertNotNull(entity);
        assertNull(entity.name);
        assertEquals(30, entity.age);
    }
    
    @Test
    @DisplayName("should materialize multiple entities")
    void materializeMultipleEntities() throws Exception {
        TableMetadata tableMetadata = new TableMetadata(
            "TestEntity",
            TestEntity.class.getName(),
            List.of(
                new FieldMetadata("id", io.memris.core.TypeCodes.TYPE_LONG, true, true),
                new FieldMetadata("name", io.memris.core.TypeCodes.TYPE_STRING, false, false),
                new FieldMetadata("age", io.memris.core.TypeCodes.TYPE_INT, false, false)
            )
        );
        
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(tableMetadata);
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class).newInstance(32, 4);
        
        // Insert multiple rows
        table.insertFrom(new Object[]{1L, "Alice", 30});
        table.insertFrom(new Object[]{2L, "Bob", 25});
        table.insertFrom(new Object[]{3L, "Charlie", 35});
        
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(metadata);
        HeapRuntimeKernel kernel = new HeapRuntimeKernel(table);
        
        // Materialize each row
        TestEntity alice = materializer.materialize(kernel, 0);
        TestEntity bob = materializer.materialize(kernel, 1);
        TestEntity charlie = materializer.materialize(kernel, 2);
        
        assertEquals("Alice", alice.name);
        assertEquals("Bob", bob.name);
        assertEquals("Charlie", charlie.name);
        
        assertEquals(30, alice.age);
        assertEquals(25, bob.age);
        assertEquals(35, charlie.age);
    }
    
    // Test entity
    public static class TestEntity {
        public Long id;
        public String name;
        public Integer age;
    }
}
