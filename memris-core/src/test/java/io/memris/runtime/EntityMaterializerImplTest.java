package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.storage.GeneratedTable;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);
        
        // Insert test data
        table.insertFrom(new Object[]{1L, "Alice", 30});
        
        // Create metadata and materializer
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(metadata);
        
        // Create kernel with table
        HeapRuntimeKernel kernel = new HeapRuntimeKernel(table);
        
        // Materialize entity
        TestEntity entity = materializer.materialize(kernel, 0);
        
        assertThat(entity).isNotNull();
        assertThat(entity.id).isEqualTo(1L);
        assertThat(entity.name).isEqualTo("Alice");
        assertThat(entity.age).isEqualTo(30);
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
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);
        
        // Insert with null name
        table.insertFrom(new Object[]{1L, null, 30});
        
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(metadata);
        HeapRuntimeKernel kernel = new HeapRuntimeKernel(table);
        
        TestEntity entity = materializer.materialize(kernel, 0);
        
        assertThat(entity).isNotNull();
        assertThat(entity.name).isNull();
        assertThat(entity.age).isEqualTo(30);
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
        GeneratedTable table = (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);
        
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
        
        assertThat(alice.name).isEqualTo("Alice");
        assertThat(bob.name).isEqualTo("Bob");
        assertThat(charlie.name).isEqualTo("Charlie");
        
        assertThat(alice.age).isEqualTo(30);
        assertThat(bob.age).isEqualTo(25);
        assertThat(charlie.age).isEqualTo(35);
    }
    
    // Test entity
    public static class TestEntity {
        @Id
        public Long id;
        public String name;
        public Integer age;
    }
}
