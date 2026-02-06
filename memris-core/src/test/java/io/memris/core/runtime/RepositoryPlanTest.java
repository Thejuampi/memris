package io.memris.core.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.repository.EntitySaverGenerator;
import io.memris.storage.GeneratedTable;
import io.memris.storage.heap.AbstractTable;
import io.memris.query.CompiledQuery;
import io.memris.runtime.EntitySaver;
import io.memris.runtime.RepositoryPlan;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TDD Tests for RepositoryPlan with EntitySaver integration.
 */
class RepositoryPlanTest {

    @Test
    void repositoryPlanShouldCarryEntitySaver() throws Exception {
        // Create a simple entity class
        Class<TestEntity> entityClass = TestEntity.class;
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(entityClass);

        // Generate EntitySaver
        EntitySaver<TestEntity, ?> entitySaver = EntitySaverGenerator.generate(entityClass, metadata);
        assertNotNull(entitySaver);

        // Create minimal RepositoryPlan with EntitySaver
        CompiledQuery[] queries = new CompiledQuery[0];
        String[] columnNames = new String[] { "id", "name" };
        byte[] typeCodes = new byte[] { 0, 8 }; // LONG, STRING

        RepositoryPlan<TestEntity> plan = RepositoryPlan.<TestEntity>builder()
                .entityClass(entityClass)
                .idColumnName("id")
                .queries(queries)
                .table(null) // Not needed for this test
                .kernel(null) // Not needed for this test
                .columnNames(columnNames)
                .typeCodes(typeCodes)
                .entitySaver(entitySaver)
                .build();

        // Verify EntitySaver is present
        assertNotNull(plan.entitySaver());
        assertSame(entitySaver, plan.entitySaver());
    }

    @Test
    void repositoryPlanShouldReturnNullEntitySaverWhenNotSet() {
        // Create minimal RepositoryPlan WITHOUT EntitySaver
        CompiledQuery[] queries = new CompiledQuery[0];
        String[] columnNames = new String[] { "id" };
        byte[] typeCodes = new byte[] { 0 };

        RepositoryPlan<TestEntity> plan = RepositoryPlan.<TestEntity>builder()
                .entityClass(TestEntity.class)
                .idColumnName("id")
                .queries(queries)
                .table(null)
                .kernel(null)
                .columnNames(columnNames)
                .typeCodes(typeCodes)
                .build();

        // Verify EntitySaver is null when not set
        assertNull(plan.entitySaver());
    }

    @Test
    void entitySaverShouldBeUsableFromRepositoryPlan() throws Exception {
        // Create entity and metadata
        Class<TestEntity> entityClass = TestEntity.class;
        EntityMetadata<TestEntity> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        EntitySaver<TestEntity, ?> entitySaver = EntitySaverGenerator.generate(entityClass, metadata);

        // Build plan with EntitySaver
        RepositoryPlan<TestEntity> plan = RepositoryPlan.<TestEntity>builder()
                .entityClass(entityClass)
                .idColumnName("id")
                .queries(new CompiledQuery[0])
                .table(null)
                .kernel(null)
                .columnNames(new String[] { "id", "name" })
                .typeCodes(new byte[] { 0, 8 })
                .entitySaver(entitySaver)
                .build();

        // Verify EntitySaver works through plan
        TestEntity entity = new TestEntity();
        entity.name = "Test";

        // Use raw type to work around wildcard capture issues
        @SuppressWarnings("rawtypes")
        EntitySaver rawSaver = plan.entitySaver();

        // Test extractId (should return null for new entity)
        Object id = rawSaver.extractId(entity);
        assertNull(id);

        // Test setId
        rawSaver.setId(entity, 42L);
        assertEquals(42L, entity.id);

        // Test extractId after setting
        id = rawSaver.extractId(entity);
        assertEquals(42L, id);
    }

    // Test entity class
    public static class TestEntity {
        @Id
        public Long id;
        public String name;
    }
}
