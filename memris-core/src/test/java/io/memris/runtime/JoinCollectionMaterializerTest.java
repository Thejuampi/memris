package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class JoinCollectionMaterializerTest {

    @Test
    void hydrateShouldPopulateSetAndInvokePostLoad() throws Exception {
        GeneratedTable sourceTable = newTable(
                "SourceSet",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("ownerId", TypeCodes.TYPE_LONG, false, false)));

        GeneratedTable targetTable = newTable(
                "TargetSet",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("ownerId", TypeCodes.TYPE_LONG, false, false)));

        long sourceRef = sourceTable.insertFrom(new Object[] { 1L, 7L });
        targetTable.insertFrom(new Object[] { 100L, 7L });
        targetTable.insertFrom(new Object[] { 200L, 7L });

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setChildren", MethodType.methodType(void.class, Set.class));
        MethodHandle postLoad = MethodHandles.lookup()
                .findVirtual(TargetEntity.class, "postLoad", MethodType.methodType(void.class));

        JoinCollectionMaterializer materializer = new JoinCollectionMaterializer(
                1,
                1,
                TypeCodes.TYPE_LONG,
                setter,
                postLoad,
                Set.class);

        SourceEntity source = new SourceEntity();
        EntityMaterializer<TargetEntity> targetMaterializer = (table, rowIndex) ->
                new TargetEntity(table.readLong(0, rowIndex));

        materializer.hydrate(source,
                io.memris.storage.Selection.index(sourceRef),
                sourceTable,
                targetTable,
                targetMaterializer);

        assertThat(source.children).isNotNull();
        assertThat(source.children).hasSize(2);
        assertThat(source.children).allMatch(TargetEntity::isPostLoaded);
    }

    @Test
    void hydrateShouldReturnWhenSourceKeyNotPresent() throws Exception {
        GeneratedTable sourceTable = newTable(
                "SourceNull",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("ownerId", TypeCodes.TYPE_LONG, false, false)));

        GeneratedTable targetTable = newTable(
                "TargetNull",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("ownerId", TypeCodes.TYPE_LONG, false, false)));

        long sourceRef = sourceTable.insertFrom(new Object[] { 1L, null });

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setChildren", MethodType.methodType(void.class, Set.class));

        JoinCollectionMaterializer materializer = new JoinCollectionMaterializer(
                1,
                1,
                TypeCodes.TYPE_LONG,
                setter,
                null,
                Set.class);

        SourceEntity source = new SourceEntity();
        materializer.hydrate(source,
                io.memris.storage.Selection.index(sourceRef),
                sourceTable,
                targetTable,
                (table, rowIndex) -> new TargetEntity(0L));

        assertThat(source.children).isNull();
    }

    private GeneratedTable newTable(String name, List<FieldMetadata> fields) throws Exception {
        TableMetadata metadata = new TableMetadata(name, "io.memris.test." + name, fields);
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        return (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class)
                .newInstance(32, 4, 1);
    }

    private static final class SourceEntity {
        private Set<TargetEntity> children;

        public void setChildren(Set<TargetEntity> children) {
            this.children = children;
        }
    }

    private static final class TargetEntity {
        private final long id;
        private boolean postLoaded;

        private TargetEntity(long id) {
            this.id = id;
        }

        public void postLoad() {
            postLoaded = true;
        }

        boolean isPostLoaded() {
            return postLoaded;
        }
    }
}
