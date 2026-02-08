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

import static org.assertj.core.api.Assertions.assertThat;

class JoinMaterializerImplTest {

    @Test
    void hydrateReturnsWhenSetterMissing() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadata());

        long sourceRef = sourceTable.insertFrom(new Object[]{1L, 10L});
        targetTable.insertFrom(new Object[]{10L});

        JoinMaterializerImpl materializer = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, null, null);
        SourceEntity source = new SourceEntity();
        HeapRuntimeKernel targetKernel = new HeapRuntimeKernel(targetTable);
        EntityMaterializer<TargetEntity> targetMaterializer = (table, rowIndex) -> new TargetEntity(10L);

        materializer.hydrate(source, io.memris.storage.Selection.index(sourceRef), sourceTable, targetTable, targetKernel, targetMaterializer);

        assertThat(source.target).isNull();
    }

    @Test
    void hydrateSetsTargetAndInvokesPostLoad() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadata());

        long sourceRef = sourceTable.insertFrom(new Object[]{1L, 42L});
        targetTable.insertFrom(new Object[]{42L});

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setTarget", MethodType.methodType(void.class, TargetEntity.class));
        MethodHandle postLoad = MethodHandles.lookup()
                .findVirtual(TargetEntity.class, "postLoad", MethodType.methodType(void.class));

        JoinMaterializerImpl materializer = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, setter, postLoad);
        SourceEntity source = new SourceEntity();
        HeapRuntimeKernel targetKernel = new HeapRuntimeKernel(targetTable);
        EntityMaterializer<TargetEntity> targetMaterializer = (table, rowIndex) ->
                new TargetEntity(table.readLong(0, rowIndex));

        materializer.hydrate(source, io.memris.storage.Selection.index(sourceRef), sourceTable, targetTable, targetKernel, targetMaterializer);

        assertThat(source.target.postLoaded).isTrue();
    }

    private GeneratedTable newTable(TableMetadata metadata) throws Exception {
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(metadata);
        return (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);
    }

    private TableMetadata sourceMetadata() {
        return new TableMetadata(
                "Source",
                "io.memris.test.Source",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("fk", TypeCodes.TYPE_LONG, false, false)
                )
        );
    }

    private TableMetadata targetMetadata() {
        return new TableMetadata(
                "Target",
                "io.memris.test.Target",
                List.of(new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true))
        );
    }

    private static final class SourceEntity {
        private TargetEntity target;

        public void setTarget(TargetEntity target) {
            this.target = target;
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
    }
}
