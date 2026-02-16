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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JoinMaterializerImplTest {

    @Test
    void hydrateReturnsWhenSetterMissing() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadata());

        long sourceRef = sourceTable.insertFrom(new Object[] { 1L, 10L });
        targetTable.insertFrom(new Object[] { 10L });

        JoinMaterializerImpl materializer = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, null, null);
        SourceEntity source = new SourceEntity();
        EntityMaterializer<TargetEntity> targetMaterializer = (table, rowIndex) -> new TargetEntity(10L);

        materializer.hydrate(source, io.memris.storage.Selection.index(sourceRef), sourceTable, targetTable,
                targetMaterializer);

        assertThat(source.target).isNull();
    }

    @Test
    void hydrateSetsTargetAndInvokesPostLoad() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadata());

        long sourceRef = sourceTable.insertFrom(new Object[] { 1L, 42L });
        targetTable.insertFrom(new Object[] { 42L });

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setTarget", MethodType.methodType(void.class, TargetEntity.class));
        MethodHandle postLoad = MethodHandles.lookup()
                .findVirtual(TargetEntity.class, "postLoad", MethodType.methodType(void.class));

        JoinMaterializerImpl materializer = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, setter, postLoad);
        SourceEntity source = new SourceEntity();
        EntityMaterializer<TargetEntity> targetMaterializer = (table,
                rowIndex) -> new TargetEntity(table.readLong(0, rowIndex));

        materializer.hydrate(source, io.memris.storage.Selection.index(sourceRef), sourceTable, targetTable,
                targetMaterializer);

        assertThat(source.target.postLoaded).isTrue();
    }

    @Test
    void hydrateReturnsWhenSourceFkMissingOrTargetMissing() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadata());

        long nullFkRef = sourceTable.insertFrom(new Object[] { 1L, null });
        long missingTargetRef = sourceTable.insertFrom(new Object[] { 2L, 999L });
        targetTable.insertFrom(new Object[] { 42L });

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setTarget", MethodType.methodType(void.class, TargetEntity.class));
        JoinMaterializerImpl materializer = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, setter, null);

        SourceEntity withNullFk = new SourceEntity();
        materializer.hydrate(withNullFk, io.memris.storage.Selection.index(nullFkRef), sourceTable, targetTable,
                (table, rowIndex) -> new TargetEntity(table.readLong(0, rowIndex)));
        assertThat(withNullFk.target).isNull();

        SourceEntity withMissingTarget = new SourceEntity();
        materializer.hydrate(withMissingTarget, io.memris.storage.Selection.index(missingTargetRef), sourceTable,
                targetTable, (table, rowIndex) -> new TargetEntity(table.readLong(0, rowIndex)));
        assertThat(withMissingTarget.target).isNull();
    }

    @Test
    void hydrateSupportsNonIdLookupAndMultipleFkReadTypes() throws Exception {
        GeneratedTable sourceTable = newTable(new TableMetadata(
                "SourceTyped",
                "io.memris.test.SourceTyped",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("fkInt", TypeCodes.TYPE_INT, false, false),
                        new FieldMetadata("fkShort", TypeCodes.TYPE_SHORT, false, false),
                        new FieldMetadata("fkByte", TypeCodes.TYPE_BYTE, false, false),
                        new FieldMetadata("fkLong", TypeCodes.TYPE_LONG, false, false))));

        GeneratedTable targetIntTable = newTable(new TableMetadata(
                "TargetInt",
                "io.memris.test.TargetInt",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("code", TypeCodes.TYPE_INT, false, false))));

        GeneratedTable targetLongTable = newTable(new TableMetadata(
                "TargetLong",
                "io.memris.test.TargetLong",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("code", TypeCodes.TYPE_LONG, false, false))));

        long sourceRef = sourceTable.insertFrom(new Object[] { 1L, 7, (short) 7, (byte) 7, 700L });
        long sourceNoMatchRef = sourceTable.insertFrom(new Object[] { 2L, 999, (short) 7, (byte) 7, 700L });
        targetIntTable.insertFrom(new Object[] { 10L, 7 });
        targetLongTable.insertFrom(new Object[] { 20L, 700L });

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setTarget", MethodType.methodType(void.class, TargetEntity.class));
        EntityMaterializer<TargetEntity> targetMaterializer = (table, rowIndex) -> new TargetEntity(table.readLong(0, rowIndex));
        int sourceRow = io.memris.storage.Selection.index(sourceRef);

        SourceEntity intJoin = new SourceEntity();
        new JoinMaterializerImpl(1, 1, false, TypeCodes.TYPE_INT, setter, null)
                .hydrate(intJoin, sourceRow, sourceTable, targetIntTable, targetMaterializer);
        assertThat(intJoin.target.id).isEqualTo(10L);

        SourceEntity shortJoin = new SourceEntity();
        new JoinMaterializerImpl(2, 1, false, TypeCodes.TYPE_SHORT, setter, null)
                .hydrate(shortJoin, sourceRow, sourceTable, targetIntTable, targetMaterializer);
        assertThat(shortJoin.target.id).isEqualTo(10L);

        SourceEntity byteJoin = new SourceEntity();
        new JoinMaterializerImpl(3, 1, false, TypeCodes.TYPE_BYTE, setter, null)
                .hydrate(byteJoin, sourceRow, sourceTable, targetIntTable, targetMaterializer);
        assertThat(byteJoin.target.id).isEqualTo(10L);

        SourceEntity defaultTypeJoin = new SourceEntity();
        new JoinMaterializerImpl(4, 1, false, (byte) 99, setter, null)
                .hydrate(defaultTypeJoin, sourceRow, sourceTable, targetLongTable, targetMaterializer);
        assertThat(defaultTypeJoin.target.id).isEqualTo(20L);

        SourceEntity noMatch = new SourceEntity();
        int sourceNoMatchRow = io.memris.storage.Selection.index(sourceNoMatchRef);
        new JoinMaterializerImpl(1, 1, false, TypeCodes.TYPE_INT, setter, null)
                .hydrate(noMatch, sourceNoMatchRow, sourceTable, targetIntTable, targetMaterializer);
        assertThat(noMatch.target).isNull();
    }

    @Test
    void hydrateWrapsSetterAndPostLoadFailures() throws Exception {
        GeneratedTable sourceTable = newTable(sourceMetadata());
        GeneratedTable targetTable = newTable(targetMetadata());
        long sourceRef = sourceTable.insertFrom(new Object[] { 1L, 42L });
        targetTable.insertFrom(new Object[] { 42L });

        int sourceRow = io.memris.storage.Selection.index(sourceRef);
        MethodHandle throwingSetter = MethodHandles.lookup()
                .findVirtual(ThrowingSourceEntity.class, "setTarget", MethodType.methodType(void.class, TargetEntity.class));
        JoinMaterializerImpl setterFail = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, throwingSetter, null);

        assertThatThrownBy(() -> setterFail.hydrate(new ThrowingSourceEntity(), sourceRow, sourceTable, targetTable,
                (table, rowIndex) -> new TargetEntity(table.readLong(0, rowIndex))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to set joined entity");

        MethodHandle setter = MethodHandles.lookup()
                .findVirtual(SourceEntity.class, "setTarget", MethodType.methodType(void.class, TargetEntity.class));
        MethodHandle throwingPostLoad = MethodHandles.lookup()
                .findVirtual(TargetEntity.class, "postLoadFail", MethodType.methodType(void.class));
        JoinMaterializerImpl postLoadFail = new JoinMaterializerImpl(1, 0, true, TypeCodes.TYPE_LONG, setter, throwingPostLoad);

        assertThatThrownBy(() -> postLoadFail.hydrate(new SourceEntity(), sourceRow, sourceTable, targetTable,
                (table, rowIndex) -> new TargetEntity(table.readLong(0, rowIndex))))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to invoke postLoad");
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
                        new FieldMetadata("fk", TypeCodes.TYPE_LONG, false, false)));
    }

    private TableMetadata targetMetadata() {
        return new TableMetadata(
                "Target",
                "io.memris.test.Target",
                List.of(new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true)));
    }

    private static final class SourceEntity {
        private TargetEntity target;

        public void setTarget(TargetEntity target) {
            this.target = target;
        }
    }

    private static final class ThrowingSourceEntity {
        public void setTarget(TargetEntity target) {
            throw new IllegalStateException("setter boom");
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

        public void postLoadFail() {
            throw new IllegalStateException("postLoad boom");
        }
    }
}
