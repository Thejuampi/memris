package io.memris.runtime;

import io.memris.core.ColumnAccessPlan;
import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityMaterializerGeneratorHelperTest {

    @Test
    void shouldReadBoxedValuesAndHandleNullPresence() throws Exception {
        GeneratedTable table = createHelperTable();
        table.insertFrom(new Object[] {
                1L, 123, true, (byte) 7, (short) 8, 'X', 1.5f, 3.25d, "alpha", 99L
        });
        table.insertFrom(new Object[] {
                2L, null, null, null, null, null, null, null, null, null
        });

        int presentRow = Selection.index(table.lookupById(1L));
        int nullRow = Selection.index(table.lookupById(2L));

        assertThat(EntityMaterializerGenerator.readBoxedInt(table, 1, presentRow)).isEqualTo(123);
        assertThat(EntityMaterializerGenerator.readBoxedBoolean(table, 2, presentRow)).isEqualTo(true);
        assertThat(EntityMaterializerGenerator.readBoxedByte(table, 3, presentRow)).isEqualTo((byte) 7);
        assertThat(EntityMaterializerGenerator.readBoxedShort(table, 4, presentRow)).isEqualTo((short) 8);
        assertThat(EntityMaterializerGenerator.readBoxedChar(table, 5, presentRow)).isEqualTo('X');
        assertThat(EntityMaterializerGenerator.readBoxedFloat(table, 6, presentRow)).isEqualTo(1.5f);
        assertThat(EntityMaterializerGenerator.readBoxedDouble(table, 7, presentRow)).isEqualTo(3.25d);
        assertThat(EntityMaterializerGenerator.readStringIfPresent(table, 8, presentRow)).isEqualTo("alpha");
        assertThat(EntityMaterializerGenerator.readBoxedLong(table, 9, presentRow)).isEqualTo(99L);

        assertThat(EntityMaterializerGenerator.readBoxedInt(table, 1, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedBoolean(table, 2, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedByte(table, 3, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedShort(table, 4, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedChar(table, 5, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedFloat(table, 6, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedDouble(table, 7, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readStringIfPresent(table, 8, nullRow)).isNull();
        assertThat(EntityMaterializerGenerator.readBoxedLong(table, 9, nullRow)).isNull();
    }

    @Test
    void shouldGuardSetWithPlanIfPresent() {
        ColumnAccessPlan plan = ColumnAccessPlan.compile(PlanOwner.class, "nested.city");
        PlanOwner owner = new PlanOwner();

        EntityMaterializerGenerator.setWithPlanIfPresent(plan, owner, null);
        assertThat(owner.nested).isNull();

        EntityMaterializerGenerator.setWithPlanIfPresent(null, owner, "ignored");
        assertThat(owner.nested).isNull();

        EntityMaterializerGenerator.setWithPlanIfPresent(plan, null, "ignored");

        EntityMaterializerGenerator.setWithPlanIfPresent(plan, owner, "Lima");
        assertThat(owner.nested).isNotNull();
        assertThat(owner.nested.city).isEqualTo("Lima");
    }

    @Test
    void shouldRejectEntityWithoutPublicNoArgConstructor() {
        EntityMaterializerGenerator generator = new EntityMaterializerGenerator();
        var metadata = MetadataExtractor.extractEntityMetadata(PackagePrivateCtorEntity.class);

        assertThatThrownBy(() -> generator.generate(metadata))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires public no-arg constructor");
    }

    private static GeneratedTable createHelperTable() throws Exception {
        TableMetadata tableMetadata = new TableMetadata(
                "HelperValues",
                "io.memris.runtime.EntityMaterializerGeneratorHelperTest$HelperValues",
                List.of(
                        new FieldMetadata("id", TypeCodes.TYPE_LONG, true, true),
                        new FieldMetadata("intCol", TypeCodes.TYPE_INT, false, false),
                        new FieldMetadata("boolCol", TypeCodes.TYPE_BOOLEAN, false, false),
                        new FieldMetadata("byteCol", TypeCodes.TYPE_BYTE, false, false),
                        new FieldMetadata("shortCol", TypeCodes.TYPE_SHORT, false, false),
                        new FieldMetadata("charCol", TypeCodes.TYPE_CHAR, false, false),
                        new FieldMetadata("floatCol", TypeCodes.TYPE_FLOAT, false, false),
                        new FieldMetadata("doubleCol", TypeCodes.TYPE_DOUBLE, false, false),
                        new FieldMetadata("strCol", TypeCodes.TYPE_STRING, false, false),
                        new FieldMetadata("longCol", TypeCodes.TYPE_LONG, false, false)));

        Class<? extends AbstractTable> tableClass = TableGenerator.generate(tableMetadata);
        return (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(32, 4, 1);
    }

    @Entity
    static class PlanOwner {
        @Id
        public Long id;
        public PlanNested nested;
    }

    static class PlanNested {
        public String city;

        public PlanNested() {
        }
    }

    @Entity
    static class PackagePrivateCtorEntity {
        @Id
        public Long id;
        public String name;

        PackagePrivateCtorEntity() {
        }
    }
}
