package io.memris.core;

import io.memris.core.converter.TypeConverter;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EntityMetadataCoverageTest {

    @Test
    void shouldResolvePlansByPropertyAndColumnNameAndFallbackCompile() throws Exception {
        var metadata = MetadataExtractor.extractEntityMetadata(Owner.class);

        var byProperty = metadata.columnAccessPlan("profile.city");
        var byColumnName = metadata.columnAccessPlan("profile_city");

        assertThat(byProperty).isNotNull();
        assertThat(byColumnName).isNotNull();
        assertThat(byColumnName.propertyPath()).isEqualTo("profile.city");

        var manual = new EntityMetadata<>(
                Owner.class,
                Owner.class.getDeclaredConstructor(),
                "id",
                metadata.fields(),
                metadata.foreignKeyColumns(),
                metadata.converters(),
                metadata.fieldConverters(),
                metadata.indexDefinitions(),
                metadata.prePersistHandle(),
                metadata.postLoadHandle(),
                metadata.preUpdateHandle(),
                metadata.auditFields(),
                metadata.fieldGetters(),
                metadata.fieldSetters(),
                false);

        var fallback = manual.columnAccessPlan("profile.city");
        assertThat(fallback).isNotNull();
        assertThat(fallback.propertyPath()).isEqualTo("profile.city");

        assertThatThrownBy(() -> manual.columnAccessPlan("missing.path"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column access plan not found");
    }

    @Test
    void shouldCoverLookupsAndRecordHelpers() {
        var mapping = new EntityMetadata.FieldMapping(
                "profile.city",
                "profile_city",
                String.class,
                String.class,
                2,
                TypeCodes.TYPE_STRING);

        var metadata = new EntityMetadata<>(
                Owner.class,
                null,
                "id",
                List.of(
                        new EntityMetadata.FieldMapping("id", "id", Long.class, Long.class, 0, TypeCodes.TYPE_LONG),
                        mapping),
                Set.of(),
                Map.of(),
                Map.of(),
                List.of(),
                null,
                null,
                null,
                List.of(new EntityMetadata.AuditField("createdAt", EntityMetadata.AuditFieldType.CREATED_DATE, long.class)),
                Map.of(),
                Map.of(),
                false,
                Map.of("profile.city", ColumnAccessPlan.compile(Owner.class, "profile.city")),
                new ColumnAccessPlan[3]);

        assertThat(metadata.entityClass()).isEqualTo(Owner.class);
        assertThat(metadata.idColumnName()).isEqualTo("id");
        assertThat(metadata.isRecord()).isFalse();
        assertThat(metadata.resolvePropertyPosition("profile.city")).isEqualTo(2);
        assertThat(metadata.resolveColumnPosition("profile_city")).isEqualTo(2);
        assertThatThrownBy(() -> metadata.resolvePropertyPosition("missing"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> metadata.resolveColumnPosition("missing_col"))
                .isInstanceOf(IllegalArgumentException.class);

        var def = new EntityMetadata.IndexDefinition(
                "idx",
                new String[] { "id", "profile.city" },
                new int[] { 0, 2 },
                new byte[] { TypeCodes.TYPE_LONG, TypeCodes.TYPE_STRING },
                Index.IndexType.HASH,
                "memris");
        assertThat(def.composite()).isTrue();
        assertThat(def.firstFieldName()).isEqualTo("id");

        var relationshipMapping = new EntityMetadata.FieldMapping(
                "rel",
                "rel_id",
                Owner.class,
                Long.class,
                1,
                TypeCodes.TYPE_LONG,
                false,
                EntityMetadata.FieldMapping.RelationshipType.MANY_TO_ONE,
                Owner.class,
                "join_t",
                "id",
                "mapped",
                true,
                true);
        assertThat(relationshipMapping.relationshipType()).isEqualTo(EntityMetadata.FieldMapping.RelationshipType.NONE);
        assertThat(relationshipMapping.targetEntity()).isNull();
        assertThat(relationshipMapping.joinTable()).isNull();
        assertThat(relationshipMapping.referencedColumnName()).isNull();
        assertThat(relationshipMapping.mappedBy()).isNull();
        assertThat(relationshipMapping.isCollection()).isFalse();
    }

    @Test
    void shouldUseLegacyConstructorDefaultsAndDefensiveCopies() throws Exception {
        var field = new EntityMetadata.FieldMapping("id", "id", Long.class, Long.class, 0, TypeCodes.TYPE_LONG);
        var metadata = new EntityMetadata<>(
                Owner.class,
                Owner.class.getDeclaredConstructor(),
                "id",
                List.of(field),
                Set.of("owner_id"),
                Map.of("id", new DummyConverter()),
                null,
                null,
                null,
                null,
                null,
                null,
                Map.of(),
                Map.of(),
                true);

        assertThat(metadata.foreignKeyColumns()).containsExactly("owner_id");
        assertThat(metadata.fieldConverters()).isEmpty();
        assertThat(metadata.indexDefinitions()).isEmpty();
        assertThat(metadata.auditFields()).isEmpty();
        assertThat(metadata.isRecord()).isTrue();
        assertThat(metadata.columnAccessPlansByColumn()).isEmpty();
    }

    @Test
    void shouldFailFallbackPlanResolutionWhenMappingIsNotPersisted() throws Exception {
        var nonPersisted = new EntityMetadata.FieldMapping(
                "transientPath",
                "transient_path",
                String.class,
                String.class,
                -1,
                TypeCodes.TYPE_STRING);
        var metadata = new EntityMetadata<>(
                Owner.class,
                Owner.class.getDeclaredConstructor(),
                "id",
                List.of(nonPersisted),
                Set.of(),
                Map.of(),
                Map.of(),
                List.of(),
                null,
                null,
                null,
                List.of(),
                Map.of(),
                Map.of(),
                false);

        assertThatThrownBy(() -> metadata.columnAccessPlan("transientPath"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Column access plan not found");
    }

    @Entity
    static class Owner {
        @Id
        public Long id;
        public Profile profile;
    }

    static class Profile {
        public String city;
    }

    @SuppressWarnings("unused")
    private static final class DummyConverter implements TypeConverter<String, String> {
        @Override
        public Class<String> javaType() {
            return String.class;
        }

        @Override
        public Class<String> storageType() {
            return String.class;
        }

        @Override
        public String toStorage(String javaValue) {
            return javaValue;
        }

        @Override
        public String fromStorage(String storageValue) {
            return storageValue;
        }
    }
}
