package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.Id;
import io.memris.core.ManyToOne;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.converter.TypeConverter;
import io.memris.storage.GeneratedTable;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    @DisplayName("should materialize wide scalar types and skip relationship field")
    void shouldMaterializeWideScalarTypesAndSkipRelationshipField() throws Exception {
        EntityMetadata<WideTypeEntity> metadata = MetadataExtractor.extractEntityMetadata(WideTypeEntity.class);
        GeneratedTable table = tableFromMetadata("WideTypeEntity", metadata);

        long instantEpoch = 1_700_000_000_123L;
        long localDateEpoch = LocalDate.of(2025, 1, 2).toEpochDay();
        long localDateTimeEpoch = LocalDateTime.of(2025, 1, 3, 4, 5, 6).toInstant(ZoneOffset.UTC).toEpochMilli();
        long legacyDateEpoch = 1_700_000_999_000L;

        Object[] values = rowValues(metadata, Map.ofEntries(
                Map.entry("id", 10L),
                Map.entry("enabled", true),
                Map.entry("tiny", (byte) 7),
                Map.entry("small", (short) 123),
                Map.entry("grade", 'Q'),
                Map.entry("ratioF", 1.5f),
                Map.entry("ratioD", 2.25d),
                Map.entry("createdAt", instantEpoch),
                Map.entry("deliveryDate", localDateEpoch),
                Map.entry("updatedAt", localDateTimeEpoch),
                Map.entry("legacyDate", legacyDateEpoch),
                Map.entry("amount", "12.34"),
                Map.entry("units", "56789"),
                Map.entry("name", "wide-row"),
                Map.entry("related", 44L)));
        table.insertFrom(values);

        EntityMaterializerImpl<WideTypeEntity> materializer = new EntityMaterializerImpl<>(metadata);
        WideTypeEntity entity = materializer.materialize(new HeapRuntimeKernel(table), 0);

        assertThat(entity.id).isEqualTo(10L);
        assertThat(entity.enabled).isTrue();
        assertThat(entity.tiny).isEqualTo((byte) 7);
        assertThat(entity.small).isEqualTo((short) 123);
        assertThat(entity.grade).isEqualTo('Q');
        assertThat(entity.ratioF).isEqualTo(1.5f);
        assertThat(entity.ratioD).isEqualTo(2.25d);
        assertThat(entity.createdAt).isEqualTo(Instant.ofEpochMilli(instantEpoch));
        assertThat(entity.deliveryDate).isEqualTo(LocalDate.ofEpochDay(localDateEpoch));
        assertThat(entity.updatedAt).isEqualTo(LocalDateTime.ofInstant(Instant.ofEpochMilli(localDateTimeEpoch), ZoneOffset.UTC));
        assertThat(entity.legacyDate).isEqualTo(new Date(legacyDateEpoch));
        assertThat(entity.amount).isEqualByComparingTo(new BigDecimal("12.34"));
        assertThat(entity.units).isEqualTo(new BigInteger("56789"));
        assertThat(entity.name).isEqualTo("wide-row");
        assertThat(entity.related).isNull();
    }

    @Test
    @DisplayName("should wrap failures when metadata constructor is missing")
    void shouldWrapFailuresWhenMetadataConstructorIsMissing() throws Exception {
        EntityMetadata<TestEntity> base = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        EntityMetadata<TestEntity> noCtorMetadata = copyMetadata(base, null, base.fields(), base.converters());
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(noCtorMetadata);

        GeneratedTable table = simpleTestTable();
        table.insertFrom(new Object[] { 1L, "Alice", 30 });

        assertThatThrownBy(() -> materializer.materialize(new HeapRuntimeKernel(table), 0))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to materialize entity");
    }

    @Test
    @DisplayName("should wrap unknown type code during materialization")
    void shouldWrapUnknownTypeCodeDuringMaterialization() throws Exception {
        EntityMetadata<TestEntity> base = MetadataExtractor.extractEntityMetadata(TestEntity.class);
        List<FieldMapping> remapped = new ArrayList<>(base.fields().size());
        for (FieldMapping mapping : base.fields()) {
            if ("age".equals(mapping.name())) {
                remapped.add(new FieldMapping(
                        mapping.name(),
                        mapping.columnName(),
                        mapping.javaType(),
                        mapping.storageType(),
                        mapping.columnPosition(),
                        (byte) 99,
                        mapping.isRelationship(),
                        mapping.relationshipType(),
                        mapping.targetEntity(),
                        mapping.joinTable(),
                        mapping.referencedColumnName(),
                        mapping.mappedBy(),
                        mapping.isCollection(),
                        mapping.isEmbedded()));
            } else {
                remapped.add(mapping);
            }
        }
        EntityMetadata<TestEntity> badTypeMetadata = copyMetadata(base, base.entityConstructor(), remapped, base.converters());
        EntityMaterializerImpl<TestEntity> materializer = new EntityMaterializerImpl<>(badTypeMetadata);

        GeneratedTable table = simpleTestTable();
        table.insertFrom(new Object[] { 1L, "Alice", 30 });

        assertThatThrownBy(() -> materializer.materialize(new HeapRuntimeKernel(table), 0))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to materialize entity");
    }

    @Test
    @DisplayName("should throw when constructor method handle cannot be created")
    void shouldThrowWhenConstructorMethodHandleCannotBeCreated() throws Exception {
        Constructor<PrivateCtorEntity> privateCtor = PrivateCtorEntity.class.getDeclaredConstructor();
        EntityMetadata<PrivateCtorEntity> metadata = new EntityMetadata<>(
                PrivateCtorEntity.class,
                privateCtor,
                "id",
                List.of(new FieldMapping("id", "id", Long.class, long.class, 0, TypeCodes.TYPE_LONG)),
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

        assertThatThrownBy(() -> new EntityMaterializerImpl<>(metadata))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to create MethodHandle");
    }

    private GeneratedTable simpleTestTable() throws Exception {
        return tableFromMetadata("TestEntitySimple", MetadataExtractor.extractEntityMetadata(TestEntity.class));
    }

    private static GeneratedTable tableFromMetadata(String tableName, EntityMetadata<?> metadata) throws Exception {
        List<FieldMetadata> columns = metadata.fields().stream()
                .filter(mapping -> mapping.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(FieldMapping::columnPosition))
                .map(mapping -> new FieldMetadata(
                        mapping.columnName(),
                        mapping.typeCode(),
                        "id".equals(mapping.name()),
                        "id".equals(mapping.name())))
                .toList();
        TableMetadata tableMetadata = new TableMetadata(tableName, metadata.entityClass().getName(), columns);
        Class<? extends AbstractTable> tableClass = TableGenerator.generate(tableMetadata);
        return (GeneratedTable) tableClass.getConstructor(int.class, int.class, int.class).newInstance(64, 4, 1);
    }

    private static Object[] rowValues(EntityMetadata<?> metadata, Map<String, Object> byFieldName) {
        int maxCol = metadata.fields().stream()
                .filter(mapping -> mapping.columnPosition() >= 0)
                .mapToInt(FieldMapping::columnPosition)
                .max()
                .orElse(-1);
        Object[] values = new Object[maxCol + 1];
        for (FieldMapping mapping : metadata.fields()) {
            if (mapping.columnPosition() < 0) {
                continue;
            }
            values[mapping.columnPosition()] = byFieldName.get(mapping.name());
        }
        return values;
    }

    private static <T> EntityMetadata<T> copyMetadata(EntityMetadata<T> base,
            Constructor<T> ctor,
            List<FieldMapping> fields,
            Map<String, TypeConverter<?, ?>> converters) {
        return new EntityMetadata<>(
                base.entityClass(),
                ctor,
                base.idColumnName(),
                fields,
                base.foreignKeyColumns(),
                converters,
                base.fieldConverters(),
                base.indexDefinitions(),
                base.prePersistHandle(),
                base.postLoadHandle(),
                base.preUpdateHandle(),
                base.auditFields(),
                base.fieldGetters(),
                base.fieldSetters(),
                base.isRecord());
    }
    
    // Test entity
    public static class TestEntity {
        @Id
        public Long id;
        public String name;
        public Integer age;
    }

    public static class WideTypeEntity {
        @Id
        public Long id;
        public Boolean enabled;
        public Byte tiny;
        public Short small;
        public Character grade;
        public Float ratioF;
        public Double ratioD;
        public Instant createdAt;
        public LocalDate deliveryDate;
        public LocalDateTime updatedAt;
        public Date legacyDate;
        public BigDecimal amount;
        public BigInteger units;
        public String name;
        @ManyToOne
        public RelatedEntity related;
    }

    public static class RelatedEntity {
        @Id
        public Long id;
    }

    public static class PrivateCtorEntity {
        @Id
        public Long id;

        private PrivateCtorEntity() {
        }
    }
}
