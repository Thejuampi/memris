package io.memris.spring;

import io.memris.spring.converter.TypeConverter;
import io.memris.storage.ffm.FfmTable;

import java.lang.invoke.MethodHandle;
import java.util.Map;

/**
 * Layout metadata for an entity type.
 * <p>
 * Contains generation-time metadata needed to generate an EntityHydrator:
 * - Field layouts with columnId mappings
 * - Converters (nullable per field)
 * - Setter strategies
 * - Related hydrators and tables for relationship handling
 * <p>
 * This is compiled ONCE at repository creation time and then used
 * to generate zero-reflection hydrator code.
 * <p>
 * <b>Design note:</b> Uses primitive types throughout (int, byte) to avoid
 * boxing overhead. No Optional in hot paths - uses null for nullable values.
 */
public final class EntityLayout<T> {

    private final Class<T> entityClass;
    private final FieldLayout[] fields;
    private final String idColumnName;

    // Related hydrators and tables for relationship handling
    private final Map<String, EntityHydrator<?>> relatedHydrators;
    private final Map<String, FfmTable> relatedTables;

    private EntityLayout(
            Class<T> entityClass,
            FieldLayout[] fields,
            String idColumnName,
            Map<String, EntityHydrator<?>> relatedHydrators,
            Map<String, FfmTable> relatedTables) {
        this.entityClass = entityClass;
        this.fields = fields;
        this.idColumnName = idColumnName;
        this.relatedHydrators = relatedHydrators != null ? relatedHydrators : Map.of();
        this.relatedTables = relatedTables != null ? relatedTables : Map.of();
    }

    public static <T> EntityLayout<T> of(
            Class<T> entityClass,
            FieldLayout[] fields,
            String idColumnName) {
        return new EntityLayout<>(entityClass, fields, idColumnName, null, null);
    }

    public static <T> EntityLayout<T> of(
            Class<T> entityClass,
            FieldLayout[] fields,
            String idColumnName,
            Map<String, EntityHydrator<?>> relatedHydrators,
            Map<String, FfmTable> relatedTables) {
        return new EntityLayout<>(entityClass, fields, idColumnName, relatedHydrators, relatedTables);
    }

    public Class<T> entityClass() {
        return entityClass;
    }

    public FieldLayout[] fields() {
        return fields;
    }

    public String idColumnName() {
        return idColumnName;
    }

    public Map<String, EntityHydrator<?>> relatedHydrators() {
        return relatedHydrators;
    }

    public Map<String, FfmTable> relatedTables() {
        return relatedTables;
    }

    /**
     * Layout metadata for a single entity field.
     * <p>
     * Uses primitive types throughout (int, byte) for zero-allocation metadata.
     * Converters are stored as nullable references (no Optional wrapper).
     */
    public static final class FieldLayout {
        private final String propertyPath;     // e.g., "address.zip"
        private final String columnName;       // e.g., "address_zip"
        private final int columnId;            // stable column ID (primitive, not boxed)
        private final byte typeCode;           // TypeCodes.* (byte, not int)
        private final Class<?> javaType;
        private final Class<?> storageType;
        private final TypeConverter<?, ?> converterOrNull;  // nullable, no Optional wrapper
        private final SetterStrategy setterStrategy;
        private final MethodHandle setterHandleOrNull;  // if MethodHandle setter

        // Relationship metadata
        private final boolean isRelationship;
        private final EntityMetadata.FieldMapping.RelationshipType relationshipType;
        private final Class<?> targetEntity;
        private final String joinTable;
        private final boolean isCollection;
        private final boolean isEmbedded;

        private FieldLayout(
                String propertyPath,
                String columnName,
                int columnId,
                byte typeCode,
                Class<?> javaType,
                Class<?> storageType,
                TypeConverter<?, ?> converterOrNull,
                SetterStrategy setterStrategy,
                MethodHandle setterHandleOrNull,
                boolean isRelationship,
                EntityMetadata.FieldMapping.RelationshipType relationshipType,
                Class<?> targetEntity,
                String joinTable,
                boolean isCollection,
                boolean isEmbedded) {
            this.propertyPath = propertyPath;
            this.columnName = columnName;
            this.columnId = columnId;
            this.typeCode = typeCode;
            this.javaType = javaType;
            this.storageType = storageType;
            this.converterOrNull = converterOrNull;
            this.setterStrategy = setterStrategy;
            this.setterHandleOrNull = setterHandleOrNull;
            this.isRelationship = isRelationship;
            this.relationshipType = relationshipType;
            this.targetEntity = targetEntity;
            this.joinTable = joinTable;
            this.isCollection = isCollection;
            this.isEmbedded = isEmbedded;
        }

        public String propertyPath() {
            return propertyPath;
        }

        public String columnName() {
            return columnName;
        }

        public int columnId() {
            return columnId;
        }

        public byte typeCode() {
            return typeCode;
        }

        public Class<?> javaType() {
            return javaType;
        }

        public Class<?> storageType() {
            return storageType;
        }

        /**
         * Returns the converter for this field, or null if none.
         * Does not use Optional to avoid allocation in generated code paths.
         */
        public TypeConverter<?, ?> converterOrNull() {
            return converterOrNull;
        }

        public SetterStrategy setterStrategy() {
            return setterStrategy;
        }

        /**
         * Returns the setter MethodHandle if available, or null.
         * Does not use Optional to avoid allocation in generated code paths.
         */
        public MethodHandle setterHandleOrNull() {
            return setterHandleOrNull;
        }

        /**
         * Returns whether this field is a relationship.
         */
        public boolean isRelationship() {
            return isRelationship;
        }

        /**
         * Returns the relationship type.
         */
        public EntityMetadata.FieldMapping.RelationshipType relationshipType() {
            return relationshipType;
        }

        /**
         * Returns the target entity class for relationships.
         */
        public Class<?> targetEntity() {
            return targetEntity;
        }

        /**
         * Returns the join table name for @ManyToMany relationships.
         */
        public String joinTable() {
            return joinTable;
        }

        /**
         * Returns whether this field is a collection.
         */
        public boolean isCollection() {
            return isCollection;
        }

        /**
         * Returns whether this field is @Embedded.
         */
        public boolean isEmbedded() {
            return isEmbedded;
        }

        /**
         * How to set a value into the entity field.
         * <p>
         * This is used during code generation to determine the strategy
         * for setting field values in the generated hydrator.
         */
        public enum SetterStrategy {
            /** Direct setter method call (preferred) - invokevirtual in bytecode */
            DIRECT_METHOD,
            /** MethodHandle invoke (second best) - invokevirtual in bytecode */
            METHOD_HANDLE,
            /** Direct field access (fallback) - putfield in bytecode */
            DIRECT_FIELD
        }

        public static Builder builder() {
            return new Builder();
        }

        /**
         * Builder for FieldLayout.
         * <p>
         * Uses primitive types (int, byte) instead of boxed types.
         * Uses nullable references instead of Optional to avoid allocation.
         */
        public static final class Builder {
            private String propertyPath;
            private String columnName;
            private int columnId;
            private byte typeCode;
            private Class<?> javaType;
            private Class<?> storageType;
            private TypeConverter<?, ?> converterOrNull;
            private SetterStrategy setterStrategy;
            private MethodHandle setterHandleOrNull;

            // Relationship fields
            private boolean isRelationship;
            private EntityMetadata.FieldMapping.RelationshipType relationshipType;
            private Class<?> targetEntity;
            private String joinTable;
            private boolean isCollection;
            private boolean isEmbedded;

            private Builder() {}

            public Builder propertyPath(String propertyPath) {
                this.propertyPath = propertyPath;
                return this;
            }

            public Builder columnName(String columnName) {
                this.columnName = columnName;
                return this;
            }

            public Builder columnId(int columnId) {
                this.columnId = columnId;
                return this;
            }

            public Builder typeCode(byte typeCode) {
                this.typeCode = typeCode;
                return this;
            }

            public Builder javaType(Class<?> javaType) {
                this.javaType = javaType;
                return this;
            }

            public Builder storageType(Class<?> storageType) {
                this.storageType = storageType;
                return this;
            }

            public Builder converterOrNull(TypeConverter<?, ?> converterOrNull) {
                this.converterOrNull = converterOrNull;
                return this;
            }

            public Builder setterStrategy(SetterStrategy setterStrategy) {
                this.setterStrategy = setterStrategy;
                return this;
            }

            public Builder setterHandleOrNull(MethodHandle setterHandleOrNull) {
                this.setterHandleOrNull = setterHandleOrNull;
                return this;
            }

            // Relationship field builders
            public Builder isRelationship(boolean isRelationship) {
                this.isRelationship = isRelationship;
                return this;
            }

            public Builder relationshipType(EntityMetadata.FieldMapping.RelationshipType relationshipType) {
                this.relationshipType = relationshipType;
                return this;
            }

            public Builder targetEntity(Class<?> targetEntity) {
                this.targetEntity = targetEntity;
                return this;
            }

            public Builder joinTable(String joinTable) {
                this.joinTable = joinTable;
                return this;
            }

            public Builder isCollection(boolean isCollection) {
                this.isCollection = isCollection;
                return this;
            }

            public Builder isEmbedded(boolean isEmbedded) {
                this.isEmbedded = isEmbedded;
                return this;
            }

            public FieldLayout build() {
                if (propertyPath == null) throw new IllegalStateException("propertyPath required");
                if (columnName == null) throw new IllegalStateException("columnName required");
                if (javaType == null) throw new IllegalStateException("javaType required");
                if (storageType == null) throw new IllegalStateException("storageType required");
                if (setterStrategy == null) throw new IllegalStateException("setterStrategy required");
                return new FieldLayout(
                        propertyPath, columnName, columnId, typeCode,
                        javaType, storageType, converterOrNull, setterStrategy, setterHandleOrNull,
                        isRelationship, relationshipType, targetEntity, joinTable, isCollection, isEmbedded);
            }
        }
    }
}
