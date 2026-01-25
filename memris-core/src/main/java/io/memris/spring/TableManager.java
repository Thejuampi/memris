package io.memris.spring;

import java.lang.foreign.Arena;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.memris.index.HashIndex;
import io.memris.storage.ffm.FfmTable;
import io.memris.storage.ffm.FfmTable.ColumnSpec;

import jakarta.persistence.*;

/**
 * Manages table creation, caching, and lifecycle.
 * <p>
 * <b>Single Responsibility:</b> Create and manage FfmTable instances for entities.
 * <p>
 * Responsibilities:
 * <ul>
 * <li>Create tables for entity classes</li>
 * <li>Cache created tables for reuse</li>
 * <li>Create tables for nested @OneToOne/@ManyToOne entities</li>
 * <li>Create join tables for @ManyToMany relationships</li>
 <li>Cache enum values for custom types</li>
 * <li>Build indexes on tables</li>
 * </ul>
 */
public final class TableManager {

    private final Arena arena;
    private final Map<Class<?>, FfmTable> tables = new HashMap<>();
    private final Map<Class<?>, Map<String, Object>> enumValues = new HashMap<>();
    private final Map<String, FfmTable> joinTables = new HashMap<>();

    /**
     * Create a new TableManager.
     *
     * @param arena the Arena for memory allocation
     */
    public TableManager(Arena arena) {
        this.arena = arena;
    }

    /**
     * Get or create a table for the given entity class.
     *
     * @param entityClass the entity class
     * @return the table (cached or newly created)
     */
    public <T> FfmTable getOrCreateTable(Class<T> entityClass) {
        return tables.computeIfAbsent(entityClass, this::buildTable);
    }

    /**
     * Get a previously created table.
     *
     * @param entityClass the entity class
     * @return the table, or null if not created
     */
    public <T> FfmTable getTable(Class<T> entityClass) {
        @SuppressWarnings("unchecked")
        FfmTable table = (FfmTable) tables.get(entityClass);
        return table;
    }

    /**
     * Ensure that tables for nested @OneToOne/@ManyToOne entities exist.
     * <p>
     * This scans the entity class for relationships and creates tables
     * for any nested entity types that haven't been created yet.
     *
     * @param entityClass the entity class
     */
    @SuppressWarnings("unchecked")
    public <T> void ensureNestedTables(Class<T> entityClass) {
        for (var field : entityClass.getDeclaredFields()) {
            boolean hasOneToOne = field.isAnnotationPresent(OneToOne.class);
            boolean hasManyToOne = field.isAnnotationPresent(ManyToOne.class);
            if (hasOneToOne || hasManyToOne) {
                Class<?> nestedType = field.getType();
                boolean hasEntity = nestedType.isAnnotationPresent(Entity.class);
                if (hasEntity) {
                    getOrCreateTable((Class<Object>) nestedType);
                    ensureNestedTables((Class<Object>) nestedType);
                }
            }
        }
    }

    /**
     * Create join tables for @ManyToMany relationships.
     * <p>
     * Join tables store the many-to-many relationships between entities.
     * They are named using the convention: "{Entity1}_{Entity2}_join".
     *
     * @param entityClass the entity class
     * @return the created join table, or null if no @ManyToMany relationships
     */
    public <T> Map<String, FfmTable> createJoinTables(Class<T> entityClass) {
        Map<String, FfmTable> created = new HashMap<>();

        for (var field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(ManyToMany.class)) {
                Class<?> targetType = getCollectionElementType(field);
                if (targetType != null && targetType.isAnnotationPresent(Entity.class)) {
                    String joinTableName = entityClass.getSimpleName() + "_" + targetType.getSimpleName() + "_join";

                    if (!joinTables.containsKey(joinTableName)) {
                        List<ColumnSpec> columns = new ArrayList<>();

                        // Add foreign key columns for current entity
                        Class<?> sourceIdStorageType = getIdStorageType(entityClass);
                        if (sourceIdStorageType == UUID.class) {
                            columns.add(new ColumnSpec(entityClass.getSimpleName() + "_id_msb", long.class));
                            columns.add(new ColumnSpec(entityClass.getSimpleName() + "_id_lsb", long.class));
                        } else {
                            columns.add(new ColumnSpec(entityClass.getSimpleName() + "_id", sourceIdStorageType));
                        }

                        // Add foreign key columns for target entity
                        Class<?> targetIdStorageType = getIdStorageType(targetType);
                        if (targetIdStorageType == UUID.class) {
                            columns.add(new ColumnSpec(targetType.getSimpleName() + "_id_msb", long.class));
                            columns.add(new ColumnSpec(targetType.getSimpleName() + "_id_lsb", long.class));
                        } else {
                            columns.add(new ColumnSpec(targetType.getSimpleName() + "_id", targetIdStorageType));
                        }

                        FfmTable joinTable = new FfmTable(joinTableName, arena, columns);
                        joinTables.put(joinTableName, joinTable);
                        created.put(joinTableName, joinTable);
                    }
                }
            }
        }

        return created;
    }

    /**
     * Cache enum values for the given entity class.
     * <p>
     * This scans the entity class for enum fields and caches their
     * constant values for use in type conversion.
     *
     * @param entityClass the entity class
     */
    public <T> void cacheEnumValues(Class<T> entityClass) {
        Map<String, Object> enumMap = enumValues.computeIfAbsent(entityClass, k -> new HashMap<>());

        for (var field : entityClass.getDeclaredFields()) {
            if (field.isEnumConstant() || (field.getType().isEnum() && field.isAnnotationPresent(Enumerated.class))) {
                Class<?> enumType = field.getType();
                Object[] enumConstants = enumType.getEnumConstants();

                Map<String, Object> valueToName = new HashMap<>();
                for (Object constant : enumConstants) {
                    Enum<?> enumConstant = (Enum<?>) constant;
                    valueToName.put(enumConstant.name(), enumConstant);
                }

                enumMap.put(field.getName(), valueToName);
            }
        }
    }

    /**
     * Get cached enum values for an entity class and field.
     *
     * @param entityClass the entity class
     * @param fieldName the field name
     * @return map of enum value to enum constant, or null if not cached
     */
    public Map<String, Object> getEnumValues(Class<?> entityClass, String fieldName) {
        Map<String, Object> enumMap = enumValues.get(entityClass);
        if (enumMap == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        Map<String, Object> valueToName = (Map<String, Object>) enumMap.get(fieldName);
        return valueToName;
    }

    /**
     * Get a join table by name.
     *
     * @param joinTableName the join table name
     * @return the join table, or null if not found
     */
    public FfmTable getJoinTable(String joinTableName) {
        return joinTables.get(joinTableName);
    }

    /**
     * Build a table for the given entity class.
     * <p>
     * This is the core table creation logic. It scans the entity class fields
     * and creates appropriate column specifications based on field types.
     *
     * @param entityClass the entity class
     * @return the created table
     */
    private <T> FfmTable buildTable(Class<T> entityClass) {
        List<ColumnSpec> columns = new ArrayList<>();

        for (var field : entityClass.getDeclaredFields()) {
            // Check if field is an ID field (with @GeneratedValue or @jakarta.persistence.Id)
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                Class<?> type = field.getType();

                if (type == UUID.class) {
                    columns.add(new ColumnSpec(field.getName() + "_msb", long.class));
                    columns.add(new ColumnSpec(field.getName() + "_lsb", long.class));
                } else {
                    // Use TypeConverterRegistry for all ID types
                    var converter = io.memris.spring.converter.TypeConverterRegistry.getInstance().getConverter(type);
                    if (converter == null) {
                        throw new IllegalArgumentException("Unsupported ID type: " + type +
                            ". Register a TypeConverter for " + type.getName());
                    }
                    @SuppressWarnings("unchecked")
                    Class<?> storageType = (Class<?>) converter.getStorageType();
                    columns.add(new ColumnSpec(field.getName(), storageType));
                }
                continue;
            }

            // Skip @Transient fields
            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }

            // Handle @OneToOne, @ManyToOne (relationships)
            if (field.isAnnotationPresent(OneToOne.class) || field.isAnnotationPresent(ManyToOne.class)) {
                // These are foreign keys - don't create columns in this table
                continue;
            }

            // Handle @OneToMany, @ManyToMany (these use join tables)
            if (field.isAnnotationPresent(OneToMany.class) || field.isAnnotationPresent(ManyToMany.class)) {
                // These don't create columns in this table
                continue;
            }

            // Regular field - add column
            Class<?> type = field.getType();
            var converter = io.memris.spring.converter.TypeConverterRegistry.getInstance().getConverter(type);
            Class<?> storageType;

            if (converter != null) {
                storageType = converter.getStorageType();
            } else if (type.isEnum()) {
                storageType = String.class;
            } else if (type.isPrimitive()) {
                storageType = type;
            } else {
                throw new IllegalArgumentException("Unsupported field type: " + type +
                    " in entity " + entityClass.getName() + ", field: " + field.getName());
            }

            columns.add(new ColumnSpec(field.getName(), storageType));
        }

        return new FfmTable(entityClass.getSimpleName(), arena, columns);
    }

    /**
     * Get the storage type for the ID field of an entity class.
     */
    private Class<?> getIdStorageType(Class<?> entityClass) {
        // First, try to find ID field with annotations
        for (var field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class)) {
                Class<?> fieldType = field.getType();
                var converter = io.memris.spring.converter.TypeConverterRegistry.getInstance().getConverter(fieldType);
                if (converter != null) {
                    return converter.getStorageType();
                }
                return fieldType;
            }
        }

        // Legacy support: look for int "id" field
        for (var field : entityClass.getDeclaredFields()) {
            if (field.getName().equals("id")) {
                return field.getType();
            }
        }
        throw new IllegalArgumentException("No ID field found for entity: " + entityClass.getName());
    }

    /**
     * Get the element type of a collection field.
     */
    private Class<?> getCollectionElementType(java.lang.reflect.Field field) {
        Class<?> type = field.getType();
        if (type == java.util.Set.class || type == java.util.List.class) {
            java.lang.reflect.Type genericType = field.getGenericType();
            if (genericType instanceof java.lang.reflect.ParameterizedType pt) {
                java.lang.reflect.Type[] typeArgs = pt.getActualTypeArguments();
                if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                    return (Class<?>) typeArgs[0];
                }
            }
        }
        return null;
    }

    /**
     * Get the Arena used by this TableManager.
     *
     * @return the Arena
     */
    public Arena arena() {
        return arena;
    }
}
