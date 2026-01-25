package io.memris.spring;

import io.memris.kernel.Column;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.kernel.selection.SelectionVectorFactory;
import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.converter.TypeConverterRegistry;
import io.memris.storage.ffm.FfmTable;
import io.memris.storage.ffm.FfmTable.ColumnSpec;

import java.lang.foreign.Arena;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import jakarta.persistence.*;

@SuppressWarnings({"preview", "Since15"})
public final class MemrisRepositoryFactory implements AutoCloseable {
    private final Arena arena;
    private final TableManager tableManager;
    private final Map<String, IdGenerator<?>> customIdGenerators = new HashMap<>();
    private final Map<Class<?>, AtomicLong> numericIdCounters = new HashMap<>();

    // Cache MethodHandles for column.set(int, Object) calls (performance optimization)
    private final Map<Class<?>, MethodHandle> columnSetHandles = new ConcurrentHashMap<>();

    // Configuration settings
    private SortAlgorithm sortAlgorithm = SortAlgorithm.AUTO;
    private boolean enableParallelSorting = true;
    private int parallelSortThreshold = 1000;  // Use parallel sorting for results larger than this

    /**
     * Sorting algorithms for query results.
     */
    public enum SortAlgorithm {
        /** Automatically choose the best algorithm based on data size */
        AUTO,
        /** Bubble sort - O(n²) but very fast for small n (< 100) */
        BUBBLE,
        /** Java's optimized sort - O(n log n) */
        JAVA_SORT,
        /** Parallel merge sort - O(n log n) with multiple threads */
        PARALLEL_STREAM
    }

    /**
     * Set the sorting algorithm for query results.
     */
    public void setSortAlgorithm(SortAlgorithm algorithm) {
        this.sortAlgorithm = algorithm;
    }

    /**
     * Enable or disable parallel sorting for large result sets.
     */
    public void setParallelSortingEnabled(boolean enabled) {
        this.enableParallelSorting = enabled;
    }

    /**
     * Set the threshold for using parallel sorting.
     * Results larger than this size will use parallel sorting when enabled.
     */
    public void setParallelSortThreshold(int threshold) {
        this.parallelSortThreshold = threshold;
    }
    private final Map<Class<?>, Map<String, Object>> indexes = new HashMap<>();

    public MemrisRepositoryFactory() {
        this.arena = Arena.ofConfined();
        this.tableManager = new TableManager(arena);
    }

    /**
     * Register custom ID generator for testability or custom ID strategies.
     * @param name Generator name (referenced by @GeneratedValue.generator)
     * @param generator IdGenerator instance
     */
    public <T> void registerIdGenerator(String name, IdGenerator<T> generator) {
        customIdGenerators.put(name, generator);
    }

    //    METHOD removed on purpose, use createJPARepository() and fix the errors -> replace all repositories with the entity's repo.
//    @SuppressWarnings("unchecked")
//    public <T> MemrisRepository<T> createRepository(Class<T> entityClass) {
//        throw new UnsupportedOperationException("createRepository() not supported. Use createJPARepository() instead.");
//    }

    /**
     * Create repository with JPA query method support.
     * Entity class is inferred from the repository interface's generic type.
     *
     * @param repositoryInterface Repository interface extending MemrisRepository<T>
     * @return Repository instance created by RepositoryScaffolder
     */
    public <T, R extends MemrisRepository<T>> R createJPARepository(Class<R> repositoryInterface) {
        // Use System 3 scaffolder for zero-reflection query execution
        Class<T> entityClass = extractEntityClass(repositoryInterface);
        tableManager.ensureNestedTables(entityClass);
        FfmTable table = tableManager.getOrCreateTable(entityClass);
        tableManager.cacheEnumValues(entityClass);
        tableManager.createJoinTables(entityClass);

        io.memris.spring.scaffold.RepositoryScaffolder<T, R> scaffolder =
            new io.memris.spring.scaffold.RepositoryScaffolder<>(this);
        return scaffolder.createRepository(repositoryInterface, entityClass, table);
    }

    /**
     * Extract entity class T from repository interface extending MemrisRepository<T>
     */
    @SuppressWarnings("unchecked")
    private <T> Class<T> extractEntityClass(Class<? extends MemrisRepository<T>> repositoryInterface) {
        // Get the first generic interface that extends MemrisRepository
        for (Type iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType pt) {
                Type rawType = pt.getRawType();
                if (rawType instanceof Class<?> clazz && MemrisRepository.class.isAssignableFrom(clazz)) {
                    Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                        return (Class<T>) typeArgs[0];
                    }
                }
            }
        }
        throw new IllegalArgumentException("Cannot extract entity class from " + repositoryInterface.getName() + 
            ". Repository must extend MemrisRepository<EntityType>.");
    }

    /**
     * Find the ID field for an entity and return its storage type.
     * Supports @GeneratedValue, @jakarta.persistence.Id, and legacy "id" field detection.
     */
    private Class<?> getIdStorageType(Class<?> entityClass) {
        // First, try to find ID field with annotations
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class)) {
                Class<?> fieldType = field.getType();
                TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(fieldType);
                if (converter != null) {
                    return converter.getStorageType();
                }
                return fieldType;
            }
        }

        // Legacy support: look for int "id" field
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals("id")) {
                return field.getType();
            }
        }
        throw new IllegalArgumentException("No ID field found for entity: " + entityClass.getName());
    }

    /**
     * Find the ID field for an entity and return its Java field type.
     * Used for UUID detection (returns UUID.class, not the storage type).
     */
    private Class<?> getIdFieldType(Class<?> entityClass) {
        // First, try to find ID field with annotations
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class)) {
                return field.getType();
            }
        }

        // Legacy support: look for int "id" field
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals("id")) {
                return field.getType();
            }
        }
        throw new IllegalArgumentException("No ID field found for entity: " + entityClass.getName());
    }

    private Class<?> getCollectionElementType(Field field) {
        Class<?> type = field.getType();
        if (type == Set.class || type == List.class) {
            Type genericType = field.getGenericType();
            if (genericType instanceof ParameterizedType) {
                Type[] typeArgs = ((ParameterizedType) genericType).getActualTypeArguments();
                if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                    return (Class<?>) typeArgs[0];
                }
            }
        }
        return null;
    }

    private <T> FfmTable buildTable(Class<T> entityClass) {
        List<ColumnSpec> columns = new ArrayList<>();
        for (Field field : entityClass.getDeclaredFields()) {
            // Check if field is an ID field (with @GeneratedValue or @jakarta.persistence.Id)
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                Class<?> type = field.getType();
                if (type == UUID.class) {
                    columns.add(new ColumnSpec(field.getName() + "_msb", long.class));
                    columns.add(new ColumnSpec(field.getName() + "_lsb", long.class));
                } else {
                    // Use TypeConverterRegistry for all ID types (int, long, UUID, String, etc.)
                    TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(type);
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

            Class<?> type = field.getType();
            // Java 21 switch for blazing fast type dispatch
            if (isEntityType(type) ||
                field.isAnnotationPresent(OneToOne.class) ||
                field.isAnnotationPresent(ManyToOne.class)) {
                Class<?> idFieldType = getIdFieldType(type);
                if (idFieldType == UUID.class) {
                    columns.add(new ColumnSpec(field.getName() + "_msb", long.class));
                    columns.add(new ColumnSpec(field.getName() + "_lsb", long.class));
                } else {
                    Class<?> storageType = getIdStorageType(type);
                    columns.add(new ColumnSpec(field.getName() + "_id", storageType));
                }
                continue;
            } else if (type.isEnum() && field.isAnnotationPresent(Enumerated.class)) {
                EnumType enumType = field.getAnnotation(Enumerated.class).value();
                // Java 21 switch on enum type
                columns.add(new ColumnSpec(field.getName(), switch (enumType) {
                    case ORDINAL -> int.class;
                    default -> String.class;
                }));
            } else if (field.isAnnotationPresent(Transient.class)) {
                continue;
            } else if (field.isAnnotationPresent(OneToMany.class)) {
                continue;
            } else if (field.isAnnotationPresent(ManyToMany.class)) {
                continue;
            } else if (type.isAnnotationPresent(Embeddable.class)) {
                continue;
            } else if (type == UUID.class) {
                columns.add(new ColumnSpec(field.getName() + "_msb", long.class));
                columns.add(new ColumnSpec(field.getName() + "_lsb", long.class));
            } else if (type.isAnnotationPresent(Entity.class)) {
                Class<?> idFieldType = getIdFieldType(type);
                if (idFieldType == UUID.class) {
                    columns.add(new ColumnSpec(field.getName() + "_msb", long.class));
                    columns.add(new ColumnSpec(field.getName() + "_lsb", long.class));
                } else {
                    Class<?> storageType = getIdStorageType(type);
                    columns.add(new ColumnSpec(field.getName() + "_id", storageType));
                }
                continue;
            } else {
                // Use TypeConverterRegistry to determine storage type
                TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(type);
                if (converter == null) {
                    // Provide detailed diagnostics about why this field couldn't be processed
                    String errorMessage = String.format(
                        "Cannot store field '%s' of type %s. This could be because:%n" +
                        "  1. The field is not a supported primitive type%n" +
                        "  2. The field is not annotated with @Entity, @OneToOne, @ManyToOne%n" +
                        "  3. The field is an @Entity but the entity class has no @Id field%n" +
                        "  4. A TypeConverter is not registered for this custom type%n%n" +
                        "Field details:%n" +
                        "  - Name: %s%n" +
                        "  - Type: %s%n" +
                        "  - Is @Entity: %b%n" +
                        "  - Is @OneToOne: %b%n" +
                        "  - Is @ManyToOne: %b%n" +
                        "  - Is @OneToMany: %b%n" +
                        "  - Is @ManyToMany: %b%n%n" +
                        "If this is an entity relationship, ensure the target class has @Id field.%n" +
                        "If this is a custom type, register a TypeConverter.",
                        field.getName(), type.getName(), field.getName(), type.getName(),
                        type.isAnnotationPresent(Entity.class),
                        field.isAnnotationPresent(OneToOne.class),
                        field.isAnnotationPresent(ManyToOne.class),
                        field.isAnnotationPresent(OneToMany.class),
                        field.isAnnotationPresent(ManyToMany.class));

                    throw new IllegalArgumentException(errorMessage);
                }
                @SuppressWarnings("unchecked")
                Class<?> storageType = (Class<?>) converter.getStorageType();
                columns.add(new ColumnSpec(field.getName(), storageType));
            }
        }
        FfmTable table = new FfmTable(entityClass.getSimpleName(), arena, columns);

        // Build indexes for @Index annotated fields
        buildIndexes(entityClass, table);

        return table;
    }

    private <T> void buildIndexes(Class<T> entityClass, FfmTable table) {
        Map<String, Object> entityIndexes = new HashMap<>();

        // Always create a HashIndex on the ID field for O(1) lookups (findById, existsById, etc.)
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                // Create HashIndex for ID field - this is always built for performance
                entityIndexes.put(field.getName(), new HashIndex<>());
                break; // Only one ID field
            }
        }

        // Build indexes for @Index annotated fields
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Index.class)) {
                String fieldName = field.getName();
                Class<?> fieldType = field.getType();
                Index indexAnnotation = field.getAnnotation(Index.class);

                Object index;
                if (indexAnnotation.type() == Index.IndexType.BTREE) {
                    // RangeIndex for range queries (GT, LT, GTE, LTE, BETWEEN)
                    index = new RangeIndex<>();
                } else {
                    // HashIndex for equality lookups (EQ, NEQ)
                    // Also works for ranges since RangeIndex extends NavigableMap
                    index = new HashIndex<>();
                }
                entityIndexes.put(fieldName, index);
            }
        }
        if (!entityIndexes.isEmpty()) {
            indexes.put(entityClass, entityIndexes);
        }
    }

    private boolean isEntityType(Class<?> type) {
        return type.isAnnotationPresent(Entity.class) &&
               !type.isPrimitive() &&
               type != String.class;
    }

    // Java 21 switch with guarded patterns for blazing fast type dispatch
    private boolean isZero(Object value, Class<?> type) {
        if (value == null) return false;

        // Pattern matching on Class with guarded patterns - faster than string comparison
        return switch (type) {
            case Class<?> c when c == int.class || c == Integer.class -> (int) value == 0;
            case Class<?> c when c == long.class || c == Long.class -> (long) value == 0L;
            case Class<?> c when c == short.class || c == Short.class -> (short) value == (short) 0;
            case Class<?> c when c == byte.class || c == Byte.class -> (byte) value == (byte) 0;
            case Class<?> c when c == UUID.class -> {
                UUID uuid = (UUID) value;
                yield uuid.getMostSignificantBits() == 0L && uuid.getLeastSignificantBits() == 0L;
            }
            default -> false;
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T generateId(Class<?> entityClass, Field idField) {
        GeneratedValue annotation = idField.getAnnotation(GeneratedValue.class);
        GenerationType strategy = (annotation != null) ? annotation.strategy() : GenerationType.AUTO;
        Class<?> idType = idField.getType();

        // Java 21 switch for blazing fast strategy dispatch
        return switch (strategy) {
            case AUTO -> {
                // Auto-detect: numeric → IDENTITY, UUID → UUID
                GenerationType detectedStrategy = switch (idType) {
                    case Class<?> c when c == UUID.class -> GenerationType.UUID;
                    default -> GenerationType.IDENTITY;
                };
                yield generateIdWithStrategy(entityClass, idField, detectedStrategy, idType, annotation);
            }
            case IDENTITY -> generateNumericId(entityClass, idType);
            case UUID -> (T) UUID.randomUUID();
            case CUSTOM -> {
                String generatorName = annotation.generator();
                if (generatorName == null || generatorName.isEmpty()) {
                    throw new MemrisException(new IllegalArgumentException("CUSTOM strategy requires generator name in @GeneratedValue"));
                }
                IdGenerator<?> generator = customIdGenerators.get(generatorName);
                if (generator == null) {
                    throw new MemrisException(new IllegalArgumentException("No IdGenerator found for: " + generatorName));
                }
                yield (T) generator.generate();
            }
            default -> throw new MemrisException(new IllegalArgumentException("Unsupported generation strategy: " + strategy.name()));
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T generateIdWithStrategy(Class<?> entityClass, Field idField, GenerationType strategy, Class<?> idType, GeneratedValue annotation) {
        // Java 21 switch for strategy dispatch
        return switch (strategy) {
            case IDENTITY -> generateNumericId(entityClass, idType);
            case UUID -> (T) UUID.randomUUID();
            case CUSTOM -> {
                String generatorName = annotation.generator();
                IdGenerator<?> generator = customIdGenerators.get(generatorName);
                if (generator == null) {
                    throw new MemrisException(new IllegalArgumentException("No IdGenerator found for: " + generatorName));
                }
                yield (T) generator.generate();
            }
            default -> throw new MemrisException(new IllegalArgumentException("Unsupported generation strategy: " + strategy.name()));
        };
    }

    @SuppressWarnings("unchecked")
    private <T> T generateNumericId(Class<?> entityClass, Class<?> idType) {
        // Use per-entity-class atomics for lock-free ID generation
        AtomicLong counter = numericIdCounters.computeIfAbsent(
            entityClass,
            k -> new AtomicLong(1)
        );

        long nextId = counter.getAndIncrement();

        // Java 21 switch with guarded patterns - blazing fast type dispatch
        return switch (idType) {
            case Class<?> c when c == int.class || c == Integer.class -> (T) Integer.valueOf((int) nextId);
            case Class<?> c when c == long.class || c == Long.class -> (T) Long.valueOf(nextId);
            case Class<?> c when c == short.class || c == Short.class -> (T) Short.valueOf((short) nextId);
            case Class<?> c when c == byte.class || c == Byte.class -> (T) Byte.valueOf((byte) nextId);
            default -> throw new MemrisException(new IllegalArgumentException("Unsupported ID type for IDENTITY strategy: " + idType));
        };
    }

    FfmTable getTable(Class<?> entityClass) {
        return tableManager.getTable(entityClass);
    }

    /**
     * Get index for a specific field, or null if no index exists.
     */
    Object getIndex(Class<?> entityClass, String fieldName) {
        Map<String, Object> entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null) {
            return null;
        }
        return entityIndexes.get(fieldName);
    }

    /**
     * Check if an index exists for a field.
     */
    boolean hasIndex(Class<?> entityClass, String fieldName) {
        Map<String, Object> entityIndexes = indexes.get(entityClass);
        return entityIndexes != null && entityIndexes.containsKey(fieldName);
    }

    /**
     * Query using index - returns row IDs for matching entities.
     * @param entityClass The entity class
     * @param fieldName The indexed field name
     * @param operator The comparison operator
     * @param value The value to match
     * @return Array of matching row indices, or null if no index available
     */
    int[] queryIndex(Class<?> entityClass, String fieldName, Predicate.Operator operator, Object value) {
        Map<String, Object> entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null) {
            return null;
        }

        Object index = entityIndexes.get(fieldName);
        if (index == null) {
            return null;
        }

        // Java 21+ pattern matching with switch on sealed types
        return switch (index) {
            case HashIndex hashIndex when value != null -> rowIdSetToIntArray(hashIndex.lookup(value));
            case RangeIndex rangeIndex when value instanceof Comparable comp -> switch (operator) {
                case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(comp));
                case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(comp));
                case LT -> rowIdSetToIntArray(rangeIndex.lessThan(comp));
                case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(comp));
                default -> null;
            };
            default -> null;
        };
    }

    private static int[] rowIdSetToIntArray(RowIdSet rowIdSet) {
        long[] longArray = rowIdSet.toLongArray();
        int[] intArray = new int[longArray.length];
        for (int i = 0; i < longArray.length; i++) {
            intArray[i] = (int) longArray[i];  // RowId value IS the row index
        }
        return intArray;
    }

    public void close() {
        arena.close();
    }

    /**
     * Save any object (for nested entity references and collection items).
     * Returns the ID of the saved entity as Object (supports int, long, UUID, String, etc.).
     */
    @SuppressWarnings("unchecked")
    Object doSave(Object entity, Class<?> entityClass) {
        invokePrePersist(entity, entityClass);

        Field idField = null;
        Object explicitId = null;

        // Find ID field using JPA annotations (@GeneratedValue or @jakarta.persistence.Id)
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class)) {
                idField = field;
                idField.setAccessible(true);
                try {
                    explicitId = field.get(entity);
                } catch (IllegalAccessException e) {
                    throw new MemrisException(e);
                }
                break;
            }
        }

        if (idField == null) {
            for (Field field : entityClass.getDeclaredFields()) {
                if (field.getName().equals("id")) {
                    idField = field;
                    idField.setAccessible(true);
                    try {
                        explicitId = field.get(entity);
                    } catch (IllegalAccessException e) {
                        throw new MemrisException(e);
                    }
                    break;
                }
            }
        }

        // Determine if this is an update or insert
        boolean isUpdate = false;
        int existingRowIndex = -1;
        Object id = explicitId;

        // If explicit ID is set, check if row exists
        if (idField != null && explicitId != null && !isZero(explicitId, idField.getType())) {
            existingRowIndex = findRowIndexById(entityClass, idField, explicitId);
            isUpdate = (existingRowIndex != -1);
        }

        // Generate ID if not explicitly set or is zero
        if (idField != null && !isUpdate && (explicitId == null || isZero(explicitId, idField.getType()))) {
            id = generateId(entityClass, idField);
            // Only update the entity's ID field if we generated a new ID
            // (explicit IDs should already be set in the entity)
        }

        // If update, just update the index, skip full row update
        if (isUpdate) {
            updateExistingRow(entity, (Class<Object>) entityClass, idField, existingRowIndex);
            return id;
        }

        // Insert new entity
        List<Object> values = new ArrayList<>();
        Map<String, Object> collectionItems = new HashMap<>();
        String entityName = entityClass.getSimpleName();

        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Transient.class)) {
                continue;
            }
            field.setAccessible(true);
            try {
                // Compare by name since getDeclaredFields() returns new Field objects
                if (idField != null && field.getName().equals(idField.getName())) {
                    // Use generated or explicit ID - convert using TypeConverter for proper storage type
                    if (idField.getType() == UUID.class) {
                        UUID uuid = (UUID) id;
                        if (uuid == null) {
                            values.add(0L);
                            values.add(0L);
                        } else {
                            values.add(uuid.getMostSignificantBits());
                            values.add(uuid.getLeastSignificantBits());
                        }
                    } else {
                        Object idStorageValue = id;
                        TypeConverter<?, ?> idConverter = TypeConverterRegistry.getInstance().getConverter(idField.getType());
                        if (idConverter != null) {
                            idStorageValue = ((TypeConverter<Object, Object>) idConverter).toStorage(id);
                        }
                        if (idStorageValue == null && idConverter != null) {
                            idStorageValue = defaultStorageValue(idConverter.getStorageType());
                        }
                        values.add(idStorageValue);
                    }
                    // Update the entity's ID field only if we generated a new ID
                    // (explicit IDs should already be set in the entity)
                    if (idField != null && !isUpdate && (explicitId == null || isZero(explicitId, idField.getType()))) {
                        field.set(entity, id);
                    }
                } else if (field.isAnnotationPresent(OneToMany.class)) {
                    Object collection = field.get(entity);
                    if (collection != null) {
                        collectionItems.put(field.getName(), collection);
                    }
                    continue;
                } else if (field.isAnnotationPresent(ManyToMany.class)) {
                    Object collection = field.get(entity);
                    if (collection != null) {
                        collectionItems.put(field.getName(), collection);
                    }
                    continue;
                } else if (field.getType().isAnnotationPresent(Embeddable.class)) {
                    continue;
                } else if (field.isAnnotationPresent(OneToOne.class)) {
                    Object child = field.get(entity);
                    if (child != null) {
                        Object childId = doSave(child, child.getClass());
                        if (childId instanceof UUID uuid) {
                            values.add(uuid.getMostSignificantBits());
                            values.add(uuid.getLeastSignificantBits());
                        } else {
                            values.add(childId);
                        }
                    }
                    // Null child: skip this column (foreign key remains null)
                    continue;
                } else if (field.isAnnotationPresent(ManyToOne.class)) {
                    Object child = field.get(entity);
                    if (child != null) {
                        Object childId = doSave(child, child.getClass());
                        if (childId instanceof UUID uuid) {
                            values.add(uuid.getMostSignificantBits());
                            values.add(uuid.getLeastSignificantBits());
                        } else {
                            values.add(childId);
                        }
                    }
                    // Null child: skip this column (foreign key remains null)
                    continue;
                } else if (isEntityType(field.getType())) {
                    Object child = field.get(entity);
                    if (child != null) {
                        Object childId = doSave(child, child.getClass());
                        if (childId instanceof UUID uuid) {
                            values.add(uuid.getMostSignificantBits());
                            values.add(uuid.getLeastSignificantBits());
                        } else {
                            values.add(childId);
                        }
                    }
                    // Null child: skip this column (foreign key remains null)
                    continue;
                } else if (field.getType().isEnum() && field.isAnnotationPresent(Enumerated.class)) {
                    EnumType enumType = field.getAnnotation(Enumerated.class).value();
                    Object enumValue = field.get(entity);
                    if (enumValue == null) {
                        if (enumType == EnumType.ORDINAL) {
                            values.add(-1);
                        } else {
                            values.add("");
                        }
                    } else if (enumType == EnumType.ORDINAL) {
                        values.add(((Enum<?>) enumValue).ordinal());
                    } else {
                        values.add(enumValue.toString());
                    }
                } else if (field.getType() == UUID.class) {
                    UUID uuid = (UUID) field.get(entity);
                    if (uuid == null) {
                        values.add(0L);
                        values.add(0L);
                    } else {
                        values.add(uuid.getMostSignificantBits());
                        values.add(uuid.getLeastSignificantBits());
                    }
                } else {
                    // Use TypeConverter for all other types
                    Object fieldValue = field.get(entity);
                    Class<?> fieldType = field.getType();
                    TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(fieldType);
                    if (converter != null) {
                        Object storageValue = ((TypeConverter<Object, Object>) converter).toStorage(fieldValue);
                        if (storageValue == null) {
                            storageValue = defaultStorageValue(converter.getStorageType());
                        }
                        values.add(storageValue);
                    } else {
                        // Direct set for unknown types
                        if (fieldValue == null && fieldType == String.class) {
                            values.add("");
                        } else {
                            values.add(fieldValue);
                        }
                    }
                }
            } catch (IllegalAccessException e) {
                throw new MemrisException(e);
            }
        }

        // Insert the new row
        tableManager.getTable(entityClass).insert(values.toArray());

        // Compute row index after operation
        int rowIndex = (int) tableManager.getTable(entityClass).rowCount() - 1;
        RowId rid = new RowId(rowIndex >>> 16, rowIndex & 0xFFFF);

        // Update indexes for indexed fields
        updateIndexes(entity, (Class<Object>) entityClass, rid);

        // Handle collections after the row is inserted
        for (String fieldName : collectionItems.keySet()) {
            Object collection = collectionItems.get(fieldName);
            Class<?> itemClass = null;
            String targetName = null;
            boolean isManyToMany = false;

            for (Field f : entityClass.getDeclaredFields()) {
                if (f.getName().equals(fieldName)) {
                    if (f.isAnnotationPresent(OneToMany.class)) {
                        itemClass = getCollectionElementType(f);
                        targetName = itemClass != null ? itemClass.getSimpleName() : null;
                    } else if (f.isAnnotationPresent(ManyToMany.class)) {
                        itemClass = getCollectionElementType(f);
                        targetName = itemClass != null ? itemClass.getSimpleName() : null;
                        isManyToMany = true;
                    }
                    break;
                }
            }

            if (collection instanceof Iterable && itemClass != null) {
                for (Object item : (Iterable<?>) collection) {
                    if (isManyToMany) {
                        Object childId = doSave(item, itemClass);
                        String joinTableName = entityName + "_" + targetName + "_join";
                        FfmTable joinTable = tableManager.getJoinTable(joinTableName);
                        if (joinTable != null) {
                            // Insert into join table with proper ID type handling
                            insertIntoJoinTable(joinTable, entityClass, id, itemClass, childId);
                        }
                    } else {
                        doSave(item, itemClass);
                    }
                }
            }
        }

        return id;
    }

    Object doSaveAll(Class<?> entityClass, Iterable<?> entities) {
        List<Object> results = new ArrayList<>();
        for (Object entity : entities) {
            results.add(doSave(entity, entityClass));
        }
        return results;
    }

    /**
     * Insert entity ID pair into join table, handling different ID types.
     * For UUID IDs, splits into most/least significant bits (2 long columns).
     * For other types, inserts directly.
     */
    @SuppressWarnings("unchecked")
    private void insertIntoJoinTable(FfmTable joinTable, Class<?> entityClass, Object entityId, Class<?> itemClass, Object itemId) {
        List<Object> values = new ArrayList<>();

        // Handle entity ID - Use pattern matching switch for O(1) dispatch
        switch (entityId) {
            case UUID uuid -> {
                values.add(uuid.getMostSignificantBits());
                values.add(uuid.getLeastSignificantBits());
            }
            default -> values.add(entityId);
        }

        // Handle item ID - Use pattern matching switch for O(1) dispatch
        switch (itemId) {
            case UUID uuid -> {
                values.add(uuid.getMostSignificantBits());
                values.add(uuid.getLeastSignificantBits());
            }
            default -> values.add(itemId);
        }

        joinTable.insert(values.toArray());
    }

    private <T> void updateIndexes(T entity, Class<T> entityClass, RowId rowId) {
        Map<String, Object> entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null || entityIndexes.isEmpty()) {
            return;
        }

        for (Field field : entityClass.getDeclaredFields()) {
            Object index = entityIndexes.get(field.getName());
            if (index == null) {
                continue;
            }
            field.setAccessible(true);
            try {
                Object value = field.get(entity);
                if (value == null) {
                    continue;
                }
                // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
                switch (index) {
                    case HashIndex hashIndex -> hashIndex.add(value, rowId);
                    case RangeIndex rangeIndex when value instanceof Comparable comp -> rangeIndex.add(comp, rowId);
                    default -> {}
                }
            } catch (IllegalAccessException e) {
                throw new MemrisException(e);
            }
        }
    }

    void doUpdate(Object entity, Class<?> entityClass) {
        invokePreUpdate(entity, entityClass);
    }

    private String toCamelCase(String s) {
        if (s == null || s.isEmpty()) return s;
        return s.substring(0, 1).toLowerCase() + s.substring(1);
    }

    private int findRowIndexById(Class<?> entityClass, Field idField, Object idValue) {
        FfmTable table = tableManager.getTable(entityClass);
        String idColumnName = idField.getName();

        // Use hash index if available
        @SuppressWarnings("unchecked")
        HashIndex<Object> hashIndex = (HashIndex<Object>) getIndex(entityClass, idColumnName);
        if (hashIndex != null) {
            RowIdSet rowIdSet = hashIndex.lookup(idValue);
            if (rowIdSet != null && rowIdSet.size() > 0) {
                long[] longArray = rowIdSet.toLongArray();
                if (longArray.length > 0) {
                    return (int) longArray[0];  // RowId stores row index
                }
            }
        }

        // Fallback: linear scan for ID (slow path, should rarely happen)
        int rowCount = (int) table.rowCount();
        for (int i = 0; i < rowCount; i++) {
            Object existingId = getFieldValueFromTable(table, idColumnName, i, idField.getType());
            if (existingId != null && existingId.equals(idValue)) {
                return i;
            }
        }
        return -1;  // Not found
    }

    private <T> void updateExistingRow(T entity, Class<T> entityClass, Field idField, int rowIndex) {
        FfmTable table = tableManager.getTable(entityClass);

        // Invoke @PreUpdate
        invokePreUpdate(entity, entityClass);

        // Update each field value in the row using optimized type dispatch
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Transient.class) ||
                field.isAnnotationPresent(OneToMany.class) ||
                field.isAnnotationPresent(ManyToMany.class) ||
                field.isAnnotationPresent(OneToOne.class) ||
                field.isAnnotationPresent(ManyToOne.class) ||
                isEntityType(field.getType())) {
                continue;
            }

            // Skip ID field - it doesn't change during update
            if (field.getName().equals(idField.getName())) {
                continue;
            }

            field.setAccessible(true);
            try {
                Object fieldValue = field.get(entity);
                if (fieldValue == null) {
                    continue;
                }

                // Convert field value to storage type
                Class<?> fieldType = field.getType();
                TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(fieldType);
                Object storageValue;

                if (converter != null) {
                    storageValue = ((TypeConverter<Object, Object>) converter).toStorage(fieldValue);
                } else {
                    storageValue = fieldValue;
                }

                if (fieldType == UUID.class) {
                    UUID uuid = (UUID) fieldValue;
                    long msb = uuid == null ? 0L : uuid.getMostSignificantBits();
                    long lsb = uuid == null ? 0L : uuid.getLeastSignificantBits();
                    setTableValue(table, field.getName() + "_msb", msb, rowIndex);
                    setTableValue(table, field.getName() + "_lsb", lsb, rowIndex);
                } else {
                    // Use table-level type-specific set methods (no reflection!)
                    // This is blazing fast compared to Method.invoke()
                    setTableValue(table, field.getName(), storageValue, rowIndex);
                }
            } catch (Exception e) {
                throw new MemrisException(e);
            }
        }

        // Update indexes for indexed fields
        RowId rid = new RowId(rowIndex >>> 16, rowIndex & 0xFFFF);
        updateIndexes(entity, entityClass, rid);
    }

    /**
     * Set a value in the table using MethodHandle (faster than Method.invoke).
     * Performance-optimized: caches MethodHandles for each column type.
     */
    private void setTableValue(FfmTable table, String columnName, Object value, int rowIndex) {
        try {
            Column<?> column = table.column(columnName);
            MethodHandle setHandle = columnSetHandles.computeIfAbsent(column.getClass(), colClass -> {
                try {
                    // Use privateLookupIn to access private inner classes of FfmTable
                    MethodHandles.Lookup lookup = MethodHandles.privateLookupIn(colClass, MethodHandles.lookup());
                    MethodType methodType = MethodType.methodType(void.class, int.class, Object.class);
                    return lookup.findVirtual(colClass, "set", methodType);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to get MethodHandle for Column.set()", e);
                }
            });
            setHandle.invoke(column, rowIndex, value);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set column value", e);
        }
    }

    @SuppressWarnings("unchecked")
    private int findNestedId(Object nestedEntity, String idFieldName) {
        // Find ID field using annotations, not by name/type
        for (Field field : nestedEntity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class)) {
                try {
                    field.setAccessible(true);
                    Object idValue = field.get(nestedEntity);
                    // Java 21 switch for fast type dispatch - extract int value
                    return switch (idValue) {
                        case Integer i -> i;
                        case Long l -> (int) (long) l;
                        case null -> -1;
                        default -> throw new MemrisException(new IllegalArgumentException("Nested entity ID must be int or long"));
                    };
                } catch (IllegalAccessException e) {
                    throw new MemrisException(e);
                }
            }
        }
        throw new MemrisException(new IllegalArgumentException("Nested entity has no ID field with @GeneratedValue or @Id"));
    }

    private Object getFieldValueFromTable(FfmTable table, String columnName, int rowIndex, Class<?> type) {
        if (type == UUID.class) {
            Column<?> msbColumn = table.column(columnName + "_msb");
            Column<?> lsbColumn = table.column(columnName + "_lsb");
            if (msbColumn != null && lsbColumn != null) {
                long msb = table.getLong(columnName + "_msb", rowIndex);
                long lsb = table.getLong(columnName + "_lsb", rowIndex);
                if (msb == 0L && lsb == 0L) {
                    return null;
                }
                return new UUID(msb, lsb);
            }
        }

        // TODO convert to switch
        if (type == int.class || type == Integer.class) {
            return table.getInt(columnName, rowIndex);
        } else if (type == long.class || type == Long.class) {
            return table.getLong(columnName, rowIndex);
        } else if (type == boolean.class || type == Boolean.class) {
            return table.getBoolean(columnName, rowIndex);
        } else if (type == byte.class || type == Byte.class) {
            return table.getByte(columnName, rowIndex);
        } else if (type == short.class || type == Short.class) {
            return table.getShort(columnName, rowIndex);
        } else if (type == float.class || type == Float.class) {
            return table.getFloat(columnName, rowIndex);
        } else if (type == double.class || type == Double.class) {
            return table.getDouble(columnName, rowIndex);
        } else if (type == char.class || type == Character.class) {
            return table.getChar(columnName, rowIndex);
        } else if (type == String.class) {
            return table.getString(columnName, rowIndex);
        } else if (type == UUID.class) {
            return UUID.fromString(table.getString(columnName, rowIndex));
        }
        throw new MemrisException(new IllegalArgumentException("Unsupported type for getFieldValue: " + type));
    }

    private void invokePrePersist(Object entity, Class<?> entityClass) {
        for (Method method : entityClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(PrePersist.class)) {
                method.setAccessible(true);
                try {
                    method.invoke(entity);
                } catch (Exception e) {
                    throw new MemrisException(e);
                }
            }
        }
    }

    // TODO not used, this hast to be called on post load lifecycle
    void invokePostLoad(Object entity, Class<?> entityClass) {
        for (Method method : entityClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(PostLoad.class)) {
                method.setAccessible(true);
                try {
                    method.invoke(entity);
                } catch (Exception e) {
                    throw new MemrisException(e);
                }
            }
        }
    }

    void invokePreUpdate(Object entity, Class<?> entityClass) {
        for (Method method : entityClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(PreUpdate.class)) {
                method.setAccessible(true);
                try {
                    method.invoke(entity);
                } catch (Exception e) {
                    throw new MemrisException(e);
                }
            }
        }
    }

    // NOTE: join() method disabled - MemrisRepository is now marker interface
    // with no getEntityClass() method. This functionality will need to be
    // re-implemented differently or moved to a different location.
    /*
    public <L, R> List<JoinResult<L, R>> join(
            MemrisRepository<L> leftRepo,
            String leftKey,
            MemrisRepository<R> rightRepo,
            String rightKey) {
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();

        FfmTable leftTable = tableManager.getTable(leftRepo.getEntityClass());
        FfmTable rightTable = tableManager.getTable(rightRepo.getEntityClass());

        int[] leftIndices = leftTable.scanAll(factory).toIntArray();
        int[] rightIndices = rightTable.scanAll(factory).toIntArray();

        HashMap<Object, int[]> leftHash = new HashMap<>();
        // Index-based loop for O(1) iteration (no iterator creation)
        for (int i = 0; i < leftIndices.length; i++) {
            int idx = leftIndices[i];
            Object key = getKeyValue(leftTable, leftRepo.getEntityClass(), leftKey, idx);
            leftHash.computeIfAbsent(key, k -> new int[0]);
            int[] existing = leftHash.get(key);
            int[] updated = new int[existing.length + 1];
            System.arraycopy(existing, 0, updated, 0, existing.length);
            updated[existing.length] = idx;
            leftHash.put(key, updated);
        }

        List<JoinResult<L, R>> results = new ArrayList<>();
        // Index-based loop for O(1) iteration (no iterator creation)
        for (int i = 0; i < rightIndices.length; i++) {
            int idx = rightIndices[i];
            Object key = getKeyValue(rightTable, rightRepo.getEntityClass(), rightKey, idx);
            int[] leftMatches = leftHash.get(key);
            if (leftMatches != null) {
                // Index-based loop for O(1) iteration (no iterator creation)
                for (int j = 0; j < leftMatches.length; j++) {
                    int leftIdx = leftMatches[j];
                    results.add(new JoinResult<>(
                            materializeSingle(leftTable, leftRepo.getEntityClass(), leftIdx),
                            materializeSingle(rightTable, rightRepo.getEntityClass(), idx)
                    ));
                }
            }
        }
        return results;
    }

    private <T> T materializeSingle(FfmTable table, Class<T> entityClass, int rowIdx) {
        try {
            T instance = entityClass.getDeclaredConstructor().newInstance();
            for (var col : table.columns()) {
                java.lang.reflect.Field field = entityClass.getDeclaredField(col.name());
                field.setAccessible(true);
                // Use pattern matching switch for O(1) dispatch instead of O(n) string comparison
                Object value = switch (col.type()) {
                    case Class<?> c when c == int.class || c == Integer.class -> table.getInt(col.name(), rowIdx);
                    case Class<?> c when c == long.class || c == Long.class -> table.getLong(col.name(), rowIdx);
                    case Class<?> c when c == String.class -> table.getString(col.name(), rowIdx);
                    default -> throw new IllegalArgumentException("Unsupported type: " + col.type());
                };
                // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
                switch (value) {
                    case String str when str.isEmpty() && field.getType() == String.class -> field.set(instance, null);
                    default -> {
                        if (field.getType().isEnum() && field.isAnnotationPresent(Enumerated.class)) {
                            EnumType enumType = field.getAnnotation(Enumerated.class).value();
                            Class<?> enumClass = field.getType();
                            Object enumValue;
                            if (enumType == EnumType.ORDINAL) {
                                int ordinal = (Integer) value;
                                if (ordinal < 0) {
                                    field.set(instance, null);
                                    continue;
                                }
                                enumValue = enumClass.getEnumConstants()[ordinal];
                            } else {
                                String strValue = (String) value;
                                if (strValue.isEmpty()) {
                                    field.set(instance, null);
                                    continue;
                                }
                                enumValue = Enum.valueOf((Class<Enum>) enumClass, strValue);
                            }
                            field.set(instance, enumValue);
                        } else {
                            field.set(instance, value);
                        }
                    }
                }
            }
            invokePostLoad(instance, entityClass);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    */

    private Object getKeyValue(FfmTable table, Class<?> entityClass, String property, int rowIdx) {
        // Check for UUID join table columns (stored as 2 longs: property_msb and property_lsb)
        boolean hasMsb = false, hasLsb = false;
        for (var col : table.columns()) {
            if (col.name().equals(property + "_msb")) hasMsb = true;
            if (col.name().equals(property + "_lsb")) hasLsb = true;
        }

        // If UUID columns exist, reconstruct UUID from 2 longs
        if (hasMsb && hasLsb) {
            long msb = table.getLong(property + "_msb", rowIdx);
            long lsb = table.getLong(property + "_lsb", rowIdx);
            // Only reconstruct UUID if at least one bit is set (avoid null UUID)
            if (msb != 0 || lsb != 0) {
                return new UUID(msb, lsb);
            }
            return null;
        }

        // Standard column lookup for non-UUID types
        for (var col : table.columns()) {
            if (col.name().equals(property)) {
                // Use pattern matching switch for O(1) dispatch instead of O(n) string comparison
                return switch (col.type()) {
                    case Class<?> c when c == int.class || c == Integer.class -> table.getInt(col.name(), rowIdx);
                    case Class<?> c when c == long.class || c == Long.class -> table.getLong(col.name(), rowIdx);
                    case Class<?> c when c == String.class -> table.getString(col.name(), rowIdx);
                    default -> throw new IllegalArgumentException("Unsupported join key type: " + col.type());
                };
            }
        }
        throw new IllegalArgumentException("Property not found: " + property);
    }

        // ========== Helper methods for repository implementation ==========

    public <T> List<T> doFindAllById(Class<T> entityClass, Iterable<?> ids) {
        String idColumnName = findIdColumnName(entityClass);
        if (idColumnName == null) {
            return List.of();
        }
        FfmTable table = tableManager.getTable(entityClass);
        List<T> results = new ArrayList<>();
        for (Object id : ids) {
            int[] matchingRows = queryIndex(entityClass, idColumnName, Predicate.Operator.EQ, id);
            if (matchingRows != null && matchingRows.length > 0) {
                results.add(materializeRow(entityClass, table, matchingRows[0]));
            }
        }
        return results;
    }

    public <T> void doDeleteAllById(Class<T> entityClass, Iterable<?> ids) {
        String idColumnName = findIdColumnName(entityClass);
        if (idColumnName == null) {
            return;
        }
        for (Object id : ids) {
            doDeleteById(entityClass, idColumnName, id);
        }
    }

    public <T> void doDeleteAll(Class<T> entityClass, Iterable<?> entities) {
        Field idField = resolveIdField(entityClass);
        if (idField == null) {
            return;
        }
        idField.setAccessible(true);
        for (Object entity : entities) {
            try {
                Object idValue = idField.get(entity);
                doDeleteById(entityClass, idField.getName(), idValue);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public <T> void doDelete(Class<T> entityClass, Object entity) {
        if (entity == null) {
            return;
        }
        Field idField = resolveIdField(entityClass);
        if (idField == null) {
            return;
        }
        idField.setAccessible(true);
        try {
            Object idValue = idField.get(entity);
            doDeleteById(entityClass, idField.getName(), idValue);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> Object doGenericFindBy(Class<T> entityClass, String columnName, Object value, Class<?> returnType) {
        FfmTable table = tableManager.getTable(entityClass);
        int[] matchingRows = queryIndex(entityClass, columnName, Predicate.Operator.EQ, value);
        if (matchingRows == null) {
            matchingRows = scanTable(table, columnName, Predicate.Operator.EQ, value);
        }
        if (matchingRows == null || matchingRows.length == 0) {
            return emptyResult(returnType, entityClass);
        }
        List<T> results = new ArrayList<>(matchingRows.length);
        // Index-based loop for O(1) iteration (no iterator creation)
        for (int i = 0; i < matchingRows.length; i++) {
            results.add(materializeRow(entityClass, table, matchingRows[i]));
        }
        return adaptFindResults(results, returnType, entityClass);
    }

    public <T> Object doFindByIn(Class<T> entityClass, String columnName, Iterable<?> values, Class<?> returnType) {
        FfmTable table = tableManager.getTable(entityClass);
        List<T> results = new ArrayList<>();
        for (Object value : values) {
            int[] matchingRows = queryIndex(entityClass, columnName, Predicate.Operator.EQ, value);
            if (matchingRows == null) {
                matchingRows = scanTable(table, columnName, Predicate.Operator.EQ, value);
            }
            if (matchingRows != null) {
                // Index-based loop for O(1) iteration (no iterator creation)
                for (int j = 0; j < matchingRows.length; j++) {
                    results.add(materializeRow(entityClass, table, matchingRows[j]));
                }
            }
        }
        if (results.isEmpty()) {
            return emptyResult(returnType, entityClass);
        }
        return adaptFindResults(results, returnType, entityClass);
    }

    public <T> Object doFindByBetween(Class<T> entityClass, String columnName, Object start, Object end, Class<?> returnType) {
        FfmTable table = tableManager.getTable(entityClass);
        Predicate predicate = new Predicate.Between(columnName, start, end);
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
        int[] matchingRows = table.scan(predicate, factory).toIntArray();
        if (matchingRows == null || matchingRows.length == 0) {
            return emptyResult(returnType, entityClass);
        }
        List<T> results = new ArrayList<>(matchingRows.length);
        // Index-based loop for O(1) iteration (no iterator creation)
        for (int i = 0; i < matchingRows.length; i++) {
            results.add(materializeRow(entityClass, table, matchingRows[i]));
        }
        return adaptFindResults(results, returnType, entityClass);
    }

    public <T> Object doFindById(Class<T> entityClass, String idColumnName, Object id) {
        int[] matchingRows = queryIndex(entityClass, idColumnName, Predicate.Operator.EQ, id);

        if (matchingRows == null || matchingRows.length == 0) {
            return Optional.empty();
        }

        int row = matchingRows[0];
        FfmTable table = tableManager.getTable(entityClass);
        return Optional.of(materializeRow(entityClass, table, row));
    }

    public <T> boolean doExistsById(Class<T> entityClass, String idColumnName, Object id) {
        int[] matchingRows = queryIndex(entityClass, idColumnName, Predicate.Operator.EQ, id);
        return matchingRows != null && matchingRows.length > 0;
    }

    public <T> void doDeleteById(Class<T> entityClass, String idColumnName, Object id) {
        int[] matchingRows = queryIndex(entityClass, idColumnName, Predicate.Operator.EQ, id);

        if (matchingRows == null || matchingRows.length == 0) {
            return;
        }

        Object index = getIndex(entityClass, idColumnName);
        // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
        switch (index) {
            case HashIndex<?> hashIndex -> {
                @SuppressWarnings("unchecked")
                HashIndex<Object> rawIndex = (HashIndex<Object>) hashIndex;
                rawIndex.removeAll(id);
            }
            case null, default -> {}
        }
    }

    public <T> List<T> doFindAll(Class<T> entityClass) {
        FfmTable table = tableManager.getTable(entityClass);
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
        int[] rows = table.scanAll(factory).toIntArray();

        List<T> results = new ArrayList<>(rows.length);
        for (int row : rows) {
            results.add(materializeRow(entityClass, table, row));
        }

        return results;
    }

    public <T> long doCount(Class<T> entityClass) {
        String idColumnName = findIdColumnName(entityClass);
        if (idColumnName != null) {
            Object index = getIndex(entityClass, idColumnName);
            if (index instanceof HashIndex<?> hashIndex) {
                return hashIndex.size();
            }
        }
        FfmTable table = tableManager.getTable(entityClass);
        return table.rowCount();
    }

    public <T> void doDeleteAll(Class<T> entityClass) {
        String idColumnName = findIdColumnName(entityClass);
        if (idColumnName != null) {
            Object index = getIndex(entityClass, idColumnName);
            // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
            switch (index) {
                case HashIndex<?> hashIndex -> {
                    @SuppressWarnings("unchecked")
                    HashIndex<Object> rawIndex = (HashIndex<Object>) hashIndex;
                    rawIndex.clear();
                }
                case null,default -> {}
            }
        }
    }

    public <T> Object doFindBy(Class<T> entityClass, String methodName, Object[] args, Class<?> returnType) {
        // Parse method name
        String columnName = parseColumnName(methodName);
        Predicate.Operator operator = parseOperator(methodName);

        if (args.length == 0) {
            if (methodName.startsWith("countBy")) {
                return 0L;
            }
            return emptyResult(returnType, entityClass);
        }

        Object paramValue = args[0];
        FfmTable table = tableManager.getTable(entityClass);

        // Try index first
        int[] matchingRows = queryIndex(entityClass, columnName, operator, paramValue);

        // Fall back to scan
        if (matchingRows == null) {
            matchingRows = scanTable(table, columnName, operator, paramValue);
        }

        if (matchingRows == null || matchingRows.length == 0) {
            if (methodName.startsWith("countBy")) {
                return 0L;
            }
            return emptyResult(returnType, entityClass);
        }

        // Materialize
        List<T> results = new ArrayList<>(matchingRows.length);
        for (int row : matchingRows) {
            results.add(materializeRow(entityClass, table, row));
        }

        if (methodName.startsWith("countBy")) {
            return (long) results.size();
        }

        return adaptFindResults(results, returnType, entityClass);
    }

    private <T> T materializeRow(Class<T> entityClass, FfmTable table, int row) {
        try {
            T instance = entityClass.getDeclaredConstructor().newInstance();
            for (Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(Transient.class) ||
                    field.isAnnotationPresent(OneToMany.class) ||
                    field.isAnnotationPresent(ManyToMany.class)) {
                    continue;
                }
                if (field.isAnnotationPresent(OneToOne.class) ||
                    field.isAnnotationPresent(ManyToOne.class) ||
                    isEntityType(field.getType()) ||
                    field.getType().isAnnotationPresent(Embeddable.class)) {
                    continue;
                }
                field.setAccessible(true);
                String columnName = field.getName();
                if (columnName.endsWith("_id")) {
                    continue;
                }
                Object value = getFromTable(table, columnName, row, field.getType());
                if (field.getType().isEnum() && field.isAnnotationPresent(Enumerated.class)) {
                    EnumType enumType = field.getAnnotation(Enumerated.class).value();
                    if (value == null || value.toString().isEmpty()) {
                        field.set(instance, null);
                    } else if (enumType == EnumType.ORDINAL) {
                        int ordinal = ((Number) value).intValue();
                        Object[] constants = field.getType().getEnumConstants();
                        field.set(instance, ordinal >= 0 && ordinal < constants.length ? constants[ordinal] : null);
                    } else {
                        @SuppressWarnings("unchecked")
                        Class<? extends Enum> enumTypeClass = (Class<? extends Enum>) field.getType();
                        field.set(instance, Enum.valueOf(enumTypeClass, value.toString()));
                    }
                } else {
                    field.set(instance, value);
                }
            }
            invokePostLoad(instance, entityClass);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Object emptyResult(Class<?> returnType, Class<?> entityClass) {
        if (returnType == Optional.class) {
            return Optional.empty();
        }
        if (returnType == List.class) {
            return List.of();
        }
        if (returnType.isAssignableFrom(entityClass)) {
            return null;
        }
        return null;
    }

    private Object defaultStorageValue(Class<?> storageType) {
        return switch (storageType) {
            case Class<?> c when c == int.class || c == Integer.class -> 0;
            case Class<?> c when c == long.class || c == Long.class -> 0L;
            case Class<?> c when c == short.class || c == Short.class -> (short) 0;
            case Class<?> c when c == byte.class || c == Byte.class -> (byte) 0;
            case Class<?> c when c == float.class || c == Float.class -> 0.0f;
            case Class<?> c when c == double.class || c == Double.class -> 0.0d;
            case Class<?> c when c == boolean.class || c == Boolean.class -> false;
            case Class<?> c when c == char.class || c == Character.class -> (char) 0;
            case Class<?> c when c == String.class -> "";
            default -> null;
        };
    }

    private <T> Object adaptFindResults(List<T> results, Class<?> returnType, Class<T> entityClass) {
        if (returnType == Optional.class) {
            return Optional.of(results.get(0));
        }
        if (returnType == List.class) {
            return results;
        }
        if (returnType.isAssignableFrom(entityClass)) {
            return results.get(0);
        }
        return results;
    }

    private Field resolveIdField(Class<?> entityClass) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(jakarta.persistence.GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                return field;
            }
        }
        return null;
    }

    private String findIdColumnName(Class<?> entityClass) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(jakarta.persistence.GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                return field.getName();
            }
        }
        return null;
    }

    private String parseColumnName(String methodName) {
        if (methodName.startsWith("findBy") || methodName.startsWith("countBy")) {
            String condition = methodName.startsWith("findBy") ? methodName.substring(6) : methodName.substring(8);

            String columnName = condition;
            if (condition.contains("GreaterThanEqual")) {
                columnName = condition.substring(0, condition.indexOf("GreaterThanEqual"));
            } else if (condition.contains("LessThanEqual")) {
                columnName = condition.substring(0, condition.indexOf("LessThanEqual"));
            } else if (condition.contains("GreaterThan")) {
                columnName = condition.substring(0, condition.indexOf("GreaterThan"));
            } else if (condition.contains("LessThan")) {
                columnName = condition.substring(0, condition.indexOf("LessThan"));
            } else if (condition.contains("NotEqual")) {
                columnName = condition.substring(0, condition.indexOf("NotEqual"));
            } else if (condition.contains("After")) {
                columnName = condition.substring(0, condition.indexOf("After"));
            } else if (condition.contains("Before")) {
                columnName = condition.substring(0, condition.indexOf("Before"));
            } else if (condition.contains("Containing")) {
                columnName = condition.substring(0, condition.indexOf("Containing"));
            } else if (condition.contains("StartingWith")) {
                columnName = condition.substring(0, condition.indexOf("StartingWith"));
            } else if (condition.contains("EndingWith")) {
                columnName = condition.substring(0, condition.indexOf("EndingWith"));
            }
            return toCamelCase(columnName);
        }
        return null;
    }

    private Predicate.Operator parseOperator(String methodName) {
        if (methodName.contains("GreaterThanEqual")) {
            return Predicate.Operator.GTE;
        }
        if (methodName.contains("LessThanEqual")) {
            return Predicate.Operator.LTE;
        }
        if (methodName.contains("GreaterThan") || methodName.contains("After")) {
            return Predicate.Operator.GT;
        }
        if (methodName.contains("LessThan") || methodName.contains("Before")) {
            return Predicate.Operator.LT;
        }
        if (methodName.contains("NotEqual")) {
            return Predicate.Operator.NEQ;
        }
        return Predicate.Operator.EQ;
    }

    private int[] scanTable(FfmTable table, String columnName,
                            Predicate.Operator operator, Object value) {
        Predicate predicate = new Predicate.Comparison(columnName, operator, value);
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();
        return table.scan(predicate, factory).toIntArray();
    }

    private Object getFromTable(FfmTable table, String columnName,
                                int row, Class<?> type) {
        if (type == UUID.class) {
            Column<?> msbColumn = table.column(columnName + "_msb");
            Column<?> lsbColumn = table.column(columnName + "_lsb");
            if (msbColumn != null && lsbColumn != null) {
                long msb = table.getLong(columnName + "_msb", row);
                long lsb = table.getLong(columnName + "_lsb", row);
                if (msb == 0L && lsb == 0L) {
                    return null;
                }
                return new UUID(msb, lsb);
            }
        }

        Column<?> column = table.column(columnName);
        if (column == null) {
            return null;
        }
        Class<?> colType = column.type();

        // Use pattern matching switch for O(1) dispatch instead of O(n) string comparison
        return switch (colType) {
            case Class<?> c when c == Integer.class -> table.getInt(columnName, row);
            case Class<?> c when c == Long.class -> table.getLong(columnName, row);
            case Class<?> c when c == Boolean.class -> table.getBoolean(columnName, row);
            case Class<?> c when c == Byte.class -> table.getByte(columnName, row);
            case Class<?> c when c == Short.class -> table.getShort(columnName, row);
            case Class<?> c when c == Float.class -> table.getFloat(columnName, row);
            case Class<?> c when c == Double.class -> table.getDouble(columnName, row);
            case Class<?> c when c == Character.class -> table.getChar(columnName, row);
            case Class<?> c when c == String.class -> table.getString(columnName, row);
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };
    }

    /**
     * Sort results by OrderBy specifications.
     * Configurable algorithm selection: AUTO, BUBBLE, JAVA_SORT, PARALLEL_STREAM.
     * Concurrent sorting support via parallel streams.
     */
    private int[] sortResults(FfmTable table, Class<?> entityClass, int[] indices, List<io.memris.spring.plan.LogicalQuery.OrderBy> orders) {
        if (orders.isEmpty() || indices.length <= 1) {
            return indices;
        }

        // Choose algorithm based on configuration and data size
        SortAlgorithm algorithm = switch (sortAlgorithm) {
            case BUBBLE -> SortAlgorithm.BUBBLE;
            case JAVA_SORT -> SortAlgorithm.JAVA_SORT;
            case PARALLEL_STREAM -> SortAlgorithm.PARALLEL_STREAM;
            case AUTO -> {
                // Auto-select based on data size and parallel settings
                if (enableParallelSorting && indices.length >= parallelSortThreshold) {
                    yield SortAlgorithm.PARALLEL_STREAM;
                } else if (indices.length < 100) {
                    yield SortAlgorithm.BUBBLE;  // Fast for small n
                } else {
                    yield SortAlgorithm.JAVA_SORT;  // O(n log n) for larger n
                }
            }
        };

        return switch (algorithm) {
            case BUBBLE -> bubbleSort(table, entityClass, indices, orders);
            case PARALLEL_STREAM -> parallelSort(table, entityClass, indices, orders);
            default -> javaSort(table, entityClass, indices, orders);
        };
    }

    /**
     * Bubble sort - O(n²) but extremely fast for small n (< 100).
     * Zero allocation, cache-friendly for small datasets.
     */
    private int[] bubbleSort(FfmTable table, Class<?> entityClass, int[] indices, List<io.memris.spring.plan.LogicalQuery.OrderBy> orders) {
        int[] sorted = indices.clone();
        for (int i = 0; i < sorted.length - 1; i++) {
            for (int j = 0; j < sorted.length - i - 1; j++) {
                if (compareIndices(table, entityClass, sorted[j], sorted[j + 1], orders) < 0) {
                    int temp = sorted[j];
                    sorted[j] = sorted[j + 1];
                    sorted[j + 1] = temp;
                }
            }
        }
        return sorted;
    }

    /**
     * Java's optimized sort - O(n log n).
     * Uses Arrays.sort with custom comparator.
     */
    private int[] javaSort(FfmTable table, Class<?> entityClass, int[] indices, List<io.memris.spring.plan.LogicalQuery.OrderBy> orders) {
        // Create boxed Integer array for comparator
        Integer[] boxed = Arrays.stream(indices).boxed().toArray(Integer[]::new);
        Arrays.sort(boxed, (i1, i2) -> compareIndices(table, entityClass, i1, i2, orders));
        return Arrays.stream(boxed).mapToInt(Integer::intValue).toArray();
    }

    /**
     * Parallel merge sort using Java 8 streams - O(n log n) with multiple threads.
     * Best for large datasets (>1000 elements).
     */
    private int[] parallelSort(FfmTable table, Class<?> entityClass, int[] indices, List<io.memris.spring.plan.LogicalQuery.OrderBy> orders) {
        Integer[] boxed = Arrays.stream(indices).boxed().toArray(Integer[]::new);
        Arrays.parallelSort(boxed, (i1, i2) -> compareIndices(table, entityClass, i1, i2, orders));
        return Arrays.stream(boxed).mapToInt(Integer::intValue).toArray();
    }

    /**
     * Compare two row indices using OrderBy specifications.
     * Returns negative if idx1 < idx2, zero if equal, positive if idx1 > idx2.
     */
    private int compareIndices(FfmTable table, Class<?> entityClass, int idx1, int idx2, List<io.memris.spring.plan.LogicalQuery.OrderBy> orders) {
        boolean ascending = orders.get(0).ascending();
        Object val1 = getKeyValue(table, entityClass, orders.get(0).propertyPath(), idx1);
        Object val2 = getKeyValue(table, entityClass, orders.get(0).propertyPath(), idx2);
        return compareValues(val1, val2, ascending);
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object val1, Object val2, boolean ascending) {
        if (val1 == null && val2 == null) return 0;
        if (val1 == null) return ascending ? -1 : 1;
        if (val2 == null) return ascending ? 1 : -1;

        // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
        int cmp = switch (val1) {
            case Comparable<?> c1 when val2 instanceof Comparable<?> -> ((Comparable<Object>) c1).compareTo(val2);
            default -> val1.toString().compareTo(val2.toString());
        };

        return ascending ? cmp : -cmp;
    }

    /**
     * Apply DISTINCT modifier to remove duplicate entities.
     * Uses identity hash set for O(n) deduplication.
     */
    private int[] applyDistinct(int[] indices) {
        Set<Integer> seenIndices = new HashSet<>();
        List<Integer> distinct = new ArrayList<>();

        for (int idx : indices) {
            if (seenIndices.add(idx)) {
                distinct.add(idx);
            }
        }

        return distinct.stream().mapToInt(Integer::intValue).toArray();
    }

    /**
     * Perform hash join between two entity types.
     * O(n + m) hash join algorithm using HashMap for blazing fast lookups.
     *
     * @param leftClass Left entity class
     * @param leftKey Join key property in left entity
     * @param rightClass Right entity class
     * @param rightKey Join key property in right entity
     * @return List of JoinResult containing matching pairs
     */
    public <L, R> List<JoinResult<L, R>> join(
            Class<L> leftClass,
            String leftKey,
            Class<R> rightClass,
            String rightKey) {
        SelectionVectorFactory factory = SelectionVectorFactory.defaultFactory();

        FfmTable leftTable = tableManager.getTable(leftClass);
        FfmTable rightTable = tableManager.getTable(rightClass);

        int[] leftIndices = leftTable.scanAll(factory).toIntArray();
        int[] rightIndices = rightTable.scanAll(factory).toIntArray();

        // Build hash map from left table - O(n)
        HashMap<Object, int[]> leftHash = new HashMap<>();
        for (int idx : leftIndices) {
            Object key = getKeyValue(leftTable, leftClass, leftKey, idx);
            leftHash.computeIfAbsent(key, k -> new int[0]);
            int[] existing = leftHash.get(key);
            int[] updated = new int[existing.length + 1];
            System.arraycopy(existing, 0, updated, 0, existing.length);
            updated[existing.length] = idx;
            leftHash.put(key, updated);
        }

        // Probe right table and build results - O(m)
        List<JoinResult<L, R>> results = new ArrayList<>();
        for (int idx : rightIndices) {
            Object key = getKeyValue(rightTable, rightClass, rightKey, idx);
            int[] leftMatches = leftHash.get(key);
            if (leftMatches != null) {
                for (int leftIdx : leftMatches) {
                    results.add(new JoinResult<>(
                            materializeSingle(leftTable, leftClass, leftIdx),
                            materializeSingle(rightTable, rightClass, idx)
                    ));
                }
            }
        }
        return results;
    }

    /**
     * Materialize a single entity from a table row.
     * Uses pattern matching switch for O(1) type dispatch.
     */
    private <T> T materializeSingle(FfmTable table, Class<T> entityClass, int rowIdx) {
        try {
            T instance = entityClass.getDeclaredConstructor().newInstance();
            for (var col : table.columns()) {
                // Handle foreign key columns (parent_id -> parent field)
                String fieldName = col.name();
                if (fieldName.endsWith("_id")) {
                    fieldName = fieldName.substring(0, fieldName.length() - 3);
                } else if (fieldName.endsWith("_msb")) {
                    // UUID foreign key - handle both _msb and _lsb together
                    continue;
                }

                java.lang.reflect.Field field;
                try {
                    field = entityClass.getDeclaredField(fieldName);
                } catch (NoSuchFieldException e) {
                    // Column doesn't map to a field (e.g., _lsb of UUID, or unmapped column)
                    continue;
                }
                field.setAccessible(true);

                // Check if this is an entity reference field (@ManyToOne, @OneToOne, or @Entity type)
                if (field.isAnnotationPresent(ManyToOne.class) ||
                    field.isAnnotationPresent(OneToOne.class) ||
                    isEntityType(field.getType())) {
                    // This is a foreign key - load the related entity
                    // NOTE: Foreign key loading is now handled by ByteBuddy-generated code
                    // This old reflection-based code is kept as fallback for now
                    Object foreignKeyValue = switch (col.type()) {
                        case Class<?> c when c == int.class || c == Integer.class -> table.getInt(col.name(), rowIdx);
                        case Class<?> c when c == long.class || c == Long.class -> table.getLong(col.name(), rowIdx);
                        case Class<?> c when c == String.class -> table.getString(col.name(), rowIdx);
                        default -> throw new IllegalArgumentException("Unsupported type: " + col.type());
                    };

                    // Skip if foreign key is null/0
                    if (foreignKeyValue == null ||
                        (foreignKeyValue instanceof Integer i && i == 0) ||
                        (foreignKeyValue instanceof Long l && l == 0L) ||
                        (foreignKeyValue instanceof String s && s.isEmpty())) {
                        field.set(instance, null);
                        continue;
                    }

                    // Load the related entity by ID using queryIndex
                    Class<?> entityType = field.getType();
                    try {
                        FfmTable relatedTable = tableManager.getTable(entityType);
                        if (relatedTable != null) {
                            int[] matchingRows = queryIndex(entityType, col.name(), Predicate.Operator.EQ, foreignKeyValue);
                            if (matchingRows != null && matchingRows.length > 0) {
                                Object relatedEntity = materializeSingle(relatedTable, entityType, matchingRows[0]);
                                field.set(instance, relatedEntity);
                            } else {
                                field.set(instance, null);
                            }
                        } else {
                            field.set(instance, null);
                        }
                    } catch (Exception e) {
                        // Fallback to null if loading fails
                        field.set(instance, null);
                    }
                    continue;
                }

                // Regular field (not a foreign key)
                Object value = switch (col.type()) {
                    case Class<?> c when c == int.class || c == Integer.class -> table.getInt(col.name(), rowIdx);
                    case Class<?> c when c == long.class || c == Long.class -> table.getLong(col.name(), rowIdx);
                    case Class<?> c when c == String.class -> table.getString(col.name(), rowIdx);
                    default -> throw new IllegalArgumentException("Unsupported type: " + col.type());
                };
                // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
                switch (value) {
                    case String str when str.isEmpty() && field.getType() == String.class -> field.set(instance, null);
                    default -> {
                        if (field.getType().isEnum() && field.isAnnotationPresent(Enumerated.class)) {
                            EnumType enumType = field.getAnnotation(Enumerated.class).value();
                            Class<?> enumClass = field.getType();
                            Object enumValue;
                            if (enumType == EnumType.ORDINAL) {
                                int ordinal = (Integer) value;
                                if (ordinal < 0) {
                                    field.set(instance, null);
                                    continue;
                                }
                                enumValue = enumClass.getEnumConstants()[ordinal];
                            } else {
                                String strValue = (String) value;
                                if (strValue.isEmpty()) {
                                    field.set(instance, null);
                                    continue;
                                }
                                enumValue = Enum.valueOf((Class<Enum>) enumClass, strValue);
                            }
                            field.set(instance, enumValue);
                        } else {
                            field.set(instance, value);
                        }
                    }
                }
            }
            invokePostLoad(instance, entityClass);
            return instance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
