package io.memris.spring;

import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.kernel.Column;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.converter.TypeConverterRegistry;
import io.memris.storage.SimpleTable;
import io.memris.storage.SimpleTable.ColumnSpec;
import jakarta.persistence.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public final class MemrisRepositoryFactory implements AutoCloseable {

    private final Map<String, IdGenerator<?>> customIdGenerators = new HashMap<>();
    private final Map<Class<?>, AtomicLong> numericIdCounters = new HashMap<>();
    private final Map<Class<?>, io.memris.storage.GeneratedTable> tables = new HashMap<>();
    private final Map<Class<?>, Map<String, Object>> indexes = new HashMap<>();

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

    public MemrisRepositoryFactory() {

    }

    /**
     * Register custom ID generator for testability or custom ID strategies.
     * @param name Generator name (referenced by @GeneratedValue.generator)
     * @param generator IdGenerator instance
     */
    public <T> void registerIdGenerator(String name, IdGenerator<T> generator) {
        customIdGenerators.put(name, generator);
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

    private <T> io.memris.storage.GeneratedTable buildTable(Class<T> entityClass) {
        // TODO: Implement using TableGenerator to create GeneratedTable
        // For now, return null to allow compilation
        return null;
    }

    private <T> void buildIndexes(Class<T> entityClass, io.memris.kernel.Table table) {
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

    io.memris.storage.GeneratedTable getTable(Class<?> entityClass) {
        return tables.get(entityClass);
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
    public int[] queryIndex(Class<?> entityClass, String fieldName, Predicate.Operator operator, Object value) {
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
                case EQ -> rowIdSetToIntArray(rangeIndex.lookup(comp));
                case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(comp));
                case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(comp));
                case LT -> rowIdSetToIntArray(rangeIndex.lessThan(comp));
                case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(comp));
                default -> null;
            };
            case RangeIndex rangeIndex when operator == Predicate.Operator.BETWEEN && value instanceof Object[] range -> {
                if (range.length < 2) {
                    yield null;
                }
                Object lower = range[0];
                Object upper = range[1];
                if (lower instanceof Comparable lowerComp && upper instanceof Comparable upperComp) {
                    yield rowIdSetToIntArray(rangeIndex.between(lowerComp, upperComp));
                }
                yield null;
            }
            default -> null;
        };
    }

    public void addIndexEntry(Class<?> entityClass, String fieldName, Object value, int rowIndex) {
        Object index = getIndex(entityClass, fieldName);
        if (index == null || value == null) {
            return;
        }
        RowId rowId = RowId.fromLong(rowIndex);
        switch (index) {
            case HashIndex hashIndex -> hashIndex.add(value, rowId);
            case RangeIndex rangeIndex -> {
                if (value instanceof Comparable comp) {
                    rangeIndex.add(comp, rowId);
                }
            }
            default -> {
            }
        }
    }

    public void removeIndexEntry(Class<?> entityClass, String fieldName, Object value, int rowIndex) {
        Object index = getIndex(entityClass, fieldName);
        if (index == null || value == null) {
            return;
        }
        RowId rowId = RowId.fromLong(rowIndex);
        switch (index) {
            case HashIndex hashIndex -> hashIndex.remove(value, rowId);
            case RangeIndex rangeIndex -> {
                if (value instanceof Comparable comp) {
                    rangeIndex.remove(comp, rowId);
                }
            }
            default -> {
            }
        }
    }

    public void clearIndexes(Class<?> entityClass) {
        Map<String, Object> entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null) {
            return;
        }
        for (Object index : entityIndexes.values()) {
            switch (index) {
                case HashIndex hashIndex -> hashIndex.clear();
                case RangeIndex rangeIndex -> rangeIndex.clear();
                default -> {
                }
            }
        }
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
        // TODO: Clean up resources
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


    // ========== Helper methods for repository implementation ==========

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

    private Object getFromTable(io.memris.kernel.Table table, String columnName,
                                int row, Class<?> type) {
        // TODO: Implement using GeneratedTable API
        return null;
    }

    // TODO: Reimplement sorting using GeneratedTable API
    // TODO: Reimplement join() using GeneratedTable API
    // TODO: Reimplement materializeSingle() using GeneratedTable API

    /**
     * Create a JPA repository from a repository interface.
     * <p>
     * This is the main entry point for creating type-safe repositories.
     * It:
     * 1. Extracts the entity class from the repository interface
     * 2. Creates or retrieves the GeneratedTable for the entity
     * 3. Builds indexes for the entity
     * 4. Extracts entity metadata
     * 5. Plans and compiles all query methods
     * 6. Builds a RepositoryPlan
     * 7. Creates a RepositoryRuntime
     * 8. Generates the repository implementation using ByteBuddy
     *
     * @param repositoryInterface the repository interface (must extend MemrisRepository<T>)
     * @param <T>               the entity type
     * @param <R>               the repository interface type
     * @return an instantiated repository implementation
     */
    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R createJPARepository(Class<R> repositoryInterface) {
        // 1. Extract entity class from repository interface
        Class<T> entityClass = extractEntityClass(repositoryInterface);

        // 2. Create or get the table for this entity (no arena for default)
        io.memris.storage.GeneratedTable table = tables.computeIfAbsent(entityClass, ec -> buildTableForEntity(ec, null));

        // 3. Build indexes for this entity
        buildIndexesForEntity(entityClass);

        // 4. Extract entity metadata
        io.memris.spring.EntityMetadata<T> metadata = io.memris.spring.MetadataExtractor.extractEntityMetadata(entityClass);

        // 5. Get all query methods from the repository interface
        java.lang.reflect.Method[] methods = io.memris.spring.scaffold.RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);

        // 6. Plan and compile all query methods
        io.memris.spring.plan.CompiledQuery[] compiledQueries = new io.memris.spring.plan.CompiledQuery[methods.length];
        io.memris.spring.plan.QueryCompiler compiler = new io.memris.spring.plan.QueryCompiler(metadata);

        for (int i = 0; i < methods.length; i++) {
            java.lang.reflect.Method method = methods[i];
            io.memris.spring.plan.LogicalQuery logicalQuery = io.memris.spring.plan.QueryPlanner.parse(
                    method, entityClass, metadata.idColumnName());
            compiledQueries[i] = compiler.compile(logicalQuery);
        }

        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = buildJoinTables(metadata, null);
        java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> kernelsByEntity = buildJoinKernels(tablesByEntity);
        java.util.Map<Class<?>, io.memris.spring.runtime.EntityMaterializer<?>> materializersByEntity = buildJoinMaterializers(tablesByEntity);
        compiledQueries = wireJoinRuntime(compiledQueries, metadata, tablesByEntity, kernelsByEntity, materializersByEntity);

        // 7. Extract column metadata for RepositoryPlan
        String[] columnNames = extractColumnNames(metadata);
        byte[] typeCodes = extractTypeCodes(metadata);
        io.memris.spring.converter.TypeConverter<?, ?>[] converters = extractConverters(metadata);
        java.lang.invoke.MethodHandle[] setters = extractSetters(metadata);

        // 8. Create entity constructor handle
        java.lang.invoke.MethodHandle entityConstructor;
        try {
            entityConstructor = java.lang.invoke.MethodHandles.lookup()
                    .unreflectConstructor(metadata.entityConstructor());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }

        // 9. Generate EntitySaver for the entity
        io.memris.spring.runtime.EntitySaver<T> entitySaver = 
            io.memris.spring.scaffold.EntitySaverGenerator.generate(entityClass, metadata);
        
        // 10. Build RepositoryPlan
        io.memris.spring.runtime.RepositoryPlan<T> plan = io.memris.spring.runtime.RepositoryPlan.fromGeneratedTable(
                table,
                entityClass,
                metadata.idColumnName(),
                compiledQueries,
                entityConstructor,
                columnNames,
                typeCodes,
                converters,
                setters,
                tablesByEntity,
                kernelsByEntity,
                materializersByEntity,
                entitySaver
        );

        // 10. Create RepositoryRuntime
        io.memris.spring.runtime.RepositoryRuntime<T> runtime = new io.memris.spring.runtime.RepositoryRuntime<>(plan, this, metadata);

        // 11. Generate repository implementation using ByteBuddy
        io.memris.spring.scaffold.RepositoryEmitter emitter = new io.memris.spring.scaffold.RepositoryEmitter();
        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    private <T> java.util.Map<Class<?>, io.memris.storage.GeneratedTable> buildJoinTables(
        io.memris.spring.EntityMetadata<T> metadata,
        MemrisArena arena
    ) {
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = new java.util.HashMap<>();
        tablesByEntity.put(metadata.entityClass(), arena != null ? arena.getOrCreateTable(metadata.entityClass()) :
            tables.computeIfAbsent(metadata.entityClass(), ec -> buildTableForEntity(ec, null)));

        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (!field.isRelationship() || field.targetEntity() == null) {
                continue;
            }
            Class<?> target = field.targetEntity();
            io.memris.storage.GeneratedTable table = arena != null
                ? arena.getOrCreateTable(target)
                : tables.computeIfAbsent(target, ec -> buildTableForEntity(ec, null));
            tablesByEntity.putIfAbsent(target, table);
        }
        return java.util.Map.copyOf(tablesByEntity);
    }

    private java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> buildJoinKernels(
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity
    ) {
        java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> kernels = new java.util.HashMap<>();
        for (var entry : tablesByEntity.entrySet()) {
            kernels.put(entry.getKey(), new io.memris.spring.runtime.HeapRuntimeKernel(entry.getValue()));
        }
        return java.util.Map.copyOf(kernels);
    }

    private java.util.Map<Class<?>, io.memris.spring.runtime.EntityMaterializer<?>> buildJoinMaterializers(
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity
    ) {
        java.util.Map<Class<?>, io.memris.spring.runtime.EntityMaterializer<?>> materializers = new java.util.HashMap<>();
        io.memris.spring.runtime.EntityMaterializerGenerator generator = new io.memris.spring.runtime.EntityMaterializerGenerator();
        for (Class<?> entityClass : tablesByEntity.keySet()) {
            io.memris.spring.EntityMetadata<?> entityMetadata = io.memris.spring.MetadataExtractor.extractEntityMetadata(entityClass);
            materializers.put(entityClass, generator.generate(entityMetadata));
        }
        return java.util.Map.copyOf(materializers);
    }

    private <T> io.memris.spring.plan.CompiledQuery[] wireJoinRuntime(
        io.memris.spring.plan.CompiledQuery[] compiledQueries,
        io.memris.spring.EntityMetadata<T> metadata,
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity,
        java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> kernelsByEntity,
        java.util.Map<Class<?>, io.memris.spring.runtime.EntityMaterializer<?>> materializersByEntity
    ) {
        io.memris.spring.plan.CompiledQuery[] wired = new io.memris.spring.plan.CompiledQuery[compiledQueries.length];
        for (int i = 0; i < compiledQueries.length; i++) {
            var query = compiledQueries[i];
            var joins = query.joins();
            if (joins == null || joins.length == 0) {
                wired[i] = query;
                continue;
            }
            io.memris.spring.plan.CompiledQuery.CompiledJoin[] updated = new io.memris.spring.plan.CompiledQuery.CompiledJoin[joins.length];
            for (int j = 0; j < joins.length; j++) {
                var join = joins[j];
                var targetTable = tablesByEntity.get(join.targetEntity());
                var targetKernel = kernelsByEntity.get(join.targetEntity());
                var targetMaterializer = materializersByEntity.get(join.targetEntity());
                io.memris.spring.runtime.JoinExecutor executor = new io.memris.spring.runtime.JoinExecutorImpl(
                    join.sourceColumnIndex(),
                    join.targetColumnIndex(),
                    join.targetColumnIsId(),
                    join.fkTypeCode(),
                    join.joinType()
                );
                java.lang.invoke.MethodHandle setter = metadata.fieldSetters().get(join.relationshipFieldName());
                io.memris.spring.runtime.JoinMaterializer materializer = new io.memris.spring.runtime.JoinMaterializerImpl(
                    join.sourceColumnIndex(),
                    join.targetColumnIndex(),
                    join.targetColumnIsId(),
                    join.fkTypeCode(),
                    setter
                );
                updated[j] = join.withRuntime(targetTable, targetKernel, targetMaterializer, executor, materializer);
            }
            wired[i] = query.withJoins(updated);
        }
        return wired;
    }

    /**
     * Build a table for the given entity class.
     * <p>
     * Uses TableGenerator to create a GeneratedTable implementation.
     */
    /**
     * Build a table for an entity class.
     * If arena is provided, the table is cached in the arena.
     */
    <T> io.memris.storage.GeneratedTable buildTableForEntity(Class<T> entityClass, MemrisArena arena) {
        // Extract metadata to get field information
        io.memris.spring.EntityMetadata<T> metadata = io.memris.spring.MetadataExtractor.extractEntityMetadata(entityClass);

        // Build TableMetadata for TableGenerator
        java.util.List<io.memris.storage.heap.FieldMetadata> fields = new java.util.ArrayList<>();
        int fieldIndex = 0;
        byte idTypeCode = io.memris.spring.TypeCodes.TYPE_LONG; // default

        for (io.memris.spring.EntityMetadata.FieldMapping fm : metadata.fields()) {
            if (fm.columnPosition() < 0) {
                continue; // Skip collection fields (no column)
            }

            Byte tc = fm.typeCode();
            byte typeCode = (tc != null) ? tc.byteValue() : io.memris.spring.TypeCodes.TYPE_LONG;

            // Check if this is the ID field
            boolean isId = fm.name().equals(metadata.idColumnName());
            if (isId) {
                idTypeCode = typeCode;
            }

            fields.add(new io.memris.storage.heap.FieldMetadata(
                    fm.columnName(),
                    typeCode,
                    isId,
                    isId
            ));
            fieldIndex++;
        }

        String entityName = entityClass.getSimpleName();
        io.memris.storage.heap.TableMetadata tableMetadata = new io.memris.storage.heap.TableMetadata(
                entityName,
                entityClass.getCanonicalName(),
                fields
        );

        // Generate the table class using ByteBuddy
        Class<? extends io.memris.storage.heap.AbstractTable> tableClass;
        try {
            tableClass = io.memris.storage.heap.TableGenerator.generate(tableMetadata);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate table for entity: " + entityClass.getName(), e);
        }

        // Instantiate the generated table
        try {
            io.memris.storage.heap.AbstractTable table = tableClass.getDeclaredConstructor(int.class, int.class)
                    .newInstance(1024, 1024); // Default page size and max pages
            return (io.memris.storage.GeneratedTable) table;
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate table for entity: " + entityClass.getName(), e);
        }
    }

    /**
     * Build indexes for the given entity class.
     */
    private <T> void buildIndexesForEntity(Class<T> entityClass) {
        Map<String, Object> entityIndexes = new HashMap<>();

        // Always create a HashIndex on the ID field for O(1) lookups
        for (java.lang.reflect.Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                field.isAnnotationPresent(Id.class) ||
                field.getName().equals("id")) {
                entityIndexes.put(field.getName(), new io.memris.index.HashIndex<>());
                break;
            }
        }

        // Build indexes for @Index annotated fields
        for (java.lang.reflect.Field field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Index.class)) {
                String fieldName = field.getName();
                Index indexAnnotation = field.getAnnotation(Index.class);

                Object index;
                if (indexAnnotation.type() == Index.IndexType.BTREE) {
                    index = new io.memris.index.RangeIndex<>();
                } else {
                    index = new io.memris.index.HashIndex<>();
                }
                entityIndexes.put(fieldName, index);
            }
        }

        if (!entityIndexes.isEmpty()) {
            indexes.put(entityClass, entityIndexes);
        }
    }

    /**
     * Extract column names from entity metadata.
     */
    private String[] extractColumnNames(io.memris.spring.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .map(io.memris.spring.EntityMetadata.FieldMapping::columnName)
                .toArray(String[]::new);
    }

    /**
     * Extract type codes from entity metadata.
     */
    private byte[] extractTypeCodes(io.memris.spring.EntityMetadata<?> metadata) {
        var fields = metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .toList();
        byte[] typeCodes = new byte[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            // FieldMapping.typeCode() returns a Byte, get byte value or default to TYPE_LONG
            Byte tc = fields.get(i).typeCode();
            typeCodes[i] = tc != null ? tc.byteValue() : io.memris.spring.TypeCodes.TYPE_LONG;
        }
        return typeCodes;
    }

    /**
     * Extract converters from entity metadata.
     */
    private io.memris.spring.converter.TypeConverter<?, ?>[] extractConverters(io.memris.spring.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .map(fm -> metadata.converters().getOrDefault(fm.name(), null))
                .toArray(io.memris.spring.converter.TypeConverter[]::new);
    }

    /**
     * Extract setter MethodHandles from entity metadata.
     */
    private java.lang.invoke.MethodHandle[] extractSetters(io.memris.spring.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .map(fm -> metadata.fieldSetters().get(fm.name()))
                .toArray(java.lang.invoke.MethodHandle[]::new);
    }

    // ========== Arena Support Methods ==========

    private final java.util.concurrent.atomic.AtomicLong arenaCounter = new java.util.concurrent.atomic.AtomicLong(0);
    private final java.util.Map<Long, MemrisArena> arenas = new java.util.HashMap<>();

    /**
     * Create a new isolated arena.
     * <p>
     * Each arena has its own tables, repositories, and indexes.
     * Data is completely isolated between arenas.
     *
     * @return the new arena
     */
    public MemrisArena createArena() {
        long arenaId = arenaCounter.incrementAndGet();
        MemrisArena arena = new MemrisArena(arenaId, this);
        arenas.put(arenaId, arena);
        return arena;
    }

    /**
     * Create a repository in the default (non-arena) scope.
     * <p>
     * This is a convenience method for backward compatibility.
     *
     * @param repositoryInterface the repository interface
     * @param <T> the entity type
     * @param <R> the repository interface type
     * @return the repository instance
     */
    public <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface) {
        return createJPARepository(repositoryInterface);
    }
}
