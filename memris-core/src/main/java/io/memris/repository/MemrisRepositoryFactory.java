package io.memris.repository;

import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;

import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.core.IdGenerator;
import io.memris.core.MemrisConfiguration;
import io.memris.core.MemrisArena;

import io.memris.core.GeneratedValue;
import io.memris.core.Index;
import jakarta.persistence.*;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public final class MemrisRepositoryFactory implements AutoCloseable {

    private final Map<String, IdGenerator<?>> customIdGenerators = new HashMap<>();

    private final Map<Class<?>, io.memris.storage.GeneratedTable> tables = new HashMap<>();
    private final Map<Class<?>, Map<String, Object>> indexes = new HashMap<>();

    // Configuration settings
    private final MemrisConfiguration configuration;

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
     * Creates a factory with default configuration.
     */
    public MemrisRepositoryFactory() {
        this(MemrisConfiguration.builder().build());
    }

    /**
     * Creates a factory with the specified configuration.
     *
     * @param configuration the configuration to use
     */
    public MemrisRepositoryFactory(MemrisConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * Get the configuration for this factory.
     *
     * @return the configuration
     */
    public MemrisConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Register custom ID generator for testability or custom ID strategies.
     * 
     * @param name      Generator name (referenced by @GeneratedValue.generator)
     * @param generator IdGenerator instance
     */
    public <T> void registerIdGenerator(String name, IdGenerator<T> generator) {
        customIdGenerators.put(name, generator);
    }

    /**
     * Extract entity class T from repository interface extending
     * MemrisRepository<T>
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
     * 
     * @param entityClass The entity class
     * @param fieldName   The indexed field name
     * @param operator    The comparison operator
     * @param value       The value to match
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
            case RangeIndex rangeIndex when operator == Predicate.Operator.BETWEEN
                    && value instanceof Object[] range -> {
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
            intArray[i] = (int) longArray[i]; // RowId value IS the row index
        }
        return intArray;
    }

    public void close() {
        // TODO: Clean up resources
    }

    // ========== Helper methods for repository implementation ==========

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
     * @param repositoryInterface the repository interface (must extend
     *                            MemrisRepository<T>)
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return an instantiated repository implementation
     */
    public <T, R extends MemrisRepository<T>> R createJPARepository(Class<R> repositoryInterface) {
        // 1. Extract entity class from repository interface
        Class<T> entityClass = extractEntityClass(repositoryInterface);

        // 2. Create or get the table for this entity (no arena for default)
        io.memris.storage.GeneratedTable table = tables.computeIfAbsent(entityClass,
                ec -> buildTableForEntity(ec, null));

        // 3. Build indexes for this entity
        buildIndexesForEntity(entityClass);

        // 4. Extract entity metadata
        io.memris.core.EntityMetadata<T> metadata = io.memris.core.MetadataExtractor.extractEntityMetadata(entityClass);

        // 5. Get all query methods from the repository interface
        java.lang.reflect.Method[] methods = io.memris.repository.RepositoryMethodIntrospector
                .extractQueryMethods(repositoryInterface);

        // 6. Plan and compile all query methods
        io.memris.query.CompiledQuery[] compiledQueries = new io.memris.query.CompiledQuery[methods.length];
        io.memris.query.QueryCompiler compiler = new io.memris.query.QueryCompiler(metadata);

        for (int i = 0; i < methods.length; i++) {
            java.lang.reflect.Method method = methods[i];
            io.memris.query.LogicalQuery logicalQuery = io.memris.query.QueryPlanner.parse(
                    method, entityClass, metadata.idColumnName());
            compiledQueries[i] = compiler.compile(logicalQuery);
        }

        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = buildJoinTables(metadata, null);
        java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> kernelsByEntity = buildJoinKernels(tablesByEntity);
        java.util.Map<Class<?>, io.memris.runtime.EntityMaterializer<?>> materializersByEntity = buildJoinMaterializers(
                tablesByEntity);
        java.util.Map<String, io.memris.storage.SimpleTable> joinTables = buildManyToManyJoinTables(metadata);
        compiledQueries = wireJoinRuntime(compiledQueries, metadata, tablesByEntity, kernelsByEntity,
                materializersByEntity, joinTables);

        io.memris.runtime.RepositoryMethodBinding[] bindings = io.memris.runtime.RepositoryMethodBinding
                .fromQueries(compiledQueries);
        io.memris.runtime.RepositoryMethodExecutor[] executors = buildExecutors(compiledQueries, bindings);

        // 7. Extract column metadata for RepositoryPlan
        String[] columnNames = extractColumnNames(metadata);
        byte[] typeCodes = extractTypeCodes(metadata);
        io.memris.core.converter.TypeConverter<?, ?>[] converters = extractConverters(metadata);
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
        io.memris.runtime.EntitySaver<T, ?> entitySaver = io.memris.repository.EntitySaverGenerator.generate(
                entityClass,
                metadata);

        // 10. Build RepositoryPlan
        io.memris.runtime.RepositoryPlan<T> plan = io.memris.runtime.RepositoryPlan.fromGeneratedTable(
                table,
                entityClass,
                metadata.idColumnName(),
                compiledQueries,
                bindings,
                executors,
                entityConstructor,
                columnNames,
                typeCodes,
                converters,
                setters,
                tablesByEntity,
                kernelsByEntity,
                materializersByEntity,
                joinTables,
                entitySaver);

        // 10. Create RepositoryRuntime
        io.memris.runtime.RepositoryRuntime<T> runtime = new io.memris.runtime.RepositoryRuntime<>(plan, this,
                metadata);

        // 11. Generate repository implementation using ByteBuddy
        io.memris.repository.RepositoryEmitter emitter = new io.memris.repository.RepositoryEmitter();
        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    private static io.memris.runtime.RepositoryMethodExecutor[] buildExecutors(
            io.memris.query.CompiledQuery[] queries,
            io.memris.runtime.RepositoryMethodBinding[] bindings) {
        io.memris.runtime.RepositoryMethodExecutor[] executors = new io.memris.runtime.RepositoryMethodExecutor[queries.length];
        for (int i = 0; i < queries.length; i++) {
            executors[i] = executorFor(queries[i], bindings[i]);
        }
        return executors;
    }

    private static io.memris.runtime.RepositoryMethodExecutor executorFor(io.memris.query.CompiledQuery query,
            io.memris.runtime.RepositoryMethodBinding binding) {
        return switch (query.opCode()) {
            case SAVE_ONE -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime).saveOne(args[0]);
            case SAVE_ALL -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime)
                    .saveAll((Iterable<?>) args[0]);
            case FIND_BY_ID -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime).findById(args[0]);
            case FIND_ALL -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime).findAll();
            case FIND -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime).find(binding.query(),
                    binding.resolveArgs(args));
            case COUNT -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime)
                    .countFast(binding.query(), binding.resolveArgs(args));
            case COUNT_ALL -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime).countAll();
            case EXISTS -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime)
                    .existsFast(binding.query(), binding.resolveArgs(args));
            case EXISTS_BY_ID -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime).existsById(args[0]);
            case DELETE_ONE -> (runtime, args) -> {
                ((io.memris.runtime.RepositoryRuntime) runtime).deleteOne(args[0]);
                return null;
            };
            case DELETE_ALL -> (runtime, args) -> {
                ((io.memris.runtime.RepositoryRuntime) runtime).deleteAll();
                return null;
            };
            case DELETE_BY_ID -> (runtime, args) -> {
                ((io.memris.runtime.RepositoryRuntime) runtime).deleteById(args[0]);
                return null;
            };
            case DELETE_QUERY -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime)
                    .deleteQuery(binding.query(), binding.resolveArgs(args));
            case UPDATE_QUERY -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime)
                    .updateQuery(binding.query(), binding.resolveArgs(args));
            case DELETE_ALL_BY_ID -> (runtime, args) -> ((io.memris.runtime.RepositoryRuntime) runtime)
                    .deleteAllById((Iterable<?>) args[0]);
            default -> throw new UnsupportedOperationException("Unsupported OpCode: " + query.opCode());
        };
    }

    private <T> java.util.Map<Class<?>, io.memris.storage.GeneratedTable> buildJoinTables(
            io.memris.core.EntityMetadata<T> metadata,
            MemrisArena arena) {
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = new java.util.HashMap<>();
        java.util.ArrayDeque<Class<?>> queue = new java.util.ArrayDeque<>();

        tablesByEntity.put(metadata.entityClass(), arena != null ? arena.getOrCreateTable(metadata.entityClass())
                : tables.computeIfAbsent(metadata.entityClass(), ec -> buildTableForEntity(ec, null)));
        queue.add(metadata.entityClass());

        while (!queue.isEmpty()) {
            Class<?> current = queue.poll();
            io.memris.core.EntityMetadata<?> currentMetadata = io.memris.core.MetadataExtractor
                    .extractEntityMetadata(current);
            for (io.memris.core.EntityMetadata.FieldMapping field : currentMetadata.fields()) {
                if (!field.isRelationship() || field.targetEntity() == null) {
                    continue;
                }
                Class<?> target = field.targetEntity();
                if (tablesByEntity.containsKey(target)) {
                    continue;
                }
                io.memris.storage.GeneratedTable table = arena != null
                        ? arena.getOrCreateTable(target)
                        : tables.computeIfAbsent(target, ec -> buildTableForEntity(ec, null));
                tablesByEntity.put(target, table);
                queue.add(target);
            }
        }
        return java.util.Map.copyOf(tablesByEntity);
    }

    private java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> buildJoinKernels(
            java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity) {
        java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> kernels = new java.util.HashMap<>();
        for (var entry : tablesByEntity.entrySet()) {
            kernels.put(entry.getKey(), new io.memris.runtime.HeapRuntimeKernel(entry.getValue()));
        }
        return java.util.Map.copyOf(kernels);
    }

    private java.util.Map<Class<?>, io.memris.runtime.EntityMaterializer<?>> buildJoinMaterializers(
            java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity) {
        java.util.Map<Class<?>, io.memris.runtime.EntityMaterializer<?>> materializers = new java.util.HashMap<>();
        io.memris.runtime.EntityMaterializerGenerator generator = new io.memris.runtime.EntityMaterializerGenerator();
        for (Class<?> entityClass : tablesByEntity.keySet()) {
            io.memris.core.EntityMetadata<?> entityMetadata = io.memris.core.MetadataExtractor
                    .extractEntityMetadata(entityClass);
            materializers.put(entityClass, generator.generate(entityMetadata));
        }
        return java.util.Map.copyOf(materializers);
    }

    private <T> java.util.Map<String, io.memris.storage.SimpleTable> buildManyToManyJoinTables(
            io.memris.core.EntityMetadata<T> metadata) {
        java.util.Map<String, io.memris.storage.SimpleTable> joinTables = new java.util.HashMap<>();
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (!field.isRelationship() || field
                    .relationshipType() != io.memris.core.EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY) {
                continue;
            }
            if (field.joinTable() == null || field.joinTable().isBlank()) {
                continue;
            }

            io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor
                    .extractEntityMetadata(field.targetEntity());
            io.memris.core.EntityMetadata.FieldMapping sourceId = findIdField(metadata);
            io.memris.core.EntityMetadata.FieldMapping targetId = findIdField(targetMetadata);

            if (sourceId == null || targetId == null) {
                continue;
            }

            String joinColumn = field.columnName();
            if (joinColumn == null || joinColumn.isBlank()) {
                joinColumn = metadata.entityClass().getSimpleName().toLowerCase() + "_" + metadata.idColumnName();
            }
            String inverseJoinColumn = field.referencedColumnName();
            if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
                inverseJoinColumn = field.targetEntity().getSimpleName().toLowerCase() + "_"
                        + targetMetadata.idColumnName();
            }

            final String finalJoinColumn = joinColumn;
            final String finalInverseJoinColumn = inverseJoinColumn;
            joinTables.computeIfAbsent(field.joinTable(), name -> {
                java.util.List<io.memris.storage.SimpleTable.ColumnSpec<?>> specs = java.util.List.of(
                        new io.memris.storage.SimpleTable.ColumnSpec<>(finalJoinColumn, sourceId.javaType()),
                        new io.memris.storage.SimpleTable.ColumnSpec<>(finalInverseJoinColumn, targetId.javaType()));
                return new io.memris.storage.SimpleTable(name, specs);
            });

        }
        return java.util.Map.copyOf(joinTables);
    }

    private static io.memris.core.EntityMetadata.FieldMapping findIdField(io.memris.core.EntityMetadata<?> metadata) {
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.name().equals(metadata.idColumnName())) {
                return field;
            }
        }
        return null;
    }

    private <T> io.memris.query.CompiledQuery[] wireJoinRuntime(
            io.memris.query.CompiledQuery[] compiledQueries,
            io.memris.core.EntityMetadata<T> metadata,
            java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity,
            java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> kernelsByEntity,
            java.util.Map<Class<?>, io.memris.runtime.EntityMaterializer<?>> materializersByEntity,
            java.util.Map<String, io.memris.storage.SimpleTable> joinTables) {
        io.memris.query.CompiledQuery[] wired = new io.memris.query.CompiledQuery[compiledQueries.length];
        for (int i = 0; i < compiledQueries.length; i++) {
            var query = compiledQueries[i];
            var joins = query.joins();
            if (joins == null || joins.length == 0) {
                wired[i] = query;
                continue;
            }
            io.memris.query.CompiledQuery.CompiledJoin[] updated = new io.memris.query.CompiledQuery.CompiledJoin[joins.length];
            for (int j = 0; j < joins.length; j++) {
                var join = joins[j];
                var targetTable = tablesByEntity.get(join.targetEntity());
                var targetKernel = kernelsByEntity.get(join.targetEntity());
                var targetMaterializer = materializersByEntity.get(join.targetEntity());
                io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor
                        .extractEntityMetadata(join.targetEntity());
                java.lang.invoke.MethodHandle postLoadHandle = targetMetadata.postLoadHandle();
                io.memris.core.EntityMetadata.FieldMapping fieldMapping = findFieldMapping(metadata,
                        join.relationshipFieldName());
                io.memris.runtime.JoinExecutor executor = buildJoinExecutor(metadata, targetMetadata, join,
                        fieldMapping, joinTables);
                java.lang.invoke.MethodHandle setter = metadata.fieldSetters().get(join.relationshipFieldName());
                io.memris.runtime.JoinMaterializer materializer = buildJoinMaterializer(fieldMapping, join, setter,
                        postLoadHandle);
                updated[j] = join.withRuntime(targetTable, targetKernel, targetMaterializer, executor, materializer);
            }
            wired[i] = query.withJoins(updated);
        }
        return wired;
    }

    private static io.memris.runtime.JoinExecutor buildJoinExecutor(io.memris.core.EntityMetadata<?> metadata,
            io.memris.core.EntityMetadata<?> targetMetadata,
            io.memris.query.CompiledQuery.CompiledJoin join,
            io.memris.core.EntityMetadata.FieldMapping fieldMapping,
            java.util.Map<String, io.memris.storage.SimpleTable> joinTables) {
        if (fieldMapping != null && fieldMapping
                .relationshipType() == io.memris.core.EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY) {
            JoinTableInfo joinInfo = resolveJoinTableInfo(metadata, fieldMapping, joinTables);
            if (joinInfo == null) {
                return new io.memris.runtime.JoinExecutorManyToMany(null, null, null,
                        join.sourceColumnIndex(), join.fkTypeCode(), join.targetColumnIndex(), join.fkTypeCode(),
                        join.joinType());
            }
            io.memris.core.EntityMetadata.FieldMapping sourceId = findIdField(metadata);
            io.memris.core.EntityMetadata.FieldMapping targetId = findIdField(targetMetadata);
            int sourceIdColumnIndex = metadata.resolveColumnPosition(metadata.idColumnName());
            int targetIdColumnIndex = targetMetadata.resolveColumnPosition(targetMetadata.idColumnName());
            byte sourceIdTypeCode = sourceId != null ? sourceId.typeCode() : join.fkTypeCode();
            byte targetIdTypeCode = targetId != null ? targetId.typeCode() : join.fkTypeCode();
            return new io.memris.runtime.JoinExecutorManyToMany(
                    joinInfo.table,
                    joinInfo.joinColumn,
                    joinInfo.inverseJoinColumn,
                    sourceIdColumnIndex,
                    sourceIdTypeCode,
                    targetIdColumnIndex,
                    targetIdTypeCode,
                    join.joinType());
        }
        return new io.memris.runtime.JoinExecutorImpl(
                join.sourceColumnIndex(),
                join.targetColumnIndex(),
                join.targetColumnIsId(),
                join.fkTypeCode(),
                join.joinType());
    }

    private static io.memris.runtime.JoinMaterializer buildJoinMaterializer(
            io.memris.core.EntityMetadata.FieldMapping fieldMapping,
            io.memris.query.CompiledQuery.CompiledJoin join,
            java.lang.invoke.MethodHandle setter,
            java.lang.invoke.MethodHandle postLoadHandle) {
        if (fieldMapping != null && fieldMapping.isCollection()) {
            if (fieldMapping
                    .relationshipType() == io.memris.core.EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY) {
                return new io.memris.runtime.NoopJoinMaterializer();
            }
            Class<?> collectionType = fieldMapping.javaType();
            return new io.memris.runtime.JoinCollectionMaterializer(
                    join.sourceColumnIndex(),
                    join.targetColumnIndex(),
                    join.fkTypeCode(),
                    setter,
                    postLoadHandle,
                    collectionType);
        }
        return new io.memris.runtime.JoinMaterializerImpl(
                join.sourceColumnIndex(),
                join.targetColumnIndex(),
                join.targetColumnIsId(),
                join.fkTypeCode(),
                setter,
                postLoadHandle);
    }

    private record JoinTableInfo(String joinColumn, String inverseJoinColumn, io.memris.storage.SimpleTable table) {
    }

    private static JoinTableInfo resolveJoinTableInfo(io.memris.core.EntityMetadata<?> sourceMetadata,
            io.memris.core.EntityMetadata.FieldMapping field,
            java.util.Map<String, io.memris.storage.SimpleTable> joinTables) {
        if (field.joinTable() != null && !field.joinTable().isBlank()) {
            return buildJoinTableInfo(field, sourceMetadata,
                    io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity()), false, joinTables);
        }
        if (field.mappedBy() != null && !field.mappedBy().isBlank()) {
            io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor
                    .extractEntityMetadata(field.targetEntity());
            io.memris.core.EntityMetadata.FieldMapping ownerField = findFieldMapping(targetMetadata, field.mappedBy());
            if (ownerField == null || ownerField.joinTable() == null || ownerField.joinTable().isBlank()) {
                return null;
            }
            return buildJoinTableInfo(ownerField, targetMetadata, sourceMetadata, true, joinTables);
        }
        return null;
    }

    private static JoinTableInfo buildJoinTableInfo(io.memris.core.EntityMetadata.FieldMapping ownerField,
            io.memris.core.EntityMetadata<?> ownerMetadata,
            io.memris.core.EntityMetadata<?> inverseMetadata,
            boolean inverseSide,
            java.util.Map<String, io.memris.storage.SimpleTable> joinTables) {
        String joinTableName = ownerField.joinTable();
        if (joinTableName == null || joinTableName.isBlank()) {
            return null;
        }
        io.memris.storage.SimpleTable table = joinTables.get(joinTableName);
        if (table == null) {
            return null;
        }

        String joinColumn;
        String inverseJoinColumn;
        if (!inverseSide) {
            joinColumn = ownerField.columnName();
            if (joinColumn == null || joinColumn.isBlank()) {
                joinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                        + ownerMetadata.idColumnName();
            }
            inverseJoinColumn = ownerField.referencedColumnName();
            if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
                inverseJoinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                        + inverseMetadata.idColumnName();
            }
            return new JoinTableInfo(joinColumn, inverseJoinColumn, table);
        }

        joinColumn = ownerField.referencedColumnName();
        if (joinColumn == null || joinColumn.isBlank()) {
            joinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                    + inverseMetadata.idColumnName();
        }
        inverseJoinColumn = ownerField.columnName();
        if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
            inverseJoinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase() + "_"
                    + ownerMetadata.idColumnName();
        }
        return new JoinTableInfo(joinColumn, inverseJoinColumn, table);
    }

    private static io.memris.core.EntityMetadata.FieldMapping findFieldMapping(
            io.memris.core.EntityMetadata<?> metadata, String fieldName) {
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }
        return null;
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
    public <T> io.memris.storage.GeneratedTable buildTableForEntity(Class<T> entityClass, MemrisArena arena) {
        // Extract metadata to get field information
        io.memris.core.EntityMetadata<T> metadata = io.memris.core.MetadataExtractor.extractEntityMetadata(entityClass);

        // Build TableMetadata for TableGenerator
        java.util.List<io.memris.storage.heap.FieldMetadata> fields = new java.util.ArrayList<>();

        for (io.memris.core.EntityMetadata.FieldMapping fm : metadata.fields()) {
            if (fm.columnPosition() < 0) {
                continue; // Skip collection fields (no column)
            }

            Byte tc = fm.typeCode();
            byte typeCode = (tc != null) ? tc.byteValue() : io.memris.core.TypeCodes.TYPE_LONG;

            // Check if this is the ID field
            boolean isId = fm.name().equals(metadata.idColumnName());

            fields.add(new io.memris.storage.heap.FieldMetadata(
                    fm.columnName(),
                    typeCode,
                    isId,
                    isId));

        }

        String entityName = entityClass.getSimpleName();
        io.memris.storage.heap.TableMetadata tableMetadata = new io.memris.storage.heap.TableMetadata(
                entityName,
                entityClass.getCanonicalName(),
                fields);

        // Generate the table class using ByteBuddy
        Class<? extends io.memris.storage.heap.AbstractTable> tableClass;
        try {
            tableClass = io.memris.storage.heap.TableGenerator.generate(tableMetadata, configuration);
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
    private String[] extractColumnNames(io.memris.core.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .map(io.memris.core.EntityMetadata.FieldMapping::columnName)
                .toArray(String[]::new);
    }

    /**
     * Extract type codes from entity metadata.
     */
    private byte[] extractTypeCodes(io.memris.core.EntityMetadata<?> metadata) {
        var fields = metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .toList();
        byte[] typeCodes = new byte[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            // FieldMapping.typeCode() returns a Byte, get byte value or default to
            // TYPE_LONG
            Byte tc = fields.get(i).typeCode();
            typeCodes[i] = tc != null ? tc.byteValue() : io.memris.core.TypeCodes.TYPE_LONG;
        }
        return typeCodes;
    }

    /**
     * Extract converters from entity metadata.
     */
    private io.memris.core.converter.TypeConverter<?, ?>[] extractConverters(
            io.memris.core.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .map(fm -> metadata.converters().getOrDefault(fm.name(), null))
                .toArray(io.memris.core.converter.TypeConverter[]::new);
    }

    /**
     * Extract setter MethodHandles from entity metadata.
     */
    private java.lang.invoke.MethodHandle[] extractSetters(io.memris.core.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
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
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return the repository instance
     */
    public <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface) {
        return createJPARepository(repositoryInterface);
    }
}
