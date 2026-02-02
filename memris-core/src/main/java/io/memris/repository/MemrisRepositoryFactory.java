package io.memris.repository;

import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.FieldMapping;
import static io.memris.core.EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY;
import io.memris.core.GeneratedValue;
import io.memris.core.Index;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.core.MetadataExtractor;
import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.query.CompiledQuery;
import io.memris.query.QueryCompiler;
import io.memris.query.QueryPlanner;
import io.memris.runtime.EntityMaterializer;
import io.memris.runtime.HeapRuntimeKernel;
import io.memris.runtime.JoinCollectionMaterializer;
import io.memris.runtime.JoinExecutorImpl;
import io.memris.runtime.JoinExecutorManyToMany;
import io.memris.runtime.JoinMaterializerImpl;
import io.memris.runtime.NoopJoinMaterializer;
import io.memris.runtime.RepositoryMethodBinding;
import io.memris.runtime.RepositoryMethodExecutor;
import io.memris.runtime.RepositoryPlan;
import io.memris.runtime.RepositoryRuntime;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.storage.GeneratedTable;
import io.memris.storage.SimpleTable;
import io.memris.storage.SimpleTable.ColumnSpec;
import jakarta.persistence.Id;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.Locale;
import java.util.HashMap;
import java.util.Map;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;



public final class MemrisRepositoryFactory implements AutoCloseable {

    private final Map<Class<?>, GeneratedTable> tables = new HashMap<>();
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
        RuntimeExecutorGenerator.setConfiguration(configuration);
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

    GeneratedTable getTable(Class<?> entityClass) {
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

        GeneratedTable table = tables.get(entityClass);
        if (table == null) {
            return null;
        }
        java.util.function.Predicate<RowId> validator = rowId -> {
            int rowIndex = (int) rowId.value();
            long generation = table.rowGeneration(rowIndex);
            long ref = io.memris.storage.Selection.pack(rowIndex, generation);
            return table.isLive(ref);
        };

        // Java 21+ pattern matching with switch on sealed types
        return switch (index) {
            case HashIndex hashIndex when value != null -> rowIdSetToIntArray(hashIndex.lookup(value, validator));
            case RangeIndex rangeIndex when value instanceof Comparable comp -> switch (operator) {
                case EQ -> rowIdSetToIntArray(rangeIndex.lookup(comp, validator));
                case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(comp, validator));
                case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(comp, validator));
                case LT -> rowIdSetToIntArray(rangeIndex.lessThan(comp, validator));
                case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(comp, validator));
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
                    yield rowIdSetToIntArray(rangeIndex.between(lowerComp, upperComp, validator));
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

    @Override
    public void close() {
        // TODO: Clean up resources
    }

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
        var entityClass = extractEntityClass(repositoryInterface);

        // 2. Create or get the table for this entity (no arena for default)
        var table = tables.computeIfAbsent(entityClass, ec -> buildTableForEntity(ec, null));

        // 3. Build indexes for this entity
        buildIndexesForEntity(entityClass);

        // 4. Extract entity metadata
        var metadata = MetadataExtractor.extractEntityMetadata(entityClass);

        // 5. Get all query methods from the repository interface
        var methods = RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);

        // 6. Plan and compile all query methods
        var compiledQueries = new CompiledQuery[methods.length];
        var compiler = new QueryCompiler(metadata);

        for (int i = 0; i < methods.length; i++) {
            var method = methods[i];
            var logicalQuery = QueryPlanner.parse(method, entityClass);
            compiledQueries[i] = compiler.compile(logicalQuery);
        }

        var tablesByEntity = buildJoinTables(metadata, null);
        var kernelsByEntity = buildJoinKernels(tablesByEntity);
        var materializersByEntity = buildJoinMaterializers(tablesByEntity);
        var joinTables = buildManyToManyJoinTables(metadata);
        compiledQueries = wireJoinRuntime(compiledQueries, metadata, tablesByEntity, kernelsByEntity,
                materializersByEntity, joinTables);

        var bindings = RepositoryMethodBinding.fromQueries(compiledQueries);
        var executors = buildExecutors(compiledQueries, bindings);

        // 7. Extract column metadata for RepositoryPlan
        var columnNames = extractColumnNames(metadata);
        var typeCodes = extractTypeCodes(metadata);
        var converters = extractConverters(metadata);
        var setters = extractSetters(metadata);

        var conditionExecutors = RepositoryRuntime.buildConditionExecutors(compiledQueries, columnNames, metadata.entityClass(), true);
        var orderExecutors = RepositoryRuntime.buildOrderExecutors(compiledQueries, table);
        var projectionExecutors = RepositoryRuntime.buildProjectionExecutors(compiledQueries);

        // 8. Create entity constructor handle
        MethodHandle entityConstructor;
        try {
            entityConstructor = MethodHandles.lookup().unreflectConstructor(metadata.entityConstructor());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }

        // 9. Generate EntitySaver for the entity
        var entitySaver = EntitySaverGenerator.generate(entityClass, metadata);

        // 10. Build RepositoryPlan
        var plan = RepositoryPlan.fromGeneratedTable(
                table,
                entityClass,
                metadata.idColumnName(),
                compiledQueries,
                bindings,
                executors,
                conditionExecutors,
                orderExecutors,
                projectionExecutors,
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
        var runtime = new RepositoryRuntime<T>(plan, this, metadata);

        // 11. Generate repository implementation using ByteBuddy
        var emitter = new RepositoryEmitter();
        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    private static RepositoryMethodExecutor[] buildExecutors(
            CompiledQuery[] queries,
            RepositoryMethodBinding[] bindings) {
        RepositoryMethodExecutor[] executors = new RepositoryMethodExecutor[queries.length];
        for (int i = 0; i < queries.length; i++) {
            executors[i] = executorFor(queries[i], bindings[i]);
        }
        return executors;
    }

    private static RepositoryMethodExecutor executorFor(CompiledQuery query,
                                                        RepositoryMethodBinding binding) {
        return switch (query.opCode()) {
            case SAVE_ONE -> (runtime, args)
                    -> ((RepositoryRuntime) runtime).saveOne(args[0]);
            case SAVE_ALL -> (runtime, args)
                    -> ((RepositoryRuntime) runtime)
                    .saveAll((Iterable<?>) args[0]);
            case FIND_BY_ID -> (runtime, args)
                    -> ((RepositoryRuntime) runtime).findById(args[0]);
            case FIND_ALL -> (runtime, args)
                    -> ((RepositoryRuntime) runtime).findAll();
            case FIND -> (runtime, args)
                    -> runtime.find(binding.query(), binding.resolveArgs(args));
            case COUNT -> (runtime, args)
                    -> runtime.countFast(binding.query(), binding.resolveArgs(args));
            case COUNT_ALL -> (runtime, args)
                    -> runtime.countAll();
            case EXISTS -> (runtime, args)
                    -> runtime.existsFast(binding.query(), binding.resolveArgs(args));
            case EXISTS_BY_ID -> (runtime, args)
                    -> runtime.existsById(args[0]);
            case DELETE_ONE -> (runtime, args) -> {
                runtime.deleteOne(args[0]);
                return null;
            };
            case DELETE_ALL -> (runtime, args) -> {
                runtime.deleteAll();
                return null;
            };
            case DELETE_BY_ID -> (runtime, args) -> {
                runtime.deleteById(args[0]);
                return null;
            };
            case DELETE_QUERY -> (runtime, args) -> runtime
                    .deleteQuery(binding.query(), binding.resolveArgs(args));
            case UPDATE_QUERY -> (runtime, args) -> runtime
                    .updateQuery(binding.query(), binding.resolveArgs(args));
            case DELETE_ALL_BY_ID -> (runtime, args) -> runtime
                    .deleteAllById((Iterable<?>) args[0]);
            default -> throw new UnsupportedOperationException("Unsupported OpCode: " + query.opCode());
        };
    }

    private <T> Map<Class<?>, GeneratedTable> buildJoinTables(
            EntityMetadata<T> metadata,
            MemrisArena arena) {
        var tablesByEntity = new HashMap<Class<?>, GeneratedTable>();
        var queue = new ArrayDeque<Class<?>>();

        tablesByEntity.put(metadata.entityClass(), arena != null ? arena.getOrCreateTable(metadata.entityClass())
                : tables.computeIfAbsent(metadata.entityClass(), ec -> buildTableForEntity(ec, null)));
        queue.add(metadata.entityClass());

        while (!queue.isEmpty()) {
            Class<?> current = queue.poll();
            var currentMetadata = MetadataExtractor
                    .extractEntityMetadata(current);
            for (var field : currentMetadata.fields()) {
                if (!field.isRelationship() || field.targetEntity() == null) {
                    continue;
                }
                Class<?> target = field.targetEntity();
                if (tablesByEntity.containsKey(target)) {
                    continue;
                }
                var table = arena != null
                        ? arena.getOrCreateTable(target)
                        : tables.computeIfAbsent(target, ec -> buildTableForEntity(ec, null));
                tablesByEntity.put(target, table);
                queue.add(target);
            }
        }
        return Map.copyOf(tablesByEntity);
    }

    private Map<Class<?>, HeapRuntimeKernel> buildJoinKernels(
            Map<Class<?>, GeneratedTable> tablesByEntity) {
        Map<Class<?>, HeapRuntimeKernel> kernels = new java.util.HashMap<>();
        for (var entry : tablesByEntity.entrySet()) {
            kernels.put(entry.getKey(), new HeapRuntimeKernel(entry.getValue()));
        }
        return Map.copyOf(kernels);
    }

    private Map<Class<?>, EntityMaterializer<?>> buildJoinMaterializers(
            Map<Class<?>, GeneratedTable> tablesByEntity) {
        Map<Class<?>, EntityMaterializer<?>> materializers = new java.util.HashMap<>();
        var generator = new io.memris.runtime.EntityMaterializerGenerator();
        for (var entityClass : tablesByEntity.keySet()) {
            var entityMetadata = MetadataExtractor
                    .extractEntityMetadata(entityClass);
            materializers.put(entityClass, generator.generate(entityMetadata));
        }
        return Map.copyOf(materializers);
    }

    private <T> Map<String, SimpleTable> buildManyToManyJoinTables(
            EntityMetadata<T> metadata) {
        Map<String, SimpleTable> joinTables = new java.util.HashMap<>();
        for (var field : metadata.fields()) {
            if (!field.isRelationship() || field
                    .relationshipType() != MANY_TO_MANY) {
                continue;
            }
            if (field.joinTable() == null || field.joinTable().isBlank()) {
                continue;
            }

            var targetMetadata = MetadataExtractor
                    .extractEntityMetadata(field.targetEntity());
            var sourceId = findIdField(metadata);
            var targetId = findIdField(targetMetadata);

            if (sourceId == null || targetId == null) {
                continue;
            }

            var joinColumn = field.columnName();
            if (joinColumn == null || joinColumn.isBlank()) {
                joinColumn = metadata.entityClass().getSimpleName().toLowerCase(Locale.ROOT) + "_" + metadata.idColumnName();
            }
            var inverseJoinColumn = field.referencedColumnName();
            if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
                inverseJoinColumn = field.targetEntity().getSimpleName().toLowerCase(Locale.ROOT) + "_" + targetMetadata.idColumnName();
            }
            var finalJoinColumn = joinColumn;
            var finalInverseJoinColumn = inverseJoinColumn;
            joinTables.computeIfAbsent(field.joinTable(), name -> {
                var specs = java.util.List.of(
                        new ColumnSpec<>(finalJoinColumn, sourceId.javaType()),
                        new ColumnSpec<>(finalInverseJoinColumn, targetId.javaType()));
                return new SimpleTable(name, specs);
            });

        }
        return Map.copyOf(joinTables);
    }

    private static FieldMapping findIdField(io.memris.core.EntityMetadata<?> metadata) {
        for (var field : metadata.fields()) {
            if (field.name().equals(metadata.idColumnName())) {
                return field;
            }
        }
        return null;
    }

    private <T> CompiledQuery[] wireJoinRuntime(
            CompiledQuery[] compiledQueries,
            EntityMetadata<T> metadata,
            Map<Class<?>, GeneratedTable> tablesByEntity,
            Map<Class<?>, HeapRuntimeKernel> kernelsByEntity,
            Map<Class<?>, EntityMaterializer<?>> materializersByEntity,
            Map<String, SimpleTable> joinTables) {
        CompiledQuery[] wired = new CompiledQuery[compiledQueries.length];
        for (int i = 0; i < compiledQueries.length; i++) {
            var query = compiledQueries[i];
            var joins = query.joins();
            if (joins == null || joins.length == 0) {
                wired[i] = query;
                continue;
            }
            CompiledQuery.CompiledJoin[] updated = new CompiledQuery.CompiledJoin[joins.length];
            for (int j = 0; j < joins.length; j++) {
                var join = joins[j];
                var targetTable = tablesByEntity.get(join.targetEntity());
                var targetKernel = kernelsByEntity.get(join.targetEntity());
                var targetMaterializer = materializersByEntity.get(join.targetEntity());
                io.memris.core.EntityMetadata<?> targetMetadata = MetadataExtractor
                        .extractEntityMetadata(join.targetEntity());
                var postLoadHandle = targetMetadata.postLoadHandle();
                var fieldMapping = findFieldMapping(metadata,
                        join.relationshipFieldName());
                var executor = buildJoinExecutor(metadata, targetMetadata, join,
                        fieldMapping, joinTables);
                var setter = metadata.fieldSetters().get(join.relationshipFieldName());
                var materializer = buildJoinMaterializer(fieldMapping, join, setter,
                        postLoadHandle);
                updated[j] = join.withRuntime(targetTable, targetKernel, targetMaterializer, executor, materializer);
            }
            wired[i] = query.withJoins(updated);
        }
        return wired;
    }

    private static io.memris.runtime.JoinExecutor buildJoinExecutor(io.memris.core.EntityMetadata<?> metadata,
            io.memris.core.EntityMetadata<?> targetMetadata,
            CompiledQuery.CompiledJoin join,
            FieldMapping fieldMapping,
            Map<String, SimpleTable> joinTables) {
        if (fieldMapping != null && fieldMapping
                .relationshipType() == MANY_TO_MANY) {
            JoinTableInfo joinInfo = resolveJoinTableInfo(metadata, fieldMapping, joinTables);
            if (joinInfo == null) {
                return new JoinExecutorManyToMany(null, null, null,
                        join.sourceColumnIndex(), join.fkTypeCode(), join.targetColumnIndex(), join.fkTypeCode(),
                        join.joinType());
            }
            FieldMapping sourceId = findIdField(metadata);
            FieldMapping targetId = findIdField(targetMetadata);
            int sourceIdColumnIndex = metadata.resolveColumnPosition(metadata.idColumnName());
            int targetIdColumnIndex = targetMetadata.resolveColumnPosition(targetMetadata.idColumnName());
            byte sourceIdTypeCode = sourceId != null ? sourceId.typeCode() : join.fkTypeCode();
            byte targetIdTypeCode = targetId != null ? targetId.typeCode() : join.fkTypeCode();
            return new JoinExecutorManyToMany(
                    joinInfo.table,
                    joinInfo.joinColumn,
                    joinInfo.inverseJoinColumn,
                    sourceIdColumnIndex,
                    sourceIdTypeCode,
                    targetIdColumnIndex,
                    targetIdTypeCode,
                    join.joinType());
        }
        return new JoinExecutorImpl(
                join.sourceColumnIndex(),
                join.targetColumnIndex(),
                join.targetColumnIsId(),
                join.fkTypeCode(),
                join.joinType());
    }

    private static io.memris.runtime.JoinMaterializer buildJoinMaterializer(
            FieldMapping fieldMapping,
            CompiledQuery.CompiledJoin join,
            MethodHandle setter,
            MethodHandle postLoadHandle) {
        if (fieldMapping != null && fieldMapping.isCollection()) {
            if (fieldMapping
                    .relationshipType() == MANY_TO_MANY) {
                return new NoopJoinMaterializer();
            }
            Class<?> collectionType = fieldMapping.javaType();
            return new JoinCollectionMaterializer(
                    join.sourceColumnIndex(),
                    join.targetColumnIndex(),
                    join.fkTypeCode(),
                    setter,
                    postLoadHandle,
                    collectionType);
        }
        return new JoinMaterializerImpl(
                join.sourceColumnIndex(),
                join.targetColumnIndex(),
                join.targetColumnIsId(),
                join.fkTypeCode(),
                setter,
                postLoadHandle);
    }

    private record JoinTableInfo(String joinColumn, String inverseJoinColumn, SimpleTable table) {
    }

    private static JoinTableInfo resolveJoinTableInfo(io.memris.core.EntityMetadata<?> sourceMetadata,
            FieldMapping field,
            Map<String, SimpleTable> joinTables) {
        if (field.joinTable() != null && !field.joinTable().isBlank()) {
            return buildJoinTableInfo(field, sourceMetadata,
                    MetadataExtractor.extractEntityMetadata(field.targetEntity()), false, joinTables);
        }
        if (field.mappedBy() != null && !field.mappedBy().isBlank()) {
            io.memris.core.EntityMetadata<?> targetMetadata = MetadataExtractor
                    .extractEntityMetadata(field.targetEntity());
            FieldMapping ownerField = findFieldMapping(targetMetadata, field.mappedBy());
            if (ownerField == null || ownerField.joinTable() == null || ownerField.joinTable().isBlank()) {
                return null;
            }
            return buildJoinTableInfo(ownerField, targetMetadata, sourceMetadata, true, joinTables);
        }
        return null;
    }

    private static JoinTableInfo buildJoinTableInfo(FieldMapping ownerField,
                                                    io.memris.core.EntityMetadata<?> ownerMetadata,
                                                    io.memris.core.EntityMetadata<?> inverseMetadata,
                                                    boolean inverseSide,
                                                    Map<String, SimpleTable> joinTables) {
        String joinTableName = ownerField.joinTable();
        if (joinTableName == null || joinTableName.isBlank()) {
            return null;
        }
        SimpleTable table = joinTables.get(joinTableName);
        if (table == null) {
            return null;
        }

        String joinColumn;
        String inverseJoinColumn;
        if (!inverseSide) {
            joinColumn = ownerField.columnName();
            if (joinColumn == null || joinColumn.isBlank()) {
                joinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase(Locale.ROOT) + "_"
                        + ownerMetadata.idColumnName();
            }
            inverseJoinColumn = ownerField.referencedColumnName();
            if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
                inverseJoinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase(Locale.ROOT) + "_"
                        + inverseMetadata.idColumnName();
            }
            return new JoinTableInfo(joinColumn, inverseJoinColumn, table);
        }

        joinColumn = ownerField.referencedColumnName();
        if (joinColumn == null || joinColumn.isBlank()) {
            joinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase(Locale.ROOT) + "_"
                    + inverseMetadata.idColumnName();
        }
        inverseJoinColumn = ownerField.columnName();
        if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
            inverseJoinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase(Locale.ROOT) + "_"
                    + ownerMetadata.idColumnName();
        }
        return new JoinTableInfo(joinColumn, inverseJoinColumn, table);
    }

    private static FieldMapping findFieldMapping(
            io.memris.core.EntityMetadata<?> metadata, String fieldName) {
        for (FieldMapping field : metadata.fields()) {
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
    public <T> GeneratedTable buildTableForEntity(Class<T> entityClass, MemrisArena arena) {
        // Extract metadata to get field information
        EntityMetadata<T> metadata = MetadataExtractor.extractEntityMetadata(entityClass);

        // Build TableMetadata for TableGenerator
        java.util.List<io.memris.storage.heap.FieldMetadata> fields = new java.util.ArrayList<>();

        for (FieldMapping fm : metadata.fields()) {
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
            int pageSize = configuration.pageSize();
            int maxPages = configuration.maxPages();
            int initialPages = configuration.initialPages();
            io.memris.storage.heap.AbstractTable table = tableClass
                    .getDeclaredConstructor(int.class, int.class, int.class)
                    .newInstance(pageSize, maxPages, initialPages);
            return (GeneratedTable) table;
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
        for (var field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(GeneratedValue.class) ||
                    field.isAnnotationPresent(Id.class) ||
                    field.getName().equals("id")) {
                entityIndexes.put(field.getName(), new io.memris.index.HashIndex<>());
                break;
            }
        }

        // Build indexes for @Index annotated fields
        for (var field : entityClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(Index.class)) {
                var fieldName = field.getName();
                var indexAnnotation = field.getAnnotation(Index.class);

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
                .sorted(java.util.Comparator.comparingInt(FieldMapping::columnPosition))
                .map(FieldMapping::columnName)
                .toArray(String[]::new);
    }

    /**
     * Extract type codes from entity metadata.
     */
    private byte[] extractTypeCodes(io.memris.core.EntityMetadata<?> metadata) {
        var fields = metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(FieldMapping::columnPosition))
                .toList();
        var typeCodes = new byte[fields.size()];
        for (var i = 0; i < fields.size(); i++) {
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
                .sorted(java.util.Comparator.comparingInt(FieldMapping::columnPosition))
                .map(fm -> metadata.converters().getOrDefault(fm.name(), null))
                .toArray(io.memris.core.converter.TypeConverter[]::new);
    }

    /**
     * Extract setter MethodHandles from entity metadata.
     */
    private MethodHandle[] extractSetters(io.memris.core.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(FieldMapping::columnPosition))
                .map(fm -> metadata.fieldSetters().get(fm.name()))
                .toArray(MethodHandle[]::new);
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
