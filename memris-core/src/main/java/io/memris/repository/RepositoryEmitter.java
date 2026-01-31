package io.memris.repository;

import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepository;
import io.memris.runtime.RepositoryRuntime;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Generates repository implementations using ByteBuddy.
 * <p>
 * Creates dynamic proxy classes that implement repository interfaces and
 * delegate method calls to the RepositoryRuntime.
 */
public final class RepositoryEmitter {

    private final ByteBuddy byteBuddy;
    private final MemrisConfiguration configuration;

    /**
     * Creates a RepositoryEmitter with default configuration.
     */
    public RepositoryEmitter() {
        this(MemrisConfiguration.builder().build());
    }

    /**
     * Creates a RepositoryEmitter with the specified configuration.
     *
     * @param configuration the configuration to use
     */
    public RepositoryEmitter(MemrisConfiguration configuration) {
        this.configuration = configuration;
        this.byteBuddy = new ByteBuddy();
    }

    /**
     * Static factory method for creating repositories with arena scoping.
     * This is called by MemrisArena to create arena-scoped repositories.
     *
     * @param repositoryInterface the repository interface class
     * @param arena               the arena for scoping
     * @param configuration       the configuration to use
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return an instance of the generated repository implementation
     */
    @SuppressWarnings("unchecked")
    public static <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface,
            io.memris.core.MemrisArena arena,
            MemrisConfiguration configuration) {
        RepositoryEmitter emitter = new RepositoryEmitter(configuration);

        // Extract entity class and create/get table
        Class<T> entityClass = extractEntityClass(repositoryInterface);
        io.memris.storage.GeneratedTable table = arena.getOrCreateTable(entityClass);

        // Extract entity metadata
        io.memris.core.EntityMetadata<T> metadata = io.memris.core.MetadataExtractor.extractEntityMetadata(entityClass);

        // Extract and compile query methods
        java.lang.reflect.Method[] methods = RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);
        io.memris.query.CompiledQuery[] compiledQueries = new io.memris.query.CompiledQuery[methods.length];

        io.memris.query.QueryCompiler compiler = new io.memris.query.QueryCompiler(metadata);

        for (int i = 0; i < methods.length; i++) {
            java.lang.reflect.Method method = methods[i];
            io.memris.query.LogicalQuery logicalQuery = io.memris.query.QueryPlanner.parse(method, entityClass,
                    metadata.idColumnName());
            compiledQueries[i] = compiler.compile(logicalQuery);
        }

        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = buildJoinTables(metadata, arena);
        java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> kernelsByEntity = buildJoinKernels(tablesByEntity);
        java.util.Map<Class<?>, io.memris.runtime.EntityMaterializer<?>> materializersByEntity = buildJoinMaterializers(
                tablesByEntity);
        java.util.Map<String, io.memris.storage.SimpleTable> joinTables = buildManyToManyJoinTables(metadata);
        compiledQueries = wireJoinRuntime(compiledQueries, metadata, tablesByEntity, kernelsByEntity,
                materializersByEntity, joinTables);

        // Extract column metadata for RepositoryPlan
        String[] columnNames = extractColumnNames(metadata);
        byte[] typeCodes = extractTypeCodes(metadata);
        io.memris.core.converter.TypeConverter<?, ?>[] converters = extractConverters(metadata);
        java.lang.invoke.MethodHandle[] setters = extractSetters(metadata);

        // Create entity constructor handle
        java.lang.invoke.MethodHandle entityConstructor;
        try {
            entityConstructor = java.lang.invoke.MethodHandles.lookup()
                    .unreflectConstructor(metadata.entityConstructor());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }

        // Generate EntitySaver for the entity
        io.memris.runtime.EntitySaver<T> entitySaver = EntitySaverGenerator.generate(entityClass, metadata);

        // Build RepositoryPlan with compiled queries
        io.memris.runtime.RepositoryPlan<T> plan = io.memris.runtime.RepositoryPlan.fromGeneratedTable(
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
                joinTables,
                entitySaver);

        // Create RepositoryRuntime
        io.memris.runtime.RepositoryRuntime<T> runtime = new io.memris.runtime.RepositoryRuntime<>(plan, null,
                metadata);

        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    private static <T> java.util.Map<Class<?>, io.memris.storage.GeneratedTable> buildJoinTables(
            io.memris.core.EntityMetadata<T> metadata,
            io.memris.core.MemrisArena arena) {
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = new java.util.HashMap<>();
        java.util.ArrayDeque<Class<?>> queue = new java.util.ArrayDeque<>();

        tablesByEntity.put(metadata.entityClass(), arena.getOrCreateTable(metadata.entityClass()));
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
                io.memris.storage.GeneratedTable table = arena.getOrCreateTable(target);
                tablesByEntity.put(target, table);
                queue.add(target);
            }
        }
        return java.util.Map.copyOf(tablesByEntity);
    }

    private static java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> buildJoinKernels(
            java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity) {
        java.util.Map<Class<?>, io.memris.runtime.HeapRuntimeKernel> kernels = new java.util.HashMap<>();
        for (var entry : tablesByEntity.entrySet()) {
            kernels.put(entry.getKey(), new io.memris.runtime.HeapRuntimeKernel(entry.getValue()));
        }
        return java.util.Map.copyOf(kernels);
    }

    private static java.util.Map<Class<?>, io.memris.runtime.EntityMaterializer<?>> buildJoinMaterializers(
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

    private static <T> java.util.Map<String, io.memris.storage.SimpleTable> buildManyToManyJoinTables(
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

    private static <T> io.memris.query.CompiledQuery[] wireJoinRuntime(
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
     * Extract column names from entity metadata.
     */
    private static String[] extractColumnNames(io.memris.core.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .map(io.memris.core.EntityMetadata.FieldMapping::columnName)
                .toArray(String[]::new);
    }

    /**
     * Extract type codes from entity metadata.
     */
    private static byte[] extractTypeCodes(io.memris.core.EntityMetadata<?> metadata) {
        var fields = metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .toList();
        byte[] typeCodes = new byte[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Byte tc = fields.get(i).typeCode();
            typeCodes[i] = tc != null ? tc.byteValue() : io.memris.core.TypeCodes.TYPE_LONG;
        }
        return typeCodes;
    }

    /**
     * Extract converters from entity metadata.
     */
    private static io.memris.core.converter.TypeConverter<?, ?>[] extractConverters(
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
    private static java.lang.invoke.MethodHandle[] extractSetters(io.memris.core.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .map(fm -> metadata.fieldSetters().get(fm.name()))
                .toArray(java.lang.invoke.MethodHandle[]::new);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> extractEntityClass(Class<? extends MemrisRepository<?>> repositoryInterface) {
        for (java.lang.reflect.Type iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof java.lang.reflect.ParameterizedType pt) {
                java.lang.reflect.Type[] typeArgs = pt.getActualTypeArguments();
                if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                    return (Class<T>) typeArgs[0];
                }
            }
        }
        throw new IllegalArgumentException("Cannot extract entity class from " + repositoryInterface);
    }

    /**
     * Emit and instantiate a repository implementation for the given interface.
     *
     * @param repositoryInterface the repository interface class
     * @param runtime             the repository runtime for method delegation
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return an instance of the generated repository implementation
     */
    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R emitAndInstantiate(Class<R> repositoryInterface,
            RepositoryRuntime<T> runtime) {
        try {
            // Generate the implementation class
            Class<? extends R> implClass = generateImplementation(repositoryInterface, runtime);

            // Instantiate using no-arg constructor
            return implClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to emit and instantiate repository for " + repositoryInterface.getName(),
                    e);
        }
    }

    /**
     * Generate the repository implementation class.
     */
    @SuppressWarnings("unchecked")
    private <T, R extends MemrisRepository<T>> Class<? extends R> generateImplementation(Class<R> repositoryInterface,
            RepositoryRuntime<T> runtime) {
        String implClassName = repositoryInterface.getName() + "_MemrisImpl_" + System.nanoTime();

        DynamicType.Builder<?> builder = byteBuddy
                .subclass(Object.class)
                .implement(repositoryInterface)
                .name(implClassName)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        Method[] queryMethods = RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);
        for (int i = 0; i < queryMethods.length; i++) {
            Method method = queryMethods[i];
            builder = builder.method(ElementMatchers.named(method.getName())
                    .and(ElementMatchers.takesArguments(method.getParameterCount())))
                    .intercept(MethodDelegation.to(new RepositoryMethodIndexInterceptor<>(runtime, i)));
        }

        try (DynamicType.Unloaded<?> unloaded = builder.make()) {
            return (Class<? extends R>) unloaded.load(repositoryInterface.getClassLoader()).getLoaded();
        }
    }

    /**
     * Method interceptor that delegates calls to RepositoryRuntime.
     */
    public static class RepositoryMethodInterceptor<T> {
        private final RepositoryRuntime<T> runtime;
        private final Method method;

        public RepositoryMethodInterceptor(RepositoryRuntime<T> runtime, Method method) {
            this.runtime = runtime;
            this.method = method;
        }

        @net.bytebuddy.implementation.bind.annotation.RuntimeType
        public Object intercept(
                @net.bytebuddy.implementation.bind.annotation.AllArguments Object[] args) {
            // Delegate to runtime based on method name and arguments
            return runtime.executeMethod(method, args);
        }
    }

    public static class RepositoryMethodIndexInterceptor<T> {
        private final RepositoryRuntime<T> runtime;
        private final int methodIndex;

        public RepositoryMethodIndexInterceptor(RepositoryRuntime<T> runtime, int methodIndex) {
            this.runtime = runtime;
            this.methodIndex = methodIndex;
        }

        @net.bytebuddy.implementation.bind.annotation.RuntimeType
        public Object intercept(
                @net.bytebuddy.implementation.bind.annotation.AllArguments Object[] args) {
            return runtime.executeMethodIndex(methodIndex, args);
        }
    }
}
