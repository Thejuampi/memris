package io.memris.repository;

import io.memris.core.MemrisArena;
import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.storage.GeneratedTable;
import io.memris.storage.SimpleTable;
import io.memris.storage.SimpleTable.ColumnSpec;
import io.memris.query.*;
import io.memris.runtime.*;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Generates repository implementations using ByteBuddy.
 * <p>
 * Creates dynamic proxy classes that implement repository interfaces and
 * delegate method calls to the RepositoryRuntime.
 */
public final class RepositoryEmitter {

    private final ByteBuddy byteBuddy;

    /**
     * Creates a RepositoryEmitter with default configuration.
     */
    public RepositoryEmitter() {
        this.byteBuddy = new ByteBuddy();
    }

    /**
     * Static factory method for creating repositories with arena scoping.
     * This is called by MemrisArena to create arena-scoped repositories.
     *
     * @param repositoryInterface the repository interface class
     * @param arena               the arena for scoping
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return an instance of the generated repository implementation
     */
    public static <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface,
            MemrisArena arena) {
        RepositoryEmitter emitter = new RepositoryEmitter();

        // Extract entity class and create/get table
        Class<T> entityClass = extractEntityClass(repositoryInterface);
        GeneratedTable table = arena.getOrCreateTable(entityClass);

        // Extract entity metadata
        EntityMetadata<T> metadata = MetadataExtractor.extractEntityMetadata(entityClass);

        // Extract and compile query methods
        Method[] methods = RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);
        CompiledQuery[] compiledQueries = new CompiledQuery[methods.length];

        QueryCompiler compiler = new QueryCompiler(metadata);

        for (int i = 0; i < methods.length; i++) {
            Method method = methods[i];
            LogicalQuery logicalQuery = QueryPlanner.parse(method, entityClass,
                    metadata.idColumnName());
            compiledQueries[i] = compiler.compile(logicalQuery);
        }

        Map<Class<?>, GeneratedTable> tablesByEntity = buildJoinTables(metadata, arena);
        Map<Class<?>, HeapRuntimeKernel> kernelsByEntity = buildJoinKernels(tablesByEntity);
        Map<Class<?>, EntityMaterializer<?>> materializersByEntity = buildJoinMaterializers(
                tablesByEntity);
        Map<String, SimpleTable> joinTables = buildManyToManyJoinTables(metadata);
        compiledQueries = wireJoinRuntime(compiledQueries, metadata, tablesByEntity, kernelsByEntity,
                materializersByEntity, joinTables);

        // Extract column metadata for RepositoryPlan
        String[] columnNames = extractColumnNames(metadata);
        byte[] typeCodes = extractTypeCodes(metadata);
        TypeConverter<?, ?>[] converters = extractConverters(metadata);
        MethodHandle[] setters = extractSetters(metadata);

        // Create entity constructor handle
        MethodHandle entityConstructor;
        try {
            entityConstructor = MethodHandles.lookup()
                    .unreflectConstructor(metadata.entityConstructor());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }

        // Generate EntitySaver for the entity
        EntitySaver<T, ?> entitySaver = EntitySaverGenerator.generate(entityClass, metadata);

        // Build RepositoryPlan with compiled queries
        RepositoryPlan<T> plan = RepositoryPlan.fromGeneratedTable(
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
        RepositoryRuntime<T> runtime = new RepositoryRuntime<>(plan, null, metadata);

        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    private static <T> Map<Class<?>, GeneratedTable> buildJoinTables(
            EntityMetadata<T> metadata,
            MemrisArena arena) {
        Map<Class<?>, GeneratedTable> tablesByEntity = new HashMap<>();
        ArrayDeque<Class<?>> queue = new ArrayDeque<>();

        tablesByEntity.put(metadata.entityClass(), arena.getOrCreateTable(metadata.entityClass()));
        queue.add(metadata.entityClass());

        while (!queue.isEmpty()) {
            Class<?> current = queue.poll();
            EntityMetadata<?> currentMetadata = MetadataExtractor.extractEntityMetadata(current);
            for (FieldMapping field : currentMetadata.fields()) {
                if (!field.isRelationship() || field.targetEntity() == null) {
                    continue;
                }
                Class<?> target = field.targetEntity();
                if (tablesByEntity.containsKey(target)) {
                    continue;
                }
                GeneratedTable table = arena.getOrCreateTable(target);
                tablesByEntity.put(target, table);
                queue.add(target);
            }
        }
        return Map.copyOf(tablesByEntity);
    }

    private static Map<Class<?>, HeapRuntimeKernel> buildJoinKernels(
            Map<Class<?>, GeneratedTable> tablesByEntity) {
        Map<Class<?>, HeapRuntimeKernel> kernels = new HashMap<>();
        for (var entry : tablesByEntity.entrySet()) {
            kernels.put(entry.getKey(), new HeapRuntimeKernel(entry.getValue()));
        }
        return Map.copyOf(kernels);
    }

    private static Map<Class<?>, EntityMaterializer<?>> buildJoinMaterializers(
            Map<Class<?>, GeneratedTable> tablesByEntity) {
        Map<Class<?>, EntityMaterializer<?>> materializers = new HashMap<>();
        EntityMaterializerGenerator generator = new EntityMaterializerGenerator();
        for (Class<?> entityClass : tablesByEntity.keySet()) {
            EntityMetadata<?> entityMetadata = MetadataExtractor.extractEntityMetadata(entityClass);
            materializers.put(entityClass, generator.generate(entityMetadata));
        }
        return Map.copyOf(materializers);
    }

    private static <T> Map<String, SimpleTable> buildManyToManyJoinTables(
            EntityMetadata<T> metadata) {
        Map<String, SimpleTable> joinTables = new HashMap<>();
        for (FieldMapping field : metadata.fields()) {
            if (!field.isRelationship() || field.relationshipType() != FieldMapping.RelationshipType.MANY_TO_MANY) {
                continue;
            }
            if (field.joinTable() == null || field.joinTable().isBlank()) {
                continue;
            }

            EntityMetadata<?> targetMetadata = MetadataExtractor.extractEntityMetadata(field.targetEntity());
            FieldMapping sourceId = findIdField(metadata);
            FieldMapping targetId = findIdField(targetMetadata);

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
                List<ColumnSpec<?>> specs = List.of(
                        new ColumnSpec<>(finalJoinColumn, sourceId.javaType()),
                        new ColumnSpec<>(finalInverseJoinColumn, targetId.javaType()));
                return new SimpleTable(name, specs);
            });

        }
        return Map.copyOf(joinTables);
    }

    private static FieldMapping findIdField(EntityMetadata<?> metadata) {
        for (FieldMapping field : metadata.fields()) {
            if (field.name().equals(metadata.idColumnName())) {
                return field;
            }
        }
        return null;
    }

    private static <T> CompiledQuery[] wireJoinRuntime(
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
                EntityMetadata<?> targetMetadata = MetadataExtractor.extractEntityMetadata(join.targetEntity());
                MethodHandle postLoadHandle = targetMetadata.postLoadHandle();
                FieldMapping fieldMapping = findFieldMapping(metadata, join.relationshipFieldName());
                JoinExecutor executor = buildJoinExecutor(metadata, targetMetadata, join, fieldMapping, joinTables);
                MethodHandle setter = metadata.fieldSetters().get(join.relationshipFieldName());
                JoinMaterializer materializer = buildJoinMaterializer(fieldMapping, join, setter, postLoadHandle);
                updated[j] = join.withRuntime(targetTable, targetKernel, targetMaterializer, executor, materializer);
            }
            wired[i] = query.withJoins(updated);
        }
        return wired;
    }

    private static JoinExecutor buildJoinExecutor(EntityMetadata<?> metadata,
            EntityMetadata<?> targetMetadata,
            CompiledQuery.CompiledJoin join,
            FieldMapping fieldMapping,
            Map<String, SimpleTable> joinTables) {
        if (fieldMapping != null && fieldMapping.relationshipType() == FieldMapping.RelationshipType.MANY_TO_MANY) {
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

    private static JoinMaterializer buildJoinMaterializer(
            FieldMapping fieldMapping,
            CompiledQuery.CompiledJoin join,
            MethodHandle setter,
            MethodHandle postLoadHandle) {
        if (fieldMapping != null && fieldMapping.isCollection()) {
            if (fieldMapping.relationshipType() == FieldMapping.RelationshipType.MANY_TO_MANY) {
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

    private static JoinTableInfo resolveJoinTableInfo(EntityMetadata<?> sourceMetadata,
            FieldMapping field,
            Map<String, SimpleTable> joinTables) {
        if (field.joinTable() != null && !field.joinTable().isBlank()) {
            return buildJoinTableInfo(field, sourceMetadata,
                    MetadataExtractor.extractEntityMetadata(field.targetEntity()), false, joinTables);
        }
        if (field.mappedBy() != null && !field.mappedBy().isBlank()) {
            EntityMetadata<?> targetMetadata = MetadataExtractor.extractEntityMetadata(field.targetEntity());
            FieldMapping ownerField = findFieldMapping(targetMetadata, field.mappedBy());
            if (ownerField == null || ownerField.joinTable() == null || ownerField.joinTable().isBlank()) {
                return null;
            }
            return buildJoinTableInfo(ownerField, targetMetadata, sourceMetadata, true, joinTables);
        }
        return null;
    }

    private static JoinTableInfo buildJoinTableInfo(FieldMapping ownerField,
            EntityMetadata<?> ownerMetadata,
            EntityMetadata<?> inverseMetadata,
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

    private static FieldMapping findFieldMapping(EntityMetadata<?> metadata, String fieldName) {
        for (FieldMapping field : metadata.fields()) {
            if (field.name().equals(fieldName)) {
                return field;
            }
        }
        return null;
    }

    private static String[] extractColumnNames(EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(Comparator.comparingInt(FieldMapping::columnPosition))
                .map(FieldMapping::columnName)
                .toArray(String[]::new);
    }

    private static byte[] extractTypeCodes(EntityMetadata<?> metadata) {
        var fields = metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(Comparator.comparingInt(FieldMapping::columnPosition))
                .toList();
        byte[] typeCodes = new byte[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Byte tc = fields.get(i).typeCode();
            typeCodes[i] = tc != null ? tc.byteValue() : TypeCodes.TYPE_LONG;
        }
        return typeCodes;
    }

    private static TypeConverter<?, ?>[] extractConverters(EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(Comparator.comparingInt(FieldMapping::columnPosition))
                .map(fm -> metadata.converters().getOrDefault(fm.name(), null))
                .toArray(TypeConverter[]::new);
    }

    private static MethodHandle[] extractSetters(EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(Comparator.comparingInt(FieldMapping::columnPosition))
                .map(fm -> metadata.fieldSetters().get(fm.name()))
                .toArray(MethodHandle[]::new);
    }

    @SuppressWarnings("unchecked")
    private static <T> Class<T> extractEntityClass(Class<? extends MemrisRepository<?>> repositoryInterface) {
        for (Type iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType pt) {
                Type[] typeArgs = pt.getActualTypeArguments();
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

    private record JoinTableInfo(String joinColumn, String inverseJoinColumn, SimpleTable table) {
    }
}
