package io.memris.spring.scaffold;

import io.memris.spring.MemrisRepository;
import io.memris.spring.runtime.RepositoryRuntime;
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

    public RepositoryEmitter() {
        this.byteBuddy = new ByteBuddy();
    }

    /**
     * Static factory method for creating repositories with arena scoping.
     * This is called by MemrisArena to create arena-scoped repositories.
     *
     * @param repositoryInterface the repository interface class
     * @param arena               the arena for scoping
     * @param <T>               the entity type
     * @param <R>               the repository interface type
     * @return an instance of the generated repository implementation
     */
    @SuppressWarnings("unchecked")
    public static <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface, 
                                                                        io.memris.spring.MemrisArena arena) {
        RepositoryEmitter emitter = new RepositoryEmitter();
        
        // Extract entity class and create/get table
        Class<T> entityClass = extractEntityClass(repositoryInterface);
        io.memris.storage.GeneratedTable table = arena.getOrCreateTable(entityClass);
        
        // Extract entity metadata
        io.memris.spring.EntityMetadata<T> metadata = 
            io.memris.spring.MetadataExtractor.extractEntityMetadata(entityClass);
        
        // Extract and compile query methods
        java.lang.reflect.Method[] methods = 
            RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);
        io.memris.spring.plan.CompiledQuery[] compiledQueries = 
            new io.memris.spring.plan.CompiledQuery[methods.length];
        
        io.memris.spring.plan.QueryCompiler compiler = new io.memris.spring.plan.QueryCompiler(metadata);
        
        for (int i = 0; i < methods.length; i++) {
            java.lang.reflect.Method method = methods[i];
            io.memris.spring.plan.LogicalQuery logicalQuery = io.memris.spring.plan.QueryPlanner.parse(method, entityClass, metadata.idColumnName());
            compiledQueries[i] = compiler.compile(logicalQuery);
        }

        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = buildJoinTables(metadata, arena);
        java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> kernelsByEntity = buildJoinKernels(tablesByEntity);
        java.util.Map<Class<?>, io.memris.spring.runtime.EntityMaterializer<?>> materializersByEntity = buildJoinMaterializers(tablesByEntity);
        compiledQueries = wireJoinRuntime(compiledQueries, metadata, tablesByEntity, kernelsByEntity, materializersByEntity);
        
        // Extract column metadata for RepositoryPlan
        String[] columnNames = extractColumnNames(metadata);
        byte[] typeCodes = extractTypeCodes(metadata);
        io.memris.spring.converter.TypeConverter<?, ?>[] converters = extractConverters(metadata);
        java.lang.invoke.MethodHandle[] setters = extractSetters(metadata);
        
        // Create entity constructor handle
        java.lang.invoke.MethodHandle entityConstructor;
        try {
            entityConstructor = java.lang.invoke.MethodHandles.lookup()
                    .unreflectConstructor(metadata.entityConstructor());
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }
        
        // Build RepositoryPlan with compiled queries
        io.memris.spring.runtime.RepositoryPlan<T> plan = 
            io.memris.spring.runtime.RepositoryPlan.fromGeneratedTable(
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
                materializersByEntity
        );
        
        // Create RepositoryRuntime
        io.memris.spring.runtime.RepositoryRuntime<T> runtime = 
                new io.memris.spring.runtime.RepositoryRuntime<>(plan, null, metadata);
        
        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    private static <T> java.util.Map<Class<?>, io.memris.storage.GeneratedTable> buildJoinTables(
        io.memris.spring.EntityMetadata<T> metadata,
        io.memris.spring.MemrisArena arena
    ) {
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity = new java.util.HashMap<>();
        tablesByEntity.put(metadata.entityClass(), arena.getOrCreateTable(metadata.entityClass()));

        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (!field.isRelationship() || field.targetEntity() == null) {
                continue;
            }
            Class<?> target = field.targetEntity();
            io.memris.storage.GeneratedTable table = arena.getOrCreateTable(target);
            tablesByEntity.putIfAbsent(target, table);
        }
        return java.util.Map.copyOf(tablesByEntity);
    }

    private static java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> buildJoinKernels(
        java.util.Map<Class<?>, io.memris.storage.GeneratedTable> tablesByEntity
    ) {
        java.util.Map<Class<?>, io.memris.spring.runtime.HeapRuntimeKernel> kernels = new java.util.HashMap<>();
        for (var entry : tablesByEntity.entrySet()) {
            kernels.put(entry.getKey(), new io.memris.spring.runtime.HeapRuntimeKernel(entry.getValue()));
        }
        return java.util.Map.copyOf(kernels);
    }

    private static java.util.Map<Class<?>, io.memris.spring.runtime.EntityMaterializer<?>> buildJoinMaterializers(
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

    private static <T> io.memris.spring.plan.CompiledQuery[] wireJoinRuntime(
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
     * Extract column names from entity metadata.
     */
    private static String[] extractColumnNames(io.memris.spring.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .map(io.memris.spring.EntityMetadata.FieldMapping::columnName)
                .toArray(String[]::new);
    }
    
    /**
     * Extract type codes from entity metadata.
     */
    private static byte[] extractTypeCodes(io.memris.spring.EntityMetadata<?> metadata) {
        var fields = metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .toList();
        byte[] typeCodes = new byte[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Byte tc = fields.get(i).typeCode();
            typeCodes[i] = tc != null ? tc.byteValue() : io.memris.spring.TypeCodes.TYPE_LONG;
        }
        return typeCodes;
    }
    
    /**
     * Extract converters from entity metadata.
     */
    private static io.memris.spring.converter.TypeConverter<?, ?>[] extractConverters(
            io.memris.spring.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
                .map(fm -> metadata.converters().getOrDefault(fm.name(), null))
                .toArray(io.memris.spring.converter.TypeConverter[]::new);
    }
    
    /**
     * Extract setter MethodHandles from entity metadata.
     */
    private static java.lang.invoke.MethodHandle[] extractSetters(io.memris.spring.EntityMetadata<?> metadata) {
        return metadata.fields().stream()
                .filter(fm -> fm.columnPosition() >= 0)
                .sorted(java.util.Comparator.comparingInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition))
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
     * @param <T>               the entity type
     * @param <R>               the repository interface type
     * @return an instance of the generated repository implementation
     */
    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R emitAndInstantiate(Class<R> repositoryInterface, RepositoryRuntime<T> runtime) {
        try {
            // Generate the implementation class
            Class<? extends R> implClass = generateImplementation(repositoryInterface, runtime);
            
            // Instantiate using no-arg constructor
            return implClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to emit and instantiate repository for " + repositoryInterface.getName(), e);
        }
    }

    /**
     * Generate the repository implementation class.
     */
    @SuppressWarnings("unchecked")
    private <T, R extends MemrisRepository<T>> Class<? extends R> generateImplementation(Class<R> repositoryInterface, RepositoryRuntime<T> runtime) {
        String implClassName = repositoryInterface.getName() + "_MemrisImpl_" + System.nanoTime();
        
        DynamicType.Builder<?> builder = byteBuddy
                .subclass(Object.class)
                .implement(repositoryInterface)
                .name(implClassName)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        // Intercept all methods from the repository interface
        for (Method method : repositoryInterface.getMethods()) {
            if (method.isDefault() || java.lang.reflect.Modifier.isStatic(method.getModifiers())) {
                continue; // Skip default and static methods
            }
            
            builder = builder.method(ElementMatchers.named(method.getName())
                    .and(ElementMatchers.takesArguments(method.getParameterCount())))
                    .intercept(MethodDelegation.to(new RepositoryMethodInterceptor<>(runtime, method)));
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
}
