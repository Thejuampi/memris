package io.memris.spring.scaffold;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import io.memris.spring.EntityMetadata;
import io.memris.spring.MemrisRepositoryFactory;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.LogicalQuery;
import io.memris.spring.plan.QueryCompiler;
import io.memris.spring.plan.QueryPlanner;
import io.memris.spring.runtime.RepositoryRuntime;
import io.memris.storage.ffm.FfmTable;

/**
 * Orchestrates repository creation.
 * <p>
 * This is the build-time orchestrator that:
 * 1. Extracts metadata from entity class
 * 2. Extracts query methods from repository interface
 * 3. Plans and compiles queries
 * 4. Builds RepositoryRuntime
 * 5. Generates repository implementation class
 * 6. Instantiates and returns the repository
 * <p>
 * The generated repository is a thin wrapper that delegates all queries
 * to the RepositoryRuntime via typed entrypoints with constant queryId.
 *
 * @param <T> the entity type
 * @param <R> the repository type
 */
public final class RepositoryScaffolder<T, R> {

    private final MemrisRepositoryFactory factory;
    private final RepositoryEmitter emitter;

    public RepositoryScaffolder(MemrisRepositoryFactory factory) {
        this.factory = factory;
        this.emitter = new RepositoryEmitter();
    }

    /**
     * Create a repository instance.
     * <p>
     * This is the main entrypoint for repository creation.
     */
    @SuppressWarnings("unchecked")
    public R createRepository(
            Class<R> repositoryInterface,
            Class<T> entityClass,
            FfmTable table) {

        // Step 1: Extract entity metadata (one-time reflection)
        EntityMetadata<T> metadata = io.memris.spring.MetadataExtractor.extractEntityMetadata(entityClass, table);

        // Step 2: Extract query methods from repository interface
        Method[] queryMethods = extractQueryMethods(repositoryInterface);

        // Step 3: Plan and compile queries
        List<LogicalQuery> logicalQueries = new ArrayList<>();
        List<CompiledQuery> compiledQueries = new ArrayList<>();

        QueryCompiler compiler = new QueryCompiler(metadata);

        for (Method method : queryMethods) {
            LogicalQuery logical = QueryPlanner.parse(method, metadata.idColumnName());
            CompiledQuery compiled = compiler.compile(logical);

            logicalQueries.add(logical);
            compiledQueries.add(compiled);
        }

        // Step 4: Build RepositoryRuntime
        RepositoryRuntime<T> runtime = buildRuntime(
            table, factory, entityClass, metadata,
            compiledQueries.toArray(new CompiledQuery[0]));

        // Step 5: Generate and instantiate repository implementation
        return (R) emitter.emitAndInstantiate(
            repositoryInterface,
            entityClass,
            runtime,
            compiledQueries.toArray(new CompiledQuery[0]));
    }

    /**
     * Build the RepositoryRuntime with dense arrays for zero-reflection execution.
     */
    private RepositoryRuntime<T> buildRuntime(
            FfmTable table,
            MemrisRepositoryFactory factory,
            Class<T> entityClass,
            EntityMetadata<T> metadata,
            CompiledQuery[] compiledQueries) {

        // Extract dense arrays from metadata
        int fieldCount = metadata.fields().size();
        String[] columnNames = new String[fieldCount];
        byte[] typeCodes = new byte[fieldCount];
        TypeConverter<?, ?>[] converters =
            new TypeConverter<?, ?>[fieldCount];
        java.lang.invoke.MethodHandle[] setters = new java.lang.invoke.MethodHandle[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            EntityMetadata.FieldMapping fm = metadata.fields().get(i);
            columnNames[i] = fm.columnName();
            typeCodes[i] = (byte) fm.typeCode();

            // Get converter from map (may be null)
            converters[i] = metadata.converters().get(fm.name());

            // Get setter from fieldSetters map (may be null for records)
            setters[i] = metadata.fieldSetters().get(fm.name());
        }

        // Get entity constructor
        java.lang.invoke.MethodHandle constructor;
        try {
            Constructor<T> ctor = entityClass.getDeclaredConstructor();
            ctor.setAccessible(true);
            constructor = java.lang.invoke.MethodHandles.lookup().unreflectConstructor(ctor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }

        return new RepositoryRuntime<>(
            table,
            factory,
            entityClass,
            metadata.idColumnName(),
            compiledQueries,
            constructor,
            columnNames,
            typeCodes,
            converters,
            setters
        );
    }

    /**
     * Extract query methods from repository interface.
     */
    private Method[] extractQueryMethods(Class<?> repositoryInterface) {
        java.util.List<Method> methods = new ArrayList<>();

        for (Method method : repositoryInterface.getDeclaredMethods()) {
            String name = method.getName();

            // Skip write methods for now (they still use interceptors)
            if (name.startsWith("save") ||
                name.startsWith("update") ||
                name.startsWith("delete") ||
                name.startsWith("insert")) {
                continue;
            }

            // Include query methods
            methods.add(method);
        }

        return methods.toArray(new Method[0]);
    }
}
