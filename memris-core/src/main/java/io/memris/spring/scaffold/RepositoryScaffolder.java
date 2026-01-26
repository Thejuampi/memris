package io.memris.spring.scaffold;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
import java.lang.invoke.MethodHandles;
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
import io.memris.spring.runtime.EntityExtractor;
import io.memris.spring.runtime.EntityMaterializer;
import io.memris.spring.runtime.RepositoryPlan;
import io.memris.spring.runtime.RepositoryRuntime;
import io.memris.spring.runtime.RuntimeKernel;
import io.memris.storage.ffm.FfmTable;

/**
 * Orchestrates repository creation.
 * <p>
 * This is the build-time orchestrator that:
 * 1. Extracts metadata from entity class
 * 2. Extracts query methods from repository interface (deterministic ordering)
 * 3. Plans and compiles queries
 * 4. Builds RuntimeKernel from entity metadata
 * 5. Builds RepositoryPlan with all compiled components
 * 6. Creates RepositoryRuntime from the plan
 * 7. Generates repository implementation class
 * 8. Instantiates and returns the repository
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

        // Step 2: Extract query methods from repository interface (deterministic ordering)
        Method[] queryMethods = RepositoryMethodIntrospector.extractQueryMethods(repositoryInterface);

        // Step 3: Plan and compile queries
        List<LogicalQuery> logicalQueries = new ArrayList<>();
        List<CompiledQuery> compiledQueries = new ArrayList<>();

        QueryCompiler compiler = new QueryCompiler(metadata);

        for (Method method : queryMethods) {
            // TODO: Future iteration - pass pre-processed record containing entity structure
            // instead of accessing entityClass directly via reflection
            LogicalQuery logical = QueryPlanner.parse(method, entityClass, metadata.idColumnName());
            CompiledQuery compiled = compiler.compile(logical);

            logicalQueries.add(logical);
            compiledQueries.add(compiled);
        }

        // Step 4: Build RuntimeKernel from entity metadata
        RuntimeKernel kernel = buildRuntimeKernel(table, metadata);

        // Step 5: Build EntityMaterializer and EntityExtractor
        EntityMaterializer<T> materializer = buildMaterializer(entityClass, metadata);
        EntityExtractor<T> extractor = buildExtractor(entityClass, metadata);

        // Step 6: Build RepositoryPlan (single compiled artifact)
        RepositoryPlan<T> plan = RepositoryPlan.<T>builder(entityClass)
                .queries(compiledQueries.toArray(new CompiledQuery[0]))
                .kernel(kernel)
                .materializer(materializer)
                .extractor(extractor)
                .build();

        // Step 7: Create RepositoryRuntime from the plan
        RepositoryRuntime<T> runtime = new RepositoryRuntime<>(plan, factory);

        // Step 8: Generate and instantiate repository implementation
        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }

    /**
     * Build RuntimeKernel from entity metadata.
     * <p>
     * Creates index-based column accessors for zero-reflection hot path execution.
     */
    private RuntimeKernel buildRuntimeKernel(FfmTable table, EntityMetadata<T> metadata) {
        int fieldCount = metadata.fields().size();
        String[] columnNames = new String[fieldCount];
        byte[] typeCodes = new byte[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            EntityMetadata.FieldMapping fm = metadata.fields().get(i);
            columnNames[i] = fm.columnName();
            typeCodes[i] = (byte) fm.typeCode();
        }

        return RuntimeKernel.fromLegacy(table, columnNames, typeCodes);
    }

    /**
     * Build EntityMaterializer for entity construction.
     * <p>
     * Uses MethodHandles for zero-reflection materialization.
     * TODO: Generate bytecode with ByteBuddy for true zero-reflection.
     */
    @SuppressWarnings("unchecked")
    private EntityMaterializer<T> buildMaterializer(Class<T> entityClass, EntityMetadata<T> metadata) {
        // Get entity constructor
        MethodHandle constructor;
        try {
            Constructor<T> ctor = entityClass.getDeclaredConstructor();
            ctor.setAccessible(true);
            constructor = MethodHandles.lookup().unreflectConstructor(ctor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to get entity constructor", e);
        }

        // Get setters array
        int fieldCount = metadata.fields().size();
        MethodHandle[] setters = new MethodHandle[fieldCount];
        TypeConverter<?, ?>[] converters = new TypeConverter<?, ?>[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            EntityMetadata.FieldMapping fm = metadata.fields().get(i);
            setters[i] = metadata.fieldSetters().get(fm.name());
            converters[i] = metadata.converters().get(fm.name());
        }

        // Create materializer that uses kernel directly
        return (kernel, rowIndex) -> {
            try {
                T entity = (T) constructor.invoke();
                for (int i = 0; i < fieldCount; i++) {
                    RuntimeKernel.FfmColumnAccessor accessor = kernel.columnAt(i);
                    Object value = accessor.getValue(rowIndex);
                    if (converters[i] != null) {
                        value = ((TypeConverter<Object, Object>) converters[i]).fromStorage(value);
                    }
                    if (setters[i] != null) {
                        setters[i].invoke(entity, value);
                    }
                }
                return entity;
            } catch (Throwable e) {
                throw new RuntimeException("Failed to materialize entity", e);
            }
        };
    }

    /**
     * Build EntityExtractor for field extraction.
     * <p>
     * Uses reflection for now (TODO: generate bytecode with ByteBuddy).
     * TODO: Use MethodHandles.privateLookupIn() for nestmate access to private fields.
     */
    private EntityExtractor<T> buildExtractor(Class<T> entityClass, EntityMetadata<T> metadata) {
        int fieldCount = metadata.fields().size();
        String[] columnNames = new String[fieldCount];
        TypeConverter<?, ?>[] converters = new TypeConverter<?, ?>[fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            EntityMetadata.FieldMapping fm = metadata.fields().get(i);
            columnNames[i] = fm.columnName();
            converters[i] = metadata.converters().get(fm.name());
        }

        return entity -> {
            Object[] values = new Object[fieldCount];
            for (int i = 0; i < fieldCount; i++) {
                try {
                    // For now, use reflection to read field values
                    // TODO: Replace with proper pre-bound handles using privateLookupIn
                    java.lang.reflect.Field field = entityClass.getDeclaredField(columnNames[i]);
                    field.setAccessible(true);
                    Object value = field.get(entity);
                    if (converters[i] != null) {
                        value = ((TypeConverter<Object, Object>) converters[i]).toStorage(value);
                    }
                    values[i] = value;
                } catch (Throwable e) {
                    throw new RuntimeException("Failed to extract field", e);
                }
            }
            return values;
        };
    }
}
