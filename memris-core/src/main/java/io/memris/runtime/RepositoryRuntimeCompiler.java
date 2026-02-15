package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadataProvider;
import io.memris.core.MetadataExtractor;
import io.memris.core.MemrisArena;
import io.memris.query.CompiledQuery;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;

import java.util.IdentityHashMap;

/**
 * Compiles runtime artifacts once during repository creation.
 */
public final class RepositoryRuntimeCompiler {

    private RepositoryRuntimeCompiler() {
    }

    public static <T> CompiledRuntimeArtifacts compile(
            RepositoryPlan<T> plan,
            MemrisRepositoryFactory factory,
            MemrisArena arena,
            EntityMetadata<T> metadata,
            EntityMetadataProvider metadataProvider) {
        var queries = plan.queries();
        var queryIndexByInstance = buildQueryIndex(queries);
        var storageReadersByColumn = compileStorageReaders(plan);
        var conditionExecutorsByQuery = compileConditionExecutors(plan, metadata, factory, arena);
        var projectionExecutorsByQuery = compileProjectionExecutors(queries, metadataProvider);
        return new CompiledRuntimeArtifacts(
                storageReadersByColumn,
                conditionExecutorsByQuery,
                projectionExecutorsByQuery,
                queryIndexByInstance);
    }

    private static StorageValueReader[] compileStorageReaders(RepositoryPlan<?> plan) {
        var table = plan.table();
        var typeCodes = plan.typeCodes();

        if (table != null) {
            int columnCount = table.columnCount();
            if (columnCount == 0) {
                return new StorageValueReader[0];
            }
            var readers = new StorageValueReader[columnCount];
            for (var i = 0; i < columnCount; i++) {
                var typeCode = typeCodes != null && i < typeCodes.length ? typeCodes[i] : table.typeCodeAt(i);
                readers[i] = RuntimeExecutorGenerator.generateStorageValueReader(i, typeCode);
            }
            return readers;
        } else if (typeCodes != null && typeCodes.length > 0) {
            int columnCount = typeCodes.length;
            var readers = new StorageValueReader[columnCount];
            for (var i = 0; i < columnCount; i++) {
                readers[i] = RuntimeExecutorGenerator.generateStorageValueReader(i, typeCodes[i]);
            }
            return readers;
        } else {
            return new StorageValueReader[0];
        }
    }

    private static <T> ConditionExecutor[][] compileConditionExecutors(
            RepositoryPlan<T> plan,
            EntityMetadata<T> metadata,
            MemrisRepositoryFactory factory,
            MemrisArena arena) {
        var queries = plan.queries();
        if (queries == null || queries.length == 0) {
            return new ConditionExecutor[0][];
        }
        var columnNames = plan.columnNames() != null ? plan.columnNames() : new String[0];
        var primitiveNonNull = buildPrimitiveNonNullColumns(metadata, columnNames.length);
        var entityClass = metadata != null ? metadata.entityClass() : plan.entityClass();
        var useIndex = (factory != null || arena != null) && entityClass != null;
        return RepositoryRuntime.buildConditionExecutors(
                queries,
                columnNames,
                primitiveNonNull,
                entityClass != null ? entityClass : Object.class,
                useIndex);
    }

    private static boolean[] buildPrimitiveNonNullColumns(EntityMetadata<?> metadata, int columnCount) {
        var primitive = new boolean[Math.max(columnCount, 0)];
        if (metadata == null) {
            return primitive;
        }
        for (var field : metadata.fields()) {
            var columnPosition = field.columnPosition();
            if (columnPosition >= 0
                    && columnPosition < primitive.length
                    && field.javaType().isPrimitive()) {
                primitive[columnPosition] = true;
            }
        }
        return primitive;
    }

    private static ProjectionExecutor[] compileProjectionExecutors(
            CompiledQuery[] queries,
            EntityMetadataProvider metadataProvider) {
        if (queries == null || queries.length == 0) {
            return new ProjectionExecutor[0];
        }
        EntityMetadataProvider provider = metadataProvider != null
                ? metadataProvider
                : MetadataExtractor::extractEntityMetadata;
        return RepositoryRuntime.buildProjectionExecutors(queries, provider);
    }

    private static IdentityHashMap<CompiledQuery, Integer> buildQueryIndex(CompiledQuery[] queries) {
        var index = new IdentityHashMap<CompiledQuery, Integer>();
        if (queries == null) {
            return index;
        }
        for (var i = 0; i < queries.length; i++) {
            index.put(queries[i], i);
        }
        return index;
    }

    public record CompiledRuntimeArtifacts(
            StorageValueReader[] storageReadersByColumn,
            ConditionExecutor[][] conditionExecutorsByQuery,
            ProjectionExecutor[] projectionExecutorsByQuery,
            IdentityHashMap<CompiledQuery, Integer> queryIndexByInstance) {
    }
}
