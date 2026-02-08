package io.memris.runtime;

import io.memris.storage.GeneratedTable;

/**
 * EntityMaterializer constructs entity instances from table row data.
 * <p>
 * Implementations are generated at build-time as bytecode, providing:
 * <ul>
 *   <li>Zero reflection - uses pre-compiled MethodHandles</li>
 *   <li>Direct field access - no Map lookups</li>
 *   <li>Type conversion - applies TypeConverters where needed</li>
 * </ul>
 * <p>
 * <b>IMPORTANT:</b> The materializer reads from GeneratedTable directly.
 * This avoids Object[] allocations, boxing, and unnecessary copying.
 * RepositoryRuntime orchestrates but does NOT perform entity-specific logic.
 *
 * @param <T> the entity type
 */
@FunctionalInterface
public interface EntityMaterializer<T> {

    /**
     * Materialize an entity from a single row using the table.
     * <p>
     * This is a hot-path method. Implementations MUST:
     * <ul>
     *   <li>Use typed table reads for primitive/string access</li>
     *   <li>Apply TypeConverters from the entity metadata</li>
     * </ul>
     * <p>
     * <b>Entity-specific logic lives here, NOT in RepositoryRuntime.</b>
     *
     * @param table    the generated table
     * @param rowIndex the row index to materialize
     * @return the materialized entity
     */
    T materialize(GeneratedTable table, int rowIndex);

    /**
     * Compatibility helper for call sites that still have the kernel.
     */
    default T materialize(HeapRuntimeKernel kernel, int rowIndex) {
        return materialize(kernel.table(), rowIndex);
    }
}
