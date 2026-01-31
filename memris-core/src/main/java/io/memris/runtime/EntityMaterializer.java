package io.memris.runtime;

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
 * <b>IMPORTANT:</b> The materializer reads from RuntimeKernel directly.
 * This avoids Object[] allocations, boxing, and unnecessary copying.
 * RepositoryRuntime orchestrates but does NOT perform entity-specific logic.
 *
 * @param <T> the entity type
 */
@FunctionalInterface
public interface EntityMaterializer<T> {

    /**
     * Materialize an entity from a single row using the kernel.
     * <p>
     * This is a hot-path method. Implementations MUST:
     * <ul>
     *   <li>Use kernel.columnAt(int) for column access</li>
     *   <li>Use kernel.getInt(colIdx, row) for primitive access</li>
     *   <li>Use pre-compiled MethodHandles for field setting</li>
     *   <li>Apply TypeConverters from the entity metadata</li>
     * </ul>
     * <p>
     * <b>Entity-specific logic lives here, NOT in RepositoryRuntime.</b>
     *
     * @param kernel   the runtime kernel for index-based column access
     * @param rowIndex the row index to materialize
     * @return the materialized entity
     */
    T materialize(HeapRuntimeKernel kernel, int rowIndex);
}
