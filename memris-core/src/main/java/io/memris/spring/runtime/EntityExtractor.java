package io.memris.spring.runtime;

/**
 * EntityExtractor extracts field values from entity instances for storage.
 * <p>
 * Implementations are generated at build-time as bytecode, providing:
 * <ul>
 *   <li>Zero reflection - uses pre-compiled MethodHandles</li>
 *   <li>Direct field access - no Map lookups</li>
 *   <li>Type conversion - applies TypeConverters where needed</li>
 * </ul>
 *
 * @param <T> the entity type
 */
@FunctionalInterface
public interface EntityExtractor<T> {

    /**
     * Extract field values from an entity for storage.
     * <p>
     * This is a hot-path method for save/update operations.
     * Implementations MUST:
     * <ul>
     *   <li>Use pre-compiled MethodHandles for field getting</li>
     *   <li>Apply TypeConverters for storage format</li>
     *   <li>Return values in column index order</li>
     * </ul>
     *
     * @param entity the entity to extract from
     * @return array of field values in column index order
     */
    Object[] extract(T entity);
}
