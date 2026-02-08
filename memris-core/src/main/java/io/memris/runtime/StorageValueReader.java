package io.memris.runtime;

import io.memris.storage.GeneratedTable;

/**
 * Type-specialized storage value reader.
 * <p>
 * Generated at repository compilation time to eliminate runtime branching
 * on type codes when reading raw storage values.
 */
@FunctionalInterface
public interface StorageValueReader {

    /**
     * Read a storage value from the table.
     *
     * @param table    the table to read from
     * @param rowIndex the row index
     * @return the raw storage value (Integer, Long, or String depending on type)
     */
    Object read(GeneratedTable table, int rowIndex);
}
