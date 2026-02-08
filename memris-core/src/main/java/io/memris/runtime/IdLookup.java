package io.memris.runtime;

import io.memris.storage.GeneratedTable;

/**
 * Type-specialized ID lookup strategy.
 * <p>
 * Generated at repository compilation time to eliminate runtime branching
 * on ID types (Long, Integer, String, etc.) in hot paths.
 */
@FunctionalInterface
public interface IdLookup {

    /**
     * Look up a row by its ID value.
     *
     * @param table the table to search
     * @param id    the ID value (type matches what was configured at generation)
     * @return packed reference (rowIndex + generation), or -1 if not found
     */
    long lookup(GeneratedTable table, Object id);
}
