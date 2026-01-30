package io.memris.spring.runtime;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.spring.plan.LogicalQuery;

/**
 * Interface for handling query operations on a specific data type.
 * 
 * <p>This interface defines the contract for type-specific query execution,
 * allowing the system to be extended with new types without modifying the core kernel.
 * 
 * <p>Implementations should handle:
 * <ul>
 *   <li>Equality and inequality comparisons</li>
 *   <li>Range comparisons (greater than, less than, etc.)</li>
 *   <li>Set operations (in, not in)</li>
 *   <li>Type-specific operations (like, containing, etc.)</li>
 * </ul>
 * 
 * @param <T> the Java type this handler supports (e.g., Long, String, Integer)
 */
public interface TypeHandler<T> {
    
    /**
     * Execute a condition on the given column with the specified operator and value.
     * 
     * @param table the table to scan
     * @param columnIndex the column index to scan
     * @param operator the comparison operator
     * @param value the value to compare against (may be null for IS_NULL checks)
     * @param ignoreCase whether to ignore case (for string comparisons)
     * @return a selection of matching row indices
     */
    Selection executeCondition(GeneratedTable table, int columnIndex, 
                               LogicalQuery.Operator operator, T value, boolean ignoreCase);
    
    /**
     * Get the type code this handler supports.
     * Should match one of the constants in {@link io.memris.spring.TypeCodes}.
     * 
     * @return the type code
     */
    byte getTypeCode();
    
    /**
     * Get the Java class this handler supports.
     * 
     * @return the Java class
     */
    Class<T> getJavaType();
    
    /**
     * Convert a value from the argument array to the handler's type.
     * This handles cases where the argument might be a different but compatible type.
     * 
     * @param value the value to convert
     * @return the converted value
     * @throws IllegalArgumentException if the value cannot be converted
     */
    T convertValue(Object value);
}
