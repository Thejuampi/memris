package io.memris.query;

/**
 * Token types produced by QueryMethodLexer.
 * <p>
 * Unified token model for ALL repository methods:
 * <ul>
 *   <li>OPERATION - Built-in methods (save, findById, findAll, etc.)</li>
 *   <li>PROPERTY_PATH - Field names in derived queries</li>
 *   <li>OPERATOR - Comparison operators (GreaterThan, Like, etc.)</li>
 *   <li>AND/OR - Combinators</li>
 *   <li>ORDER_BY/ASC/DESC - Sorting</li>
 * </ul>
 * <p>
 * <b>Single Pipeline:</b> Every method goes through the lexer.
 * No special cases - CRUD is just another operation with a different opcode.
 */
public enum QueryMethodTokenType {
    // Operation token for built-in methods (save, findById, findAll, etc.)
    OPERATION,

    // Query result types (for derived query prefixes)
    FIND_BY,        // findByXxx(...)
    COUNT_BY,       // countByXxx(...)
    EXISTS_BY,      // existsByXxx(...)
    DELETE_BY,      // deleteByXxx(...)

    // Delete operation tokens (for delete/deleteAll)
    DELETE,         // delete(T entity)
    DELETE_ALL,     // deleteAll()

    // Predicates and operators
    PROPERTY_PATH,
    OPERATOR,
    AND,
    OR,
    ORDER_BY,
    ASC,
    DESC,
    IS_NULL,
    IS_NOT_NULL,
    TRUE,
    FALSE,
    IGNORE_CASE
}
