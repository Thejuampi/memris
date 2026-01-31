package io.memris.query;

/**
 * Operation codes for repository methods.
 * <p>
 * Unified enumeration for ALL repository operations - both built-in CRUD
 * and derived queries. This enables a single pipeline:
 * Lexer → Planner → Compiler → Executor
 * <p>
 * No special cases - CRUD is just another operation type with a different opcode.
 */
public enum OpCode {
    // Built-in CRUD operations
    SAVE_ONE,       // save(T entity)
    SAVE_ALL,       // saveAll(Iterable<T> entities)
    DELETE_ONE,     // delete(T entity)
    DELETE_ALL,     // deleteAll()
    DELETE_BY_ID,   // deleteById(ID id)

    // Built-in query operations
    FIND_BY_ID,     // findById(ID id)
    FIND_ALL,       // findAll()
    FIND_ALL_BY_ID, // findAllById(Iterable<ID>) - reserved for Spring Data compatibility
    EXISTS_BY_ID,   // existsById(ID id)
    COUNT_ALL,      // count()

    // Reserved delete operations
    DELETE_ALL_BY_ID, // deleteAllById(Iterable<ID>) - reserved for Spring Data compatibility

    // Derived query operations (findByXxx, countByXxx, existsByXxx, deleteByXxx)
    FIND,           // findByXxx(...), findByXxxAndYyy(...)
    COUNT,          // countByXxx(...)
    EXISTS,         // existsByXxx(...)
    DELETE_QUERY,   // deleteByXxx(...)
    UPDATE_QUERY    // @Query update
}
