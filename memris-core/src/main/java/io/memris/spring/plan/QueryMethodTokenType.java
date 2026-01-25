package io.memris.spring.plan;

public enum QueryMethodTokenType {
    // Query methods with conditions
    FIND_BY,
    READ_BY,
    QUERY_BY,
    COUNT_BY,
    EXISTS_BY,
    DELETE_BY,
    GET_BY,

    // CRUD operations (simple, no conditions)
    SAVE,           // save(T entity)
    SAVE_ALL,       // saveAll(List<T> entities)
    DELETE,         // delete(T entity)
    DELETE_ALL,     // deleteAll()

    // Query results (simple, no conditions)
    FIND_ALL,       // findAll()
    COUNT_ALL,      // count()

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
