package io.memris.spring;

import io.memris.kernel.Predicate;
import io.memris.spring.QueryMethodParser;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Metadata for a query method parsed at repository creation time.
 * All parsing happens once, then this metadata drives bytecode generation.
 */
public final class QueryMetadata {
    final Method method;
    final String methodName;
    final Class<?> returnType;
    final QueryMethodParser.QueryType queryType;
    final Integer limit;
    final boolean isDistinct;
    final List<Condition> conditions;
    final List<QueryMethodParser.OrderBy> orders;

    public QueryMetadata(
            Method method,
            String methodName,
            Class<?> returnType,
            QueryMethodParser.QueryType queryType,
            Integer limit,
            boolean isDistinct,
            List<Condition> conditions,
            List<QueryMethodParser.OrderBy> orders) {
        this.method = method;
        this.methodName = methodName;
        this.returnType = returnType;
        this.queryType = queryType;
        this.limit = limit;
        this.isDistinct = isDistinct;
        this.conditions = conditions;
        this.orders = orders;
    }

    public Method method() {
        return method;
    }

    public String methodName() {
        return methodName;
    }

    public Class<?> returnType() {
        return returnType;
    }

    public QueryMethodParser.QueryType queryType() {
        return queryType;
    }

    public Integer limit() {
        return limit;
    }

    public boolean isDistinct() {
        return isDistinct;
    }

    public List<Condition> conditions() {
        return conditions;
    }

    public List<QueryMethodParser.OrderBy> orders() {
        return orders;
    }

    /**
     * Single condition in a query.
     * e.g., "name = ?" or "price > ?"
     */
    public record Condition(
            String columnName,      // Column name (e.g., "name")
            Predicate.Operator operator,  // Operator (e.g., EQ, GT)
            int parameterIndex       // Index in method parameters (0-based)
    ) {}
}
