package io.memris.spring.plan;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Parses Spring Data JPA query method names into LogicalQuery.
 * <p>
 * This is a build-time component that parses method names once during
 * repository creation. The output (LogicalQuery) is then compiled
 * to CompiledQuery for execution.
 * <p>
 * Uses context-aware QueryMethodLexer to support entity metadata,
 * nested properties, and Spring Data JPA specification compliance.
 * <p>
 * Supported patterns:
 * - findById → ReturnKind.ONE_OPTIONAL, Condition(id, EQ, arg0)
 * - findAll → ReturnKind.MANY_LIST, no conditions
 * - findByXxx → ReturnKind.MANY_LIST, Condition(xxx, EQ, arg0)
 * - findByXxxAndYyy → ReturnKind.MANY_LIST, Condition(xxx, EQ, arg0), Condition(yyy, EQ, arg1)
 * - findByXxxOrYyy → ReturnKind.MANY_LIST, OR conditions
 * - countByXxx → ReturnKind.COUNT_LONG, Condition(xxx, EQ, arg0)
 * - existsById → ReturnKind.EXISTS_BOOL, Condition(id, EQ, arg0)
 * - findByXxxGreaterThan → ReturnKind.MANY_LIST, Condition(xxx, GT, arg0)
 * - findByXxxOrderByYyy → ReturnKind.MANY_LIST, OrderBy(yyy, ASC)
 * <p>
 * Supported operators:
 * - Comparison: GreaterThan, LessThan, GreaterThanEqual, LessThanEqual, Between
 * - Equality: Equals, NotEqual, IsNull, NotNull, In, NotIn
 * - String: Like, NotLike, StartingWith, EndingWith, Containing
 * - Boolean: True, False
 * - Date: Before, After
 * - Modifiers: IgnoreCase (applies to string operators)
 * - Combinators: And, Or
 * - Sorting: OrderBy, Asc, Desc
 *
 * @see LogicalQuery
 * @see QueryMethodLexer
 * @see QueryCompiler
 */
public final class QueryPlanner {

    private QueryPlanner() {
        // Utility class
    }

    /**
     * Parse a repository method into a LogicalQuery.
     *
     * @param method repository method to parse
     * @param entityClass entity class for context-aware property resolution
     * @param idColumnName name of ID column (e.g., "id")
     * @return a LogicalQuery representing the semantic meaning of the method
     */
    public static LogicalQuery parse(Method method, Class<?> entityClass, String idColumnName) {
        String methodName = method.getName();
        Class<?> returnType = method.getReturnType();
        Class<?>[] paramTypes = method.getParameterTypes();

        LogicalQuery.ReturnKind returnKind = determineReturnKind(methodName, returnType, paramTypes.length);
        List<LogicalQuery.Condition> conditions = new ArrayList<>();
        LogicalQuery.OrderBy orderBy = null;

        // Tokenize using context-aware lexer
        List<QueryMethodToken> tokens = QueryMethodLexer.tokenize(entityClass, methodName);

        // Process tokens
        int argIndex = 0;
        String pendingOperatorValue = null;
        boolean pendingIgnoreCase = false;
        String pendingProperty = null;
        boolean inOrderBy = false;
        Boolean orderByDirection = null;

        for (QueryMethodToken token : tokens) {
            switch (token.type()) {
                case PROPERTY_PATH -> {
                    if (inOrderBy) {
                        pendingProperty = token.value();
                    } else if (pendingProperty != null) {
                        // We have an operator waiting for its property
                        finalizeCondition(pendingProperty, pendingOperatorValue, pendingIgnoreCase, argIndex++, conditions);
                        pendingProperty = token.value();
                        pendingOperatorValue = null;
                        pendingIgnoreCase = false;
                    } else {
                        // New property, no operator yet (will default to EQ)
                        pendingProperty = token.value();
                    }
                }
                case OPERATOR -> {
                    String value = token.value();
                    if (value.equals("OrderBy")) {
                        inOrderBy = true;
                        if (pendingProperty != null) {
                            finalizeCondition(pendingProperty, pendingOperatorValue, pendingIgnoreCase, argIndex++, conditions);
                            pendingProperty = null;
                            pendingOperatorValue = null;
                            pendingIgnoreCase = false;
                        }
                    } else if (value.equals("IgnoreCase")) {
                        pendingIgnoreCase = true;
                    } else {
                        pendingOperatorValue = value;
                    }
                }
                case AND, OR -> {
                    if (pendingProperty != null) {
                        finalizeCondition(pendingProperty, pendingOperatorValue, pendingIgnoreCase, argIndex++, conditions);
                        pendingProperty = null;
                        pendingOperatorValue = null;
                        pendingIgnoreCase = false;
                    }
                }
                case ASC -> {
                    orderByDirection = true;
                }
                case DESC -> {
                    orderByDirection = false;
                }
                case FIND_BY, COUNT, EXISTS_BY, DELETE_BY, FIND_ALL -> {
                    // Prefix tokens - skip
                }
            }
        }

        // Finalize last condition if pending
        if (pendingProperty != null && !inOrderBy && pendingProperty != null) {
            finalizeCondition(pendingProperty, pendingOperatorValue, pendingIgnoreCase, argIndex++, conditions);
        }

        // Finalize OrderBy
        if (inOrderBy && pendingProperty != null) {
            if (orderByDirection != null) {
                orderBy = LogicalQuery.OrderBy.of(pendingProperty, orderByDirection);
            } else {
                orderBy = LogicalQuery.OrderBy.asc(pendingProperty);
            }
        }

        return LogicalQuery.of(methodName, returnKind,
                               conditions.toArray(new LogicalQuery.Condition[0]),
                               orderBy);
    }

    private static void finalizeCondition(String propertyPath, String operatorValue,
                                       boolean ignoreCase, int argIndex,
                                       List<LogicalQuery.Condition> conditions) {
        LogicalQuery.Operator operator = (operatorValue == null) ? LogicalQuery.Operator.EQ
                                                             : mapToOperator(operatorValue, ignoreCase);
        conditions.add(LogicalQuery.Condition.of(propertyPath, operator, argIndex, ignoreCase));
    }

    private static LogicalQuery.ReturnKind determineReturnKind(String methodName, Class<?> returnType, int paramCount) {
        if (methodName.startsWith("find")) {
            if (returnType.equals(java.util.Optional.class)) {
                return LogicalQuery.ReturnKind.ONE_OPTIONAL;
            } else {
                return LogicalQuery.ReturnKind.MANY_LIST;
            }
        } else if (methodName.startsWith("count")) {
            return LogicalQuery.ReturnKind.COUNT_LONG;
        } else if (methodName.startsWith("exists")) {
            return LogicalQuery.ReturnKind.EXISTS_BOOL;
        } else if (methodName.startsWith("delete")) {
            return LogicalQuery.ReturnKind.MANY_LIST;
        } else if (methodName.equals("findAll")) {
            return LogicalQuery.ReturnKind.MANY_LIST;
        }
        throw new IllegalArgumentException("Cannot determine return kind for: " + methodName);
    }

    private static LogicalQuery.Operator mapToOperator(String operator, boolean ignoreCase) {
        return switch (operator) {
            case "Equals", "Is" -> ignoreCase ? LogicalQuery.Operator.IGNORE_CASE_EQ : LogicalQuery.Operator.EQ;
            case "NotEqual", "Not" -> LogicalQuery.Operator.NE;
            case "GreaterThan" -> LogicalQuery.Operator.GT;
            case "GreaterThanEqual" -> LogicalQuery.Operator.GTE;
            case "LessThan" -> LogicalQuery.Operator.LT;
            case "LessThanEqual" -> LogicalQuery.Operator.LTE;
            case "Between" -> LogicalQuery.Operator.BETWEEN;
            case "In" -> LogicalQuery.Operator.IN;
            case "NotIn" -> LogicalQuery.Operator.NOT_IN;
            case "Like" -> ignoreCase ? LogicalQuery.Operator.IGNORE_CASE_LIKE : LogicalQuery.Operator.LIKE;
            case "NotLike" -> LogicalQuery.Operator.NOT_LIKE;
            case "StartingWith" -> LogicalQuery.Operator.STARTING_WITH;
            case "EndingWith" -> LogicalQuery.Operator.ENDING_WITH;
            case "Containing" -> LogicalQuery.Operator.CONTAINING;
            case "IsNull" -> LogicalQuery.Operator.IS_NULL;
            case "NotNull" -> LogicalQuery.Operator.NOT_NULL;
            case "True" -> LogicalQuery.Operator.IS_TRUE;
            case "False" -> LogicalQuery.Operator.IS_FALSE;
            case "Before" -> LogicalQuery.Operator.BEFORE;
            case "After" -> LogicalQuery.Operator.AFTER;
            case "IgnoreCase" -> throw new IllegalArgumentException("IgnoreCase should be handled as modifier, not operator");
            default -> throw new IllegalArgumentException("Unknown operator: " + operator);
        };
    }

    /**
     * Legacy overload for backward compatibility.
     * Passes null for entityClass, disabling context-aware property validation.
     */
    @Deprecated
    public static LogicalQuery parse(Method method, String idColumnName) {
        return parse(method, null, idColumnName);
    }
}
