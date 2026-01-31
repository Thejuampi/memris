package io.memris.query;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Enum of Spring Data JPA query method operators with their LogicalQuery mappings.
 * <p>
 * This enum provides fast, type-safe mapping from method name strings to
 * LogicalQuery.Operator values, replacing the switch statement approach.
 * <p>
 * Each enum value knows its string keyword(s) and the corresponding LogicalQuery.Operator.
 */
enum QueryOperator {
    // Equality operators
    EQUALS("Equals", "Is", LogicalQuery.Operator.EQ, true),
    NOT_EQUAL("NotEqual", "Not", LogicalQuery.Operator.NE),

    // Comparison operators
    GREATER_THAN("GreaterThan", LogicalQuery.Operator.GT),
    GREATER_THAN_EQUAL("GreaterThanEqual", LogicalQuery.Operator.GTE),
    LESS_THAN("LessThan", LogicalQuery.Operator.LT),
    LESS_THAN_EQUAL("LessThanEqual", LogicalQuery.Operator.LTE),
    BETWEEN("Between", LogicalQuery.Operator.BETWEEN),

    // Collection operators
    IN("In", LogicalQuery.Operator.IN),
    NOT_IN("NotIn", LogicalQuery.Operator.NOT_IN),

    // String operators
    LIKE("Like", LogicalQuery.Operator.LIKE, true),
    NOT_LIKE("NotLike", LogicalQuery.Operator.NOT_LIKE),
    STARTING_WITH("StartingWith", LogicalQuery.Operator.STARTING_WITH),
    NOT_STARTING_WITH("NotStartingWith", LogicalQuery.Operator.NOT_STARTING_WITH),
    ENDING_WITH("EndingWith", LogicalQuery.Operator.ENDING_WITH),
    NOT_ENDING_WITH("NotEndingWith", LogicalQuery.Operator.NOT_ENDING_WITH),
    CONTAINING("Containing", LogicalQuery.Operator.CONTAINING),
    NOT_CONTAINING("NotContaining", LogicalQuery.Operator.NOT_CONTAINING),

    // Null check operators
    IS_NULL("IsNull", LogicalQuery.Operator.IS_NULL),
    NOT_NULL("NotNull", LogicalQuery.Operator.NOT_NULL),

    // Boolean operators
    IS_TRUE("True", LogicalQuery.Operator.IS_TRUE),
    IS_FALSE("False", LogicalQuery.Operator.IS_FALSE),

    // Date operators
    BEFORE("Before", LogicalQuery.Operator.BEFORE),
    AFTER("After", LogicalQuery.Operator.AFTER);

    private final String[] keywords;
    private final LogicalQuery.Operator logicalOperator;
    private final boolean supportsIgnoreCase;

    QueryOperator(String keyword, LogicalQuery.Operator logicalOperator) {
        this(keyword, logicalOperator, false);
    }

    QueryOperator(String keyword1, String keyword2, LogicalQuery.Operator logicalOperator) {
        this(new String[]{keyword1, keyword2}, logicalOperator, false);
    }

    QueryOperator(String keyword1, String keyword2, LogicalQuery.Operator logicalOperator, boolean supportsIgnoreCase) {
        this(new String[]{keyword1, keyword2}, logicalOperator, supportsIgnoreCase);
    }

    QueryOperator(String keyword, LogicalQuery.Operator logicalOperator, boolean supportsIgnoreCase) {
        this(new String[]{keyword}, logicalOperator, supportsIgnoreCase);
    }

    QueryOperator(String[] keywords, LogicalQuery.Operator logicalOperator, boolean supportsIgnoreCase) {
        this.keywords = keywords;
        this.logicalOperator = logicalOperator;
        this.supportsIgnoreCase = supportsIgnoreCase;
    }

    /**
     * Get the LogicalQuery.Operator for this query operator.
     * If ignoreCase is true and this operator supports it, returns the case-insensitive variant.
     */
    LogicalQuery.Operator getLogicalOperator(boolean ignoreCase) {
        if (ignoreCase && supportsIgnoreCase) {
            return getCaseInsensitiveVariant();
        }
        return logicalOperator;
    }

    /**
     * Get the case-insensitive variant of this operator.
     */
    private LogicalQuery.Operator getCaseInsensitiveVariant() {
        return switch (this) {
            case EQUALS -> LogicalQuery.Operator.IGNORE_CASE_EQ;
            case LIKE -> LogicalQuery.Operator.IGNORE_CASE_LIKE;
            default -> logicalOperator;
        };
    }

    /**
     * Check if this operator matches the given keyword.
     */
    boolean matches(String keyword) {
        for (String k : keywords) {
            if (k.equals(keyword)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Registry for fast keyword -> QueryOperator lookup.
     */
    private static final Map<String, QueryOperator> KEYWORD_REGISTRY;

    static {
        KEYWORD_REGISTRY = new HashMap<>();
        for (QueryOperator op : values()) {
            for (String keyword : op.keywords) {
                KEYWORD_REGISTRY.put(keyword, op);
            }
        }
    }

    /**
     * Find a QueryOperator by its keyword string.
     *
     * @param keyword the operator keyword (e.g., "GreaterThan", "Like")
     * @return the matching QueryOperator, or null if not found
     */
    static QueryOperator fromKeyword(String keyword) {
        return KEYWORD_REGISTRY.get(keyword);
    }
}

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
     * Encapsulates parsing state during token processing.
     * <p>
     * This mutable state holder tracks the current position in the query method
     * parsing process, including pending tokens, validation flags, and the
     * current argument index.
     */
    private static final class ParseState {
        // Pending condition state
        String pendingProperty;
        String pendingOperatorValue;
        boolean pendingIgnoreCase;

        // OrderBy state
        boolean inOrderBy;
        Boolean orderByDirection;

        // Validation state
        boolean lastTokenWasCombinator;
        boolean lastTokenWasOperator;
        boolean hasPropertyAfterBy;

        // Combinator for the next condition (AND/OR)
        LogicalQuery.Combinator nextCombinator;

        // Argument index for parameter binding
        int argIndex;

        ParseState() {
            this.argIndex = 0;
            this.nextCombinator = LogicalQuery.Combinator.AND; // default
        }
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

        if (method.isAnnotationPresent(io.memris.core.Query.class)) {
            return JpqlQueryParser.parse(method, entityClass);
        }

        // Fast path: Check for built-in operations using MethodKey signature matching
        // This correctly handles overloads like deleteById(Long) vs deleteById(UUID)
        // Uses BuiltInResolver for deterministic tie-breaking and ambiguity detection

        // First check active built-ins
        OpCode builtInOp = BuiltInResolver.resolveBuiltInOpCode(method, QueryMethodLexer.BUILT_INS);

        // If not found, check reserved built-ins (for future Spring Data compatibility)
        if (builtInOp == null) {
            builtInOp = BuiltInResolver.resolveBuiltInOpCode(method, QueryMethodLexer.RESERVED_BUILT_INS);
        }

        if (builtInOp != null) {
            // no nice: better to keep it all generic with below code.
            return planBuiltIn(methodName, builtInOp, paramTypes.length);
        }

        // Otherwise: derived query parsing
        LimitParseResult limitParse = parseLimit(methodName);
        if (limitParse.limit() > 0 && !isFindPrefix(extractPrefix(methodName))) {
            throw new IllegalArgumentException("Top/First is only supported for find/read/query/get methods: " + methodName);
        }
        String parseName = limitParse.normalizedName();

        // Tokenize using context-aware lexer
        List<QueryMethodToken> tokens = QueryMethodLexer.tokenize(entityClass, parseName);
        LogicalQuery.ReturnKind returnKind = determineReturnKind(parseName, returnType, paramTypes.length);
        List<LogicalQuery.Condition> conditions = new ArrayList<>();
        LogicalQuery.OrderBy[] orderBy = null;

        // Initialize parse state
        ParseState state = new ParseState();

        for (QueryMethodToken token : tokens) {
            switch (token.type()) {
                case PROPERTY_PATH -> handlePropertyToken(token, state, methodName, conditions);
                case OPERATOR -> handleOperatorToken(token, state, methodName, conditions);
                case AND, OR -> handleCombinatorToken(token.type(), state, methodName, conditions);
                case ASC -> state.orderByDirection = true;
                case DESC -> state.orderByDirection = false;
                case FIND_BY, COUNT_BY, EXISTS_BY, DELETE_BY, DELETE, DELETE_ALL -> {
                    // Query method prefix tokens - skip
                }
                case OPERATION -> {
                    // "no nice" Built-in operations - should not reach here since we handle them earlier
                    // But if we do, skip the token
                }
            }
        }

        // Validation: No property after "By"
        validateHasPropertyAfterBy(state.hasPropertyAfterBy, state.inOrderBy, methodName);

        // Validation: Ending with combinator
        validateNotEndingWithCombinator(state.lastTokenWasCombinator, methodName);

        // Validation: OrderBy without property
        validateOrderByHasProperty(state.inOrderBy, state.pendingProperty, methodName);

        // Finalize last condition if pending
        if (state.pendingProperty != null && !state.inOrderBy) {
            state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue, state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
        }

        // Finalize OrderBy
        if (state.inOrderBy && state.pendingProperty != null) {
            if (state.orderByDirection != null) {
                orderBy = new LogicalQuery.OrderBy[]{
                    LogicalQuery.OrderBy.of(state.pendingProperty, state.orderByDirection)
                };
            } else {
                orderBy = new LogicalQuery.OrderBy[]{
                    LogicalQuery.OrderBy.asc(state.pendingProperty)
                };
            }
        }

        // Determine OpCode for derived queries based on method prefix
        OpCode opCode = determineOpCodeForDerived(parseName, returnKind);

        return LogicalQuery.of(opCode, returnKind,
                               conditions.toArray(new LogicalQuery.Condition[0]),
                               new LogicalQuery.Join[0],
                               orderBy,
                               limitParse.limit(),
                               state.argIndex);
    }

    /**
     * Determine OpCode for derived queries based on method prefix and ReturnKind.
     */
    private static OpCode determineOpCodeForDerived(String methodName, LogicalQuery.ReturnKind returnKind) {
        String lower = methodName.toLowerCase();

        // Check for deleteBy prefix
        if (lower.startsWith("deleteby")) {
            return OpCode.DELETE_QUERY;
        }

        // Otherwise, determine from ReturnKind
        return switch (returnKind) {
            case MANY_LIST, ONE_OPTIONAL -> OpCode.FIND;
            case COUNT_LONG -> OpCode.COUNT;
            case EXISTS_BOOL -> OpCode.EXISTS;
            default -> throw new IllegalArgumentException("Unexpected return kind for derived query: " + returnKind);
        };
    }

    /**
     * Finalize a condition and return the next argIndex.
     * <p>
     * This handles the parameter consumption for different operator types:
     * - Unary operators (IS_NULL, IS_TRUE, IS_FALSE, NOT_NULL) consume 0 parameters
     * - BETWEEN consumes 2 parameters
     * - Other operators consume 1 parameter
     */
    private static int finalizeCondition(String propertyPath, String operatorValue,
                                       boolean ignoreCase, int argIndex,
                                       List<LogicalQuery.Condition> conditions,
                                       LogicalQuery.Combinator combinator,
                                       ParseState state) {
        LogicalQuery.Operator operator = (operatorValue == null)
                ? LogicalQuery.Operator.EQ
                : mapToOperator(operatorValue, ignoreCase);

        conditions.add(LogicalQuery.Condition.of(propertyPath, operator, argIndex, ignoreCase, combinator));

        // Reset combinator to default (AND) after using it - each combinator only applies once
        if (state != null) {
            state.nextCombinator = LogicalQuery.Combinator.AND;
        }

        // Return the next argIndex based on how many parameters this operator consumes
        return switch (operator) {
            case IS_NULL, NOT_NULL, IS_TRUE, IS_FALSE -> argIndex; // Unary operators consume 0 params
            case BETWEEN -> argIndex + 2; // Between consumes 2 params
            default -> argIndex + 1; // Most operators consume 1 param
        };
    }

    /**
     * Handle a PROPERTY_PATH token.
     */
    private static void handlePropertyToken(QueryMethodToken token, ParseState state,
                                            String methodName, List<LogicalQuery.Condition> conditions) {
        state.hasPropertyAfterBy = true;
        if (state.inOrderBy) {
            state.pendingProperty = token.value();
        } else if (state.pendingProperty != null) {
            validatePropertyAfterOperator(state.pendingOperatorValue, methodName, token.value());
            state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue, state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
            state.pendingProperty = token.value();
            state.pendingOperatorValue = null;
            state.pendingIgnoreCase = false;
        } else {
            state.pendingProperty = token.value();
        }
        state.lastTokenWasCombinator = false;
        state.lastTokenWasOperator = false;
    }

    /**
     * Handle an OPERATOR token.
     */
    private static void handleOperatorToken(QueryMethodToken token, ParseState state,
                                            String methodName, List<LogicalQuery.Condition> conditions) {
        String value = token.value();
        if (value.equals("OrderBy")) {
            state.inOrderBy = true;
            if (state.pendingProperty != null) {
                state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue, state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
                state.pendingProperty = null;
                state.pendingOperatorValue = null;
                state.pendingIgnoreCase = false;
            }
        } else if (value.equals("IgnoreCase")) {
            state.pendingIgnoreCase = true;
        } else {
            validateNoConsecutiveOperators(state.lastTokenWasOperator, state.pendingOperatorValue, methodName, value);
            state.pendingOperatorValue = value;
            state.lastTokenWasOperator = true;
        }
        state.lastTokenWasCombinator = false;
    }

    /**
     * Handle an AND or OR combinator token.
     */
    private static void handleCombinatorToken(QueryMethodTokenType tokenType, ParseState state, String methodName,
                                               List<LogicalQuery.Condition> conditions) {
        validateNoConsecutiveCombinators(state.lastTokenWasCombinator, methodName);
        // Set combinator for the next condition
        state.nextCombinator = (tokenType == QueryMethodTokenType.AND) 
            ? LogicalQuery.Combinator.AND 
            : LogicalQuery.Combinator.OR;
        if (state.pendingProperty != null) {
            // Finalize current condition with the combinator that follows it
            state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue, state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
            state.pendingProperty = null;
            state.pendingOperatorValue = null;
            state.pendingIgnoreCase = false;
        }
        state.lastTokenWasCombinator = true;
        state.lastTokenWasOperator = false;
    }

    // ========== Validation methods ==========

    /**
     * Validate that there are no consecutive combinators.
     */
    private static void validateNoConsecutiveCombinators(boolean lastTokenWasCombinator, String methodName) {
        if (lastTokenWasCombinator) {
            throw new IllegalArgumentException("Invalid query method '%s': consecutive combinators (And/Or)".formatted(methodName));
        }
    }

    /**
     * Validate that there are no consecutive operators.
     */
    private static void validateNoConsecutiveOperators(boolean lastTokenWasOperator, String pendingOperatorValue,
                                                        String methodName, String value) {
        if (lastTokenWasOperator && pendingOperatorValue != null) {
            throw new IllegalArgumentException("Invalid query method '%s': consecutive operators '%s' and '%s'".formatted(methodName, pendingOperatorValue, value));
        }
    }

    /**
     * Validate that a property doesn't appear after an operator without a combinator.
     */
    private static void validatePropertyAfterOperator(String pendingOperatorValue, String methodName,
                                                      String property) {
        if (pendingOperatorValue != null) {
            throw new IllegalArgumentException("Invalid query method '%s': property '%s' appears after operator '%s' without combinator (And/Or)".formatted(methodName, property, pendingOperatorValue));
        }
    }

    /**
     * Validate that there's at least one property after "By".
     */
    private static void validateHasPropertyAfterBy(boolean hasPropertyAfterBy, boolean inOrderBy, String methodName) {
        if (!hasPropertyAfterBy && !inOrderBy) {
            throw new IllegalArgumentException("Invalid query method '%s': no property specified after 'By'".formatted(methodName));
        }
    }

    /**
     * Validate that method doesn't end with a combinator.
     */
    private static void validateNotEndingWithCombinator(boolean lastTokenWasCombinator, String methodName) {
        if (lastTokenWasCombinator) {
            throw new IllegalArgumentException("Invalid query method '%s': method ends with combinator (And/Or) without property".formatted(methodName));
        }
    }

    /**
     * Validate that OrderBy has a property.
     */
    private static void validateOrderByHasProperty(boolean inOrderBy, String pendingProperty, String methodName) {
        if (inOrderBy && pendingProperty == null) {
            throw new IllegalArgumentException("Invalid query method '%s': OrderBy specified without property".formatted(methodName));
        }
    }

    /**
     * Determines the return kind based on method name pattern and return type.
     * <p>
     * Uses pattern matching on method name (same approach as QueryMethodLexer.classifyOperation)
     * to support both query and CRUD operations dynamically.
     */
    private static LogicalQuery.ReturnKind determineReturnKind(String methodName, Class<?> returnType, int paramCount) {
        String prefix = extractPrefix(methodName);
        String remaining = methodName.substring(prefix.length());
        boolean hasBy = remaining.startsWith("By");
        boolean isAll = "All".equals(remaining);

        return switch (prefix.toLowerCase()) {
            case "find", "read", "query", "get" -> {
                if (isAll || remaining.isEmpty()) yield LogicalQuery.ReturnKind.MANY_LIST; // findAll()
                if (hasBy) {
                    // findById() vs findByXxx()
                    yield returnType.equals(java.util.Optional.class)
                        ? LogicalQuery.ReturnKind.ONE_OPTIONAL
                        : LogicalQuery.ReturnKind.MANY_LIST;
                }
                throw new IllegalArgumentException("Invalid find method: " + methodName);
            }
            case "count" -> LogicalQuery.ReturnKind.COUNT_LONG;  // count() or countByXxx()
            case "exists" -> LogicalQuery.ReturnKind.EXISTS_BOOL;  // existsByXxx()
            case "delete" -> {
                if (hasBy && remaining.startsWith("ById")) yield LogicalQuery.ReturnKind.DELETE_BY_ID;
                if (hasBy) yield LogicalQuery.ReturnKind.MANY_LIST;  // deleteByXxx()
                if (isAll) yield LogicalQuery.ReturnKind.DELETE_ALL;  // deleteAll()
                yield LogicalQuery.ReturnKind.DELETE;  // delete(T)
            }
            case "save" -> {
                if (isAll) yield LogicalQuery.ReturnKind.SAVE_ALL;  // saveAll()
                yield LogicalQuery.ReturnKind.SAVE;  // save()
            }
            default -> throw new IllegalArgumentException("Unknown prefix: " + prefix);
        };
    }

    /**
     * Extracts the prefix from a method name (case-insensitive).
     * <p>
     * Uses QueryMethodLexer.PREFIXES to ensure consistency.
     */
    private static String extractPrefix(String methodName) {
        String lower = methodName.toLowerCase();
        for (String prefix : QueryMethodLexer.PREFIXES) {
            if (lower.startsWith(prefix)) {
                return methodName.substring(0, prefix.length());
            }
        }
        // no prefix match -> treat entire name as "prefix"
        return methodName;
    }

    private static boolean isFindPrefix(String prefix) {
        return switch (prefix.toLowerCase()) {
            case "find", "read", "query", "get" -> true;
            default -> false;
        };
    }

    private record LimitParseResult(int limit, String normalizedName) {
    }

    private static LimitParseResult parseLimit(String methodName) {
        String prefix = extractPrefix(methodName);
        String remaining = methodName.substring(prefix.length());

        if (remaining.startsWith("Top")) {
            return parseLimitAfterPrefix(methodName, prefix, remaining, 3);
        }
        if (remaining.startsWith("First")) {
            return parseLimitAfterPrefix(methodName, prefix, remaining, 5);
        }
        return new LimitParseResult(0, methodName);
    }

    private static LimitParseResult parseLimitAfterPrefix(String methodName, String prefix, String remaining, int keywordLength) {
        int idx = keywordLength;
        int startDigits = idx;
        while (idx < remaining.length() && Character.isDigit(remaining.charAt(idx))) {
            idx++;
        }
        int limit = 1;
        if (idx > startDigits) {
            limit = Integer.parseInt(remaining.substring(startDigits, idx));
        }
        String normalized = prefix + remaining.substring(idx);
        return new LimitParseResult(limit, normalized);
    }

    /**
     * Map a method name operator string to LogicalQuery.Operator.
     * <p>
     * Uses the QueryOperator enum for fast, type-safe mapping.
     *
     * @param operator the operator keyword from the method name
     * @param ignoreCase whether to use case-insensitive matching
     * @return the corresponding LogicalQuery.Operator
     * @throws IllegalArgumentException if the operator is unknown
     */
    private static LogicalQuery.Operator mapToOperator(String operator, boolean ignoreCase) {
        if ("IgnoreCase".equals(operator)) {
            throw new IllegalArgumentException("IgnoreCase should be handled as modifier, not operator");
        }

        QueryOperator queryOperator = QueryOperator.fromKeyword(operator);
        if (queryOperator == null) {
            throw new IllegalArgumentException("Unknown operator: " + operator);
        }

        return queryOperator.getLogicalOperator(ignoreCase);
    }

    /**
     * Plan a built-in operation (save, findById, findAll, etc.).
     * <p>
     * Built-ins are recognized by exact name match and produce LogicalQuery
     * with no conditions (except FIND_BY_ID which has an ID condition).
     * <p>
     * Reserved operations (if any) are defined for future Spring Data compatibility
     * and throw "not yet implemented" at runtime.
     */
    private static LogicalQuery planBuiltIn(String methodName, OpCode op, int arity) {
        return switch (op) {
            // Built-in query operations
            case FIND_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.ONE_OPTIONAL,
                    new LogicalQuery.Condition[]{LogicalQuery.Condition.idCondition(0)},
                    null,
                    arity);

            case FIND_ALL -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.MANY_LIST,
                    new LogicalQuery.Condition[0],
                    null,
                    arity);

            case FIND_ALL_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.MANY_LIST,
                    new LogicalQuery.Condition[]{
                        LogicalQuery.Condition.of(
                            LogicalQuery.Condition.ID_PROPERTY,
                            LogicalQuery.Operator.IN,
                            0)
                    },
                    null,
                    arity);

            case EXISTS_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.EXISTS_BOOL,
                    new LogicalQuery.Condition[]{LogicalQuery.Condition.idCondition(0)},
                    null,
                    arity);

            case COUNT_ALL -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.COUNT_LONG,
                    new LogicalQuery.Condition[0],
                    null,
                    arity);

            // Built-in CRUD operations
            case SAVE_ONE -> LogicalQuery.crud(
                    op,
                    LogicalQuery.ReturnKind.SAVE,
                    arity);

            case SAVE_ALL -> LogicalQuery.crud(
                    op,
                    LogicalQuery.ReturnKind.SAVE_ALL,
                    arity);

            case DELETE_ONE -> LogicalQuery.crud(
                    op,
                    LogicalQuery.ReturnKind.DELETE,
                    arity);

            case DELETE_ALL -> LogicalQuery.crud(
                    op,
                    LogicalQuery.ReturnKind.DELETE_ALL,
                    arity);

            case DELETE_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.DELETE_BY_ID,
                    new LogicalQuery.Condition[]{LogicalQuery.Condition.idCondition(0)},
                    null,
                    arity);

            case DELETE_ALL_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.MANY_LIST,
                    new LogicalQuery.Condition[]{
                        LogicalQuery.Condition.of(
                            LogicalQuery.Condition.ID_PROPERTY,
                            LogicalQuery.Operator.IN,
                            0)
                    },
                    null,
                    arity);

            default -> throw new IllegalArgumentException("Unsupported built-in operation: " + op);
        };
    }
}
