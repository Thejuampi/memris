package io.memris.query;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Locale.ROOT;

/**
 * Enum of Spring Data JPA query method operators with their LogicalQuery
 * mappings.
 * <p>
 * This enum provides fast, type-safe mapping from method name strings to
 * LogicalQuery.Operator values, replacing the switch statement approach.
 * <p>
 * Each enum value knows its string keyword(s) and the corresponding
 * LogicalQuery.Operator.
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
        this(new String[] { keyword1, keyword2 }, logicalOperator, false);
    }

    QueryOperator(String keyword1, String keyword2, LogicalQuery.Operator logicalOperator, boolean supportsIgnoreCase) {
        this(new String[] { keyword1, keyword2 }, logicalOperator, supportsIgnoreCase);
    }

    QueryOperator(String keyword, LogicalQuery.Operator logicalOperator, boolean supportsIgnoreCase) {
        this(new String[] { keyword }, logicalOperator, supportsIgnoreCase);
    }

    QueryOperator(String[] keywords, LogicalQuery.Operator logicalOperator, boolean supportsIgnoreCase) {
        this.keywords = keywords;
        this.logicalOperator = logicalOperator;
        this.supportsIgnoreCase = supportsIgnoreCase;
    }

    /**
     * Get the LogicalQuery.Operator for this query operator.
     * If ignoreCase is true and this operator supports it, returns the
     * case-insensitive variant.
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
 * - findAll → ReturnKind.MANY_LIST (or MANY_SET), no conditions
 * - findByXxx → ReturnKind.MANY_LIST (or MANY_SET), Condition(xxx, EQ, arg0)
 * - findByXxxAndYyy → ReturnKind.MANY_LIST (or MANY_SET), Condition(xxx, EQ, arg0),
 * Condition(yyy, EQ, arg1)
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
     * @param method       repository method to parse
     * @param entityClass  entity class for context-aware property resolution
     * @return a LogicalQuery representing the semantic meaning of the method
     */
    public static LogicalQuery parse(Method method, Class<?> entityClass) {
        String methodName = method.getName();
        Class<?> returnType = method.getReturnType();
        Class<?>[] paramTypes = method.getParameterTypes();

        if (method.isAnnotationPresent(io.memris.core.Query.class)) {
            return JpqlQueryParser.parse(method, entityClass);
        }

        if (isRecordProjectionReturnType(method)) {
            throw new IllegalArgumentException("Record projections require @Query with select aliases: " + methodName);
        }

        // Fast path: Check for built-in operations using MethodKey signature matching
        // This correctly handles overloads like deleteById(Long) vs deleteById(UUID)
        // Uses BuiltInResolver for deterministic tie-breaking and ambiguity detection

        // First check active built-ins
        var builtInOp = BuiltInResolver.resolveBuiltInOpCode(method, QueryMethodLexer.BUILT_INS);

        // If not found, check reserved built-ins (for future Spring Data compatibility)
        if (builtInOp == null) {
            builtInOp = BuiltInResolver.resolveBuiltInOpCode(method, QueryMethodLexer.RESERVED_BUILT_INS);
        }

        if (builtInOp != null) {
            // no nice: better to keep it all generic with below code.
            return planBuiltIn(builtInOp, paramTypes.length);
        }

        // Otherwise: derived query parsing
        var limitParse = parseLimit(methodName);
        var parseName = limitParse.normalizedName();
        var distinctParse = parseDistinct(parseName);
        parseName = distinctParse.normalizedName();
        var distinct = distinctParse.distinct();

        // Tokenize using context-aware lexer
        List<QueryMethodToken> tokens = QueryMethodLexer.tokenize(entityClass, parseName);
        var returnKind = determineReturnKind(parseName, returnType);
        var grouping = extractGrouping(method, parseName, returnType);
        if (isMapReturnType(returnType) && grouping == null) {
            throw new IllegalArgumentException(
                    "Map return types require GroupingBy or countBy with Map return: " + methodName);
        }
        if (grouping != null) {
            if (parseName.contains("OrderBy")) {
                throw new IllegalArgumentException(
                        "Grouping queries do not support OrderBy: " + methodName);
            }
            List<LogicalQuery.Condition> conditions = new ArrayList<>();
        var state = new ParseState();
        var isCountGroupingWithoutConditions = parseName.toLowerCase(ROOT).startsWith("countby")
                && !parseName.contains("GroupingBy")
                && returnKind == LogicalQuery.ReturnKind.MANY_MAP;
            if (isCountGroupingWithoutConditions) {
                if (paramTypes.length > 0) {
                    throw new IllegalArgumentException(
                            "countBy grouping without GroupingBy does not accept parameters: " + methodName);
                }
                for (QueryMethodToken token : tokens) {
                    switch (token.type()) {
                        case OPERATOR, OR -> throw new IllegalArgumentException(
                                "countBy grouping without GroupingBy only supports simple property paths: "
                                        + methodName);
                        default -> {
                        }
                    }
                }
            }
            if (!isCountGroupingWithoutConditions) {
                var parseNameForConditions = parseName;
                var groupingIndex = parseName.indexOf("GroupingBy");
                if (groupingIndex >= 0) {
                    parseNameForConditions = parseName.substring(0, groupingIndex);
                }
                List<QueryMethodToken> groupingTokens = QueryMethodLexer.tokenize(entityClass, parseNameForConditions);
                for (QueryMethodToken token : groupingTokens) {
                    switch (token.type()) {
                        case PROPERTY_PATH -> handlePropertyToken(token, state, methodName, conditions);
                        case OPERATOR -> handleOperatorToken(token, state, methodName, conditions);
                        case AND, OR -> handleCombinatorToken(token.type(), state, methodName, conditions);
                        case ASC, DESC, FIND_BY, COUNT_BY, EXISTS_BY, DELETE_BY, DELETE, DELETE_ALL, OPERATION -> {
                            // skip
                        }
                    }
                }
                validateHasPropertyAfterBy(state.hasPropertyAfterBy, state.inOrderBy, true, methodName);
                validateNotEndingWithCombinator(state.lastTokenWasCombinator, methodName);
                validateOrderByHasProperty(state.inOrderBy, state.pendingProperty, methodName);
                if (state.pendingProperty != null && !state.inOrderBy) {
                    state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue,
                            state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
                }
            }
            return LogicalQuery.of(
                    determineOpCodeForDerived(parseName, returnKind),
                    returnKind,
                    conditions.toArray(new LogicalQuery.Condition[0]),
                    new LogicalQuery.UpdateAssignment[0],
                    null,
                    new LogicalQuery.Join[0],
                    null,
                    grouping,
                    null,
                    0,
                    false,
                    new Object[0],
                    new int[0],
                    state.argIndex);
        }
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
                    // "no nice" Built-in operations - should not reach here since we handle them
                    // earlier
                    // But if we do, skip the token
                }
            }
        }

        // Validation: No property after "By"
        validateHasPropertyAfterBy(state.hasPropertyAfterBy, state.inOrderBy, false, methodName);

        // Validation: Ending with combinator
        validateNotEndingWithCombinator(state.lastTokenWasCombinator, methodName);

        // Validation: OrderBy without property
        validateOrderByHasProperty(state.inOrderBy, state.pendingProperty, methodName);

        // Finalize last condition if pending
        if (state.pendingProperty != null && !state.inOrderBy) {
            state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue,
                    state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
        }

        // Finalize OrderBy
        if (state.inOrderBy && state.pendingProperty != null) {
            if (state.orderByDirection != null) {
                orderBy = new LogicalQuery.OrderBy[] {
                        LogicalQuery.OrderBy.of(state.pendingProperty, state.orderByDirection)
                };
            } else {
                orderBy = new LogicalQuery.OrderBy[] {
                        LogicalQuery.OrderBy.asc(state.pendingProperty)
                };
            }
        }

        // Determine OpCode for derived queries based on method prefix
        var opCode = determineOpCodeForDerived(parseName, returnKind);

        return LogicalQuery.of(opCode, returnKind,
                conditions.toArray(new LogicalQuery.Condition[0]),
                new LogicalQuery.UpdateAssignment[0],
                null,
                new LogicalQuery.Join[0],
                orderBy,
                null,
                null,
                limitParse.limit(),
                distinct,
                new Object[0],
                new int[0],
                state.argIndex);
    }

    /**
     * Determine OpCode for derived queries based on method prefix and ReturnKind.
     */
    private static OpCode determineOpCodeForDerived(String methodName, LogicalQuery.ReturnKind returnKind) {
        String lower = methodName.toLowerCase(ROOT);

        // Check for deleteBy prefix
        if (lower.startsWith("deleteby")) {
            return OpCode.DELETE_QUERY;
        }

        // Otherwise, determine from ReturnKind
        return switch (returnKind) {
            case MANY_LIST, MANY_SET, MANY_MAP, ONE_OPTIONAL -> OpCode.FIND;
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

        // Reset combinator to default (AND) after using it - each combinator only
        // applies once
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
            state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue,
                    state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
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
            if ("OrderBy".equals(value)) {
            state.inOrderBy = true;
            if (state.pendingProperty != null) {
                state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue,
                        state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
                state.pendingProperty = null;
                state.pendingOperatorValue = null;
                state.pendingIgnoreCase = false;
            }
        } else             if ("IgnoreCase".equals(value)) {
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
            state.argIndex = finalizeCondition(state.pendingProperty, state.pendingOperatorValue,
                    state.pendingIgnoreCase, state.argIndex, conditions, state.nextCombinator, state);
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
            throw new IllegalArgumentException(
                    "Invalid query method '%s': consecutive combinators (And/Or)".formatted(methodName));
        }
    }

    /**
     * Validate that there are no consecutive operators.
     */
    private static void validateNoConsecutiveOperators(boolean lastTokenWasOperator, String pendingOperatorValue,
            String methodName, String value) {
        if (lastTokenWasOperator && pendingOperatorValue != null) {
            throw new IllegalArgumentException("Invalid query method '%s': consecutive operators '%s' and '%s'"
                    .formatted(methodName, pendingOperatorValue, value));
        }
    }

    /**
     * Validate that a property doesn't appear after an operator without a
     * combinator.
     */
    private static void validatePropertyAfterOperator(String pendingOperatorValue, String methodName,
            String property) {
        if (pendingOperatorValue != null) {
            throw new IllegalArgumentException(
                    "Invalid query method '%s': property '%s' appears after operator '%s' without combinator (And/Or)"
                            .formatted(methodName, property, pendingOperatorValue));
        }
    }

    /**
     * Validate that there's at least one property after "By".
     */
    private static void validateHasPropertyAfterBy(boolean hasPropertyAfterBy, boolean inOrderBy, boolean isGroupingQuery, String methodName) {
        if (!hasPropertyAfterBy && !inOrderBy && !isGroupingQuery) {
            throw new IllegalArgumentException(
                    "Invalid query method '%s': no property specified after 'By'".formatted(methodName));
        }
    }

    /**
     * Validate that method doesn't end with a combinator.
     */
    private static void validateNotEndingWithCombinator(boolean lastTokenWasCombinator, String methodName) {
        if (lastTokenWasCombinator) {
            throw new IllegalArgumentException(
                    "Invalid query method '%s': method ends with combinator (And/Or) without property"
                            .formatted(methodName));
        }
    }

    /**
     * Validate that OrderBy has a property.
     */
    private static void validateOrderByHasProperty(boolean inOrderBy, String pendingProperty, String methodName) {
        if (inOrderBy && pendingProperty == null) {
            throw new IllegalArgumentException(
                    "Invalid query method '%s': OrderBy specified without property".formatted(methodName));
        }
    }

    /**
     * Determines the return kind based on method name pattern and return type.
     * <p>
     * Uses pattern matching on method name (same approach as
     * QueryMethodLexer.classifyOperation)
     * to support both query and CRUD operations dynamically.
     */
    private static LogicalQuery.ReturnKind determineReturnKind(String methodName, Class<?> returnType) {
        var prefix = extractPrefix(methodName);
        var remaining = methodName.substring(prefix.length());
        var hasBy = remaining.startsWith("By");
        var isAll = "All".equals(remaining);

        return switch (prefix.toLowerCase(ROOT)) {
            case "find", "read", "query", "get" -> {
                if (remaining.startsWith("AllGroupingBy")) {
                    yield LogicalQuery.ReturnKind.MANY_MAP;
                }
                if (isAll || remaining.isEmpty())
                    yield isSetReturnType(returnType)
                            ? LogicalQuery.ReturnKind.MANY_SET
                            : LogicalQuery.ReturnKind.MANY_LIST; // findAll()
                if (hasBy) {
                    // findById() vs findByXxx()
                    yield returnType.equals(java.util.Optional.class)
                            ? LogicalQuery.ReturnKind.ONE_OPTIONAL
                            : (isSetReturnType(returnType)
                                    ? LogicalQuery.ReturnKind.MANY_SET
                                    : (isMapReturnType(returnType)
                                            ? LogicalQuery.ReturnKind.MANY_MAP
                                            : LogicalQuery.ReturnKind.MANY_LIST));
                }
                throw new IllegalArgumentException("Invalid find method: " + methodName);
            }
            case "count" -> isMapReturnType(returnType)
                    ? LogicalQuery.ReturnKind.MANY_MAP
                    : LogicalQuery.ReturnKind.COUNT_LONG; // count() or countByXxx()
            case "exists" -> LogicalQuery.ReturnKind.EXISTS_BOOL; // existsByXxx()
            case "delete" -> {
                if (hasBy && remaining.startsWith("ById"))
                    yield LogicalQuery.ReturnKind.DELETE_BY_ID;
                if (hasBy)
                    yield LogicalQuery.ReturnKind.MANY_LIST; // deleteByXxx()
                if (isAll)
                    yield LogicalQuery.ReturnKind.DELETE_ALL; // deleteAll()
                yield LogicalQuery.ReturnKind.DELETE; // delete(T)
            }
            case "save" -> {
                if (isAll)
                    yield LogicalQuery.ReturnKind.SAVE_ALL; // saveAll()
                yield LogicalQuery.ReturnKind.SAVE; // save()
            }
            default -> throw new IllegalArgumentException("Unknown prefix: " + prefix);
        };
    }

    private static boolean isSetReturnType(Class<?> returnType) {
        return java.util.Set.class.isAssignableFrom(returnType);
    }

    private static boolean isMapReturnType(Class<?> returnType) {
        return java.util.Map.class.isAssignableFrom(returnType);
    }

    /**
     * Extract grouping information from method name for Map return types.
     * <p>
     * Examples:
     * - "findAllGroupingByDepartment" → Grouping("department", LIST)
     * - "countByDepartment" → Grouping("department", COUNT)
     */
    private static LogicalQuery.Grouping extractGrouping(Method method, String methodName, Class<?> returnType) {
        if (!isMapReturnType(returnType)) {
            return null;
        }

        var keyType = resolveMapKeyType(method, methodName);
        var groupingToken = "GroupingBy";
        var groupingByIndex = methodName.indexOf(groupingToken);
        if (groupingByIndex >= 0) {
        var afterGrouping = methodName.substring(groupingByIndex + groupingToken.length());
        var groupingPart = afterGrouping;
            var byIndex = afterGrouping.indexOf("By");
            if (byIndex > 0) {
                groupingPart = afterGrouping.substring(0, byIndex);
            }
            if (!groupingPart.isEmpty()) {
        String[] properties = parseGroupingProperties(groupingPart, methodName);
        var valueType = methodName.toLowerCase().startsWith("count")
                ? LogicalQuery.Grouping.GroupValueType.COUNT
                : (methodName.contains("AsSet")
                        ? LogicalQuery.Grouping.GroupValueType.SET
                        : LogicalQuery.Grouping.GroupValueType.LIST);
                return new LogicalQuery.Grouping(properties, keyType, valueType);
            }
        }

        var byIndex = methodName.indexOf("By");
        if (byIndex >= 0 && methodName.toLowerCase().startsWith("count")) {
            var afterBy = methodName.substring(byIndex + 2);
            if (!afterBy.isEmpty()) {
                String[] properties = parseGroupingProperties(afterBy, methodName);
                return new LogicalQuery.Grouping(properties, keyType, LogicalQuery.Grouping.GroupValueType.COUNT);
            }
        }

        return null;
    }

    private static Class<?> resolveMapKeyType(Method method, String methodName) {
        Type generic = method.getGenericReturnType();
        if (generic instanceof ParameterizedType parameterized) {
            Type[] args = parameterized.getActualTypeArguments();
            if (args.length == 2 && args[0] instanceof Class<?> keyClass) {
                return keyClass;
            }
        }
        throw new IllegalArgumentException("Map return type must declare key type for grouping: " + methodName);
    }

    @SuppressWarnings("StringSplitter")
    private static String[] parseGroupingProperties(String propertySuffix, String methodName) {
        String normalized = propertySuffix;
        if (normalized.endsWith("AsSet")) {
            normalized = normalized.substring(0, normalized.length() - "AsSet".length());
        }
        String[] rawParts = normalized.split("And");
        if (rawParts.length == 0) {
            throw new IllegalArgumentException("GroupingBy requires properties: " + methodName);
        }
        String[] properties = new String[rawParts.length];
        for (int i = 0; i < rawParts.length; i++) {
            String raw = rawParts[i];
            if (raw.isEmpty()) {
                throw new IllegalArgumentException("GroupingBy has empty property segment: " + methodName);
            }
            properties[i] = Character.toLowerCase(raw.charAt(0)) + raw.substring(1);
        }
        return properties;
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

    private static boolean isRecordProjectionReturnType(Method method) {
        Class<?> rawType = method.getReturnType();
        if (rawType.isRecord()) {
            return true;
        }
        Type generic = method.getGenericReturnType();
        if (generic instanceof ParameterizedType parameterized) {
            Type[] args = parameterized.getActualTypeArguments();
            if (args.length == 1 && args[0] instanceof Class<?> argClass && argClass.isRecord()) {
                return Optional.class.equals(rawType) || List.class.isAssignableFrom(rawType);
            }
        }
        return false;
    }

    private record LimitParseResult(int limit, String normalizedName) {
    }

    private record DistinctParseResult(boolean distinct, String normalizedName) {
    }

    private static LimitParseResult parseLimit(String methodName) {
        String prefix = extractPrefix(methodName);
        String remaining = methodName.substring(prefix.length());

        if (remaining.startsWith("Top")) {
            return parseLimitAfterPrefix(prefix, remaining, 3);
        }
        if (remaining.startsWith("First")) {
            return parseLimitAfterPrefix(prefix, remaining, 5);
        }
        return new LimitParseResult(0, methodName);
    }

    private static DistinctParseResult parseDistinct(String methodName) {
        String prefix = extractPrefix(methodName);
        String remaining = methodName.substring(prefix.length());
        if (remaining.startsWith("Distinct")) {
            return new DistinctParseResult(true, prefix + remaining.substring("Distinct".length()));
        }
        return new DistinctParseResult(false, methodName);
    }

    private static LimitParseResult parseLimitAfterPrefix(String prefix, String remaining, int keywordLength) {
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
     * @param operator   the operator keyword from the method name
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
    private static LogicalQuery planBuiltIn(OpCode op, int arity) {
        return switch (op) {
            // Built-in query operations
            case FIND_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.ONE_OPTIONAL,
                    new LogicalQuery.Condition[] { LogicalQuery.Condition.idCondition(0) },
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
                    new LogicalQuery.Condition[] {
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
                    new LogicalQuery.Condition[] { LogicalQuery.Condition.idCondition(0) },
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
                    new LogicalQuery.Condition[] { LogicalQuery.Condition.idCondition(0) },
                    null,
                    arity);

            case DELETE_ALL_BY_ID -> LogicalQuery.of(
                    op,
                    LogicalQuery.ReturnKind.MANY_LIST,
                    new LogicalQuery.Condition[] {
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
