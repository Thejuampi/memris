package io.memris.spring;

import io.memris.kernel.Predicate;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class QueryMethodParser {

    // Query type enumeration
    public enum QueryType {
        FIND, READ, GET, QUERY, STREAM,
        COUNT, EXISTS,
        DELETE, REMOVE
    }

    // Sort direction
    public enum SortDirection {
        ASC, DESC
    }

    // Sort order specification
    public record OrderBy(String property, SortDirection direction) {}

    // Result of parsing a query method (for metadata extraction)
    public record ParsedQueryResult(
        QueryType queryType,
        boolean isDistinct,
        Integer limit,
        List<String> conditions,
        List<OrderBy> orders,
        List<Predicate.Operator> operators,
        boolean isQueryMethod
    ) {
        public static ParsedQueryResult notQueryMethod() {
            return new ParsedQueryResult(QueryType.FIND, false, null, List.of(), List.of(), List.of(), false);
        }
    }

    // Match query prefixes with optional modifiers
    // Group 1: query type (find|query|get|read|stream|count|exists|delete|remove)
    // Group 2: modifier (Distinct|First|Top or Top{n})
    // Group 3: just the numeric part if Top{n}
    // Group 4: separator (By|And|Or)
    // Group 5: conditions + order by
    private static final Pattern QUERY_PATTERN = Pattern.compile(
        "^(find|query|get|read|stream|count|exists|delete|remove)" +
        "(Distinct|First|Top(\\d*))?" +
        "(By|And|Or)" +
        "(.+)$",
        Pattern.CASE_INSENSITIVE
    );

    static boolean isQueryMethod(Method method) {
        return QUERY_PATTERN.matcher(method.getName()).find();
    }

    static ParsedQueryResult parseQuery(Method method) {
        String methodName = method.getName();
        Matcher matcher = QUERY_PATTERN.matcher(methodName);
        if (!matcher.find()) {
            return ParsedQueryResult.notQueryMethod();
        }

        // Extract query type
        String queryTypeStr = matcher.group(1).toLowerCase();
        QueryType queryType = switch (queryTypeStr) {
            case "find", "query", "get", "read", "stream" -> QueryType.FIND;
            case "count" -> QueryType.COUNT;
            case "exists" -> QueryType.EXISTS;
            case "delete" -> QueryType.DELETE;
            case "remove" -> QueryType.REMOVE;
            default -> QueryType.FIND;
        };

        // Extract distinct flag
        boolean isDistinct = matcher.group(2) != null && matcher.group(2).equalsIgnoreCase("Distinct");

        // Extract limit from Top{n} modifier
        Integer limit = null;
        if (matcher.group(2) != null && matcher.group(2).toLowerCase().startsWith("top")) {
            String numberPart = matcher.group(3);  // Group 3 contains the digits (empty if just "Top")
            if (numberPart != null && !numberPart.isEmpty()) {
                try {
                    limit = Integer.parseInt(numberPart);
                } catch (NumberFormatException e) {
                    limit = 1;  // Invalid number, default to 1
                }
            } else {
                limit = 1;  // Top without number means limit 1
            }
        }
        // First also means limit 1
        if (matcher.group(2) != null && matcher.group(2).equalsIgnoreCase("First")) {
            limit = 1;
        }

        // Extract conditions (everything after By/And/Or)
        String conditionsPart = matcher.group(5);
        if (conditionsPart == null || conditionsPart.isEmpty()) {
            return new ParsedQueryResult(queryType, isDistinct, limit, List.of(), List.of(), List.of(), true);
        }

        // Split by OrderBy to separate conditions from ordering
        String[] orderBySplit = conditionsPart.split("OrderBy", 2);
        String conditionsStr = orderBySplit[0];
        String orderByStr = orderBySplit.length > 1 ? orderBySplit[1] : null;

        // Parse conditions
        List<String> conditions = splitConditions(conditionsStr);
        List<Predicate.Operator> operators = new ArrayList<>();
        List<String> columnNames = new ArrayList<>();

        for (String condition : conditions) {
            operators.add(detectOperator(condition));
            // Extract property name and convert to column name
            String propertyName = extractPropertyName(condition);
            columnNames.add(toColumnName(propertyName));
        }

        // Parse OrderBy clauses
        List<OrderBy> orders = new ArrayList<>();
        if (orderByStr != null && !orderByStr.isEmpty()) {
            // Handle multiple OrderBy clauses (e.g., "OrderByLastnameAscOrderByFirstnameDesc")
            // Property name: starts with uppercase, followed by any letters
            // Direction: optional Asc/Desc suffix
            // Use lookahead to stop property name at Asc/Desc suffix or end of string
            Pattern multiOrderPattern = Pattern.compile(
                "([A-Z][a-zA-Z]*?(?=(?:Asc|Desc)$|$))(?:(Asc|Desc))?",
                Pattern.CASE_INSENSITIVE
            );
            Matcher orderMatcher = multiOrderPattern.matcher(orderByStr);
            int lastEnd = 0;
            while (orderMatcher.find()) {
                String property = orderMatcher.group(1);
                String directionStr = orderMatcher.group(2);
                SortDirection direction = directionStr != null && directionStr.equalsIgnoreCase("Desc")
                    ? SortDirection.DESC
                    : SortDirection.ASC;
                orders.add(new OrderBy(toColumnName(property), direction));
                lastEnd = orderMatcher.end();
            }
        }

        return new ParsedQueryResult(queryType, isDistinct, limit, columnNames, orders, operators, true);
    }

    static Predicate parse(Method method, Object[] args) {
        ParsedQueryResult result = parseQuery(method);

        List<Predicate> predicates = new ArrayList<>();
        Parameter[] params = method.getParameters();
        int argIndex = 0;

        for (int i = 0; i < result.conditions().size(); i++) {
            String columnName = result.conditions().get(i);
            Predicate.Operator operator = result.operators().get(i);
            Parameter param = params.length > i ? params[i] : null;

            switch (operator) {
                case BETWEEN -> {
                    if (args == null || argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires 2 parameters");
                    }
                    Object lower = args[argIndex];
                    Object upper = args[argIndex + 1];
                    predicates.add(new Predicate.Between(columnName, lower, upper));
                    argIndex += 2;
                }
                case IN, NOT_IN -> {
                    if (args == null || argIndex >= args.length) {
                        throw new IllegalArgumentException(operator + " requires Collection parameter");
                    }
                    Object value = args[argIndex];
                    // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
                    switch (value) {
                        case Collection<?> collection -> {
                            if (operator == Predicate.Operator.NOT_IN) {
                                predicates.add(new Predicate.Not(new Predicate.In(columnName, collection)));
                            } else {
                                predicates.add(new Predicate.In(columnName, collection));
                            }
                        }
                        default -> throw new IllegalArgumentException(operator + " requires Collection parameter");
                    }
                    argIndex += 1;
                }
                default -> {
                    Object value = args != null && argIndex < args.length ? args[argIndex] : null;
                    predicates.add(new Predicate.Comparison(columnName, operator, value));
                    argIndex += 1;
                }
            }
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        // Check for OR in conditions
        String methodName = method.getName();
        if (Pattern.compile("\\bOr\\b", Pattern.CASE_INSENSITIVE).matcher(methodName).find()) {
            return new Predicate.Or(predicates);
        }
        return new Predicate.And(predicates);
    }

    /**
     * Parse a query from pre-parsed metadata and arguments.
     * This avoids re-parsing the method name on every call (used by compiled queries).
     */
    static Predicate parseFromParsed(ParsedQueryResult result, Object[] args) {
        List<Predicate> predicates = new ArrayList<>();
        int argIndex = 0;

        for (int i = 0; i < result.conditions().size(); i++) {
            String columnName = result.conditions().get(i);
            Predicate.Operator operator = result.operators().get(i);

            switch (operator) {
                case BETWEEN -> {
                    if (args == null || argIndex + 1 >= args.length) {
                        throw new IllegalArgumentException("BETWEEN requires 2 parameters");
                    }
                    Object lower = args[argIndex];
                    Object upper = args[argIndex + 1];
                    predicates.add(new Predicate.Between(columnName, lower, upper));
                    argIndex += 2;
                }
                case IN, NOT_IN -> {
                    if (args == null || argIndex >= args.length) {
                        throw new IllegalArgumentException(operator + " requires Collection parameter");
                    }
                    Object value = args[argIndex];
                    // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
                    switch (value) {
                        case Collection<?> collection -> {
                            if (operator == Predicate.Operator.NOT_IN) {
                                predicates.add(new Predicate.Not(new Predicate.In(columnName, collection)));
                            } else {
                                predicates.add(new Predicate.In(columnName, collection));
                            }
                        }
                        default -> throw new IllegalArgumentException(operator + " requires Collection parameter");
                    }
                    argIndex += 1;
                }
                default -> {
                    Object value = args != null && argIndex < args.length ? args[argIndex] : null;
                    predicates.add(new Predicate.Comparison(columnName, operator, value));
                    argIndex += 1;
                }
            }
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        // Default to AND (OR would need to be detected during method parsing)
        return new Predicate.And(predicates);
    }

    private enum Connector { AND, OR }

    private static List<String> splitConditions(String name) {
        List<String> conditions = new ArrayList<>();
        Pattern pattern = Pattern.compile("\\b(And|Or)\\b", Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(name);
        int lastEnd = 0;
        while (matcher.find()) {
            String before = name.substring(lastEnd, matcher.start());
            if (!before.isEmpty()) {
                conditions.add(before);
            }
            lastEnd = matcher.end();
        }
        String remaining = name.substring(lastEnd);
        if (!remaining.isEmpty()) {
            conditions.add(remaining);
        }
        return conditions;
    }

    private static Predicate.Operator detectOperator(String condition) {
        // First check for Not prefix (e.g., "NotIn", "NotLike" - negation of In/Like)
        // This is used when "Not" appears at the BEGINNING of the condition
        if (condition.startsWith("Not")) {
            String rest = condition.substring(3);
            Predicate.Operator op = detectOperatorInternal(rest);
            return switch (op) {
                case EQ -> Predicate.Operator.NEQ;
                case IN -> Predicate.Operator.NOT_IN;
                case LIKE -> Predicate.Operator.NOT_LIKE;
                case CONTAINING -> Predicate.Operator.NOT_LIKE;
                default -> Predicate.Operator.NEQ;
            };
        }
        return detectOperatorInternal(condition);
    }

    /**
     * Detect operator from a condition string that has no prefix.
     * This handles suffixes like "In", "Not", "True", "False", etc.
     */
    private static Predicate.Operator detectOperatorInternal(String condition) {
        // Check for compound suffixes in order (longest first)
        if (endsWithAny(condition, "GreaterThanEqual")) {
            return Predicate.Operator.GTE;
        } else if (endsWithAny(condition, "LessThanEqual")) {
            return Predicate.Operator.LTE;
        } else if (endsWithAny(condition, "GreaterThan")) {
            return Predicate.Operator.GT;
        } else if (endsWithAny(condition, "LessThan")) {
            return Predicate.Operator.LT;
        } else if (endsWithAny(condition, "Between")) {
            return Predicate.Operator.BETWEEN;
        } else if (endsWithAny(condition, "StartingWith", "StartsWith")) {
            return Predicate.Operator.STARTING_WITH;
        } else if (endsWithAny(condition, "EndingWith", "EndsWith")) {
            return Predicate.Operator.ENDING_WITH;
        } else if (endsWithAny(condition, "NotLike")) {
            return Predicate.Operator.NOT_LIKE;
        } else if (endsWithAny(condition, "NotIn")) {
            return Predicate.Operator.NOT_IN;
        } else if (endsWithAny(condition, "NotEqual")) {
            return Predicate.Operator.NEQ;
        } else if (endsWithAny(condition, "Like")) {
            return Predicate.Operator.LIKE;
        } else if (endsWithAny(condition, "Containing", "Contains")) {
            return Predicate.Operator.CONTAINING;
        } else if (endsWithAny(condition, "IsNotNull")) {
            return Predicate.Operator.IS_NOT_NULL;
        } else if (endsWithAny(condition, "NotNull")) {
            return Predicate.Operator.IS_NOT_NULL;
        } else if (endsWithAny(condition, "IsNull")) {
            return Predicate.Operator.IS_NULL;
        } else if (endsWithAny(condition, "Null")) {
            // JPA: Null without Is prefix means IS_NULL
            return Predicate.Operator.IS_NULL;
        } else if (endsWithAny(condition, "IsTrue")) {
            return Predicate.Operator.IS_TRUE;
        } else if (endsWithAny(condition, "True")) {
            return Predicate.Operator.IS_TRUE;
        } else if (endsWithAny(condition, "IsFalse")) {
            return Predicate.Operator.IS_FALSE;
        } else if (endsWithAny(condition, "False")) {
            return Predicate.Operator.IS_FALSE;
        } else if (endsWithAny(condition, "After")) {
            return Predicate.Operator.AFTER;
        } else if (endsWithAny(condition, "Before")) {
            return Predicate.Operator.BEFORE;
        } else if (endsWithAny(condition, "AllIgnoreCase")) {
            return Predicate.Operator.IGNORE_CASE;
        } else if (endsWithAny(condition, "IgnoreCase")) {
            return Predicate.Operator.IGNORE_CASE;
        } else if (endsWithAny(condition, "Equals")) {
            // JPA: Equals is equivalent to Is (both mean EQ)
            return Predicate.Operator.EQ;
        } else if (endsWithAny(condition, "Is")) {
            // JPA: Is means EQ (e.g., findByEmailIs -> findByEmail)
            return Predicate.Operator.EQ;
        }
        // "In" must be checked after compound suffixes
        else if (endsWithAny(condition, "In")) {
            return Predicate.Operator.IN;
        }
        // "Not" suffix (checked after "NotIn", "NotLike", "NotEqual", etc.) means NEQ
        else if (endsWithAny(condition, "Not")) {
            return Predicate.Operator.NEQ;
        }

        return Predicate.Operator.EQ;
    }

    /**
     * Extract the property name from a condition by stripping the operator suffix.
     * For example: "LastnameNot" -> "Lastname", "StatusIn" -> "Status", "ActiveTrue" -> "Active"
     */
    private static String extractPropertyName(String condition) {
        // Handle Not prefix first
        if (condition.startsWith("Not")) {
            String rest = condition.substring(3);
            String suffix = extractOperatorSuffix(rest);
            return rest.substring(0, rest.length() - suffix.length());
        }
        String suffix = extractOperatorSuffix(condition);
        if (suffix.isEmpty()) {
            // No operator suffix found, return the whole condition
            return condition;
        }
        return condition.substring(0, condition.length() - suffix.length());
    }

    /**
     * Extract just the operator suffix from a condition.
     * Returns empty string if no operator suffix is found.
     */
    private static String extractOperatorSuffix(String condition) {
        // Check for compound suffixes in order (longest first)
        if (endsWithAny(condition, "GreaterThanEqual")) {
            return "GreaterThanEqual";
        } else if (endsWithAny(condition, "LessThanEqual")) {
            return "LessThanEqual";
        } else if (endsWithAny(condition, "GreaterThan")) {
            return "GreaterThan";
        } else if (endsWithAny(condition, "LessThan")) {
            return "LessThan";
        } else if (endsWithAny(condition, "Between")) {
            return "Between";
        } else if (endsWithAny(condition, "StartingWith", "StartsWith")) {
            return "StartingWith";
        } else if (endsWithAny(condition, "EndingWith", "EndsWith")) {
            return "EndingWith";
        } else if (endsWithAny(condition, "NotLike")) {
            return "NotLike";
        } else if (endsWithAny(condition, "NotIn")) {
            return "NotIn";
        } else if (endsWithAny(condition, "NotEqual")) {
            return "NotEqual";
        } else if (endsWithAny(condition, "Like")) {
            return "Like";
        } else if (endsWithAny(condition, "Containing", "Contains")) {
            return "Containing";
        } else if (endsWithAny(condition, "IsNotNull")) {
            return "IsNotNull";
        } else if (endsWithAny(condition, "NotNull")) {
            return "NotNull";
        } else if (endsWithAny(condition, "IsNull")) {
            return "IsNull";
        } else if (endsWithAny(condition, "Null")) {
            // JPA supports both "IsNull" and "Null" (without Is prefix)
            return "Null";
        } else if (endsWithAny(condition, "IsTrue")) {
            return "IsTrue";
        } else if (endsWithAny(condition, "True")) {
            return "True";
        } else if (endsWithAny(condition, "IsFalse")) {
            return "IsFalse";
        } else if (endsWithAny(condition, "False")) {
            return "False";
        } else if (endsWithAny(condition, "After")) {
            return "After";
        } else if (endsWithAny(condition, "Before")) {
            return "Before";
        } else if (endsWithAny(condition, "AllIgnoreCase")) {
            return "AllIgnoreCase";
        } else if (endsWithAny(condition, "IgnoreCase")) {
            return "IgnoreCase";
        } else if (endsWithAny(condition, "Equals")) {
            // JPA: Equals is equivalent to Is (both mean EQ)
            return "Equals";
        } else if (endsWithAny(condition, "Is")) {
            // JPA: Is means EQ (e.g., findByEmailIs -> findByEmail)
            return "Is";
        } else if (endsWithAny(condition, "Not")) {
            // Not suffix for NEQ (e.g., "StatusNot" -> property "Status", operator NEQ)
            return "Not";
        } else if (endsWithAny(condition, "In")) {
            return "In";
        }
        return "";
    }

    private static boolean endsWithAny(String s, String... suffixes) {
        for (String suffix : suffixes) {
            if (s.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private static String toColumnName(String s) {
        if (s == null || s.isEmpty()) return s;

        // Convert camelCase to snake_case for database column name
        StringBuilder result = new StringBuilder();
        boolean prevUpper = false;

        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);

            if (Character.isUpperCase(c)) {
                if (i > 0 && !prevUpper) {
                    result.append('_');
                }
                result.append(Character.toLowerCase(c));
                prevUpper = true;
            } else {
                result.append(c);
                prevUpper = false;
            }
        }

        return result.toString();
    }
}
