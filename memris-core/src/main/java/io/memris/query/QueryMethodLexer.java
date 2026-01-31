package io.memris.query;

import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class QueryMethodLexer {

    /**
     * Built-in method signatures that map directly to operations.
     * <p>
     * Uses MethodKey (name + parameter types) for exact signature matching.
     * This correctly handles overloads like deleteById(Long) vs deleteById(UUID).
     * <p>
     * <b>Signature strategy:</b>
     * <ul>
     * <li>Entity CRUD (save, delete) use Object.class - matches any entity
     * type</li>
     * *
     * <li>ID operations (findById, deleteById, existsById) use IdParam marker -
     * matches any ID type</li>
     * <li>Iterable operations (saveAll) are exact - Iterable required</li>
     * <li>Zero-arg operations (findAll, count, deleteAll) are exact</li>
     * </ul>
     * <p>
     * NOTE: This is package-private for QueryPlanner access only.
     * The lexer does not use this map - it's used by the planner which has
     * access to the full Method object for signature matching.
     * <p>
     * <b>Reserved operations:</b> Keys marked as "reserved" are for future Spring
     * Data compatibility.
     * They are defined now to prevent the Object wildcard from accidentally
     * capturing them later.
     * These will throw "not yet implemented" at runtime until implemented.
     */
    static final java.util.Map<io.memris.repository.RepositoryMethodIntrospector.MethodKey, OpCode> BUILT_INS;
    static final java.util.Map<io.memris.repository.RepositoryMethodIntrospector.MethodKey, OpCode> RESERVED_BUILT_INS;

    static {
        // Active built-ins (implemented)
        BUILT_INS = java.util.Map.ofEntries(
                // save(T entity) - any entity type
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("save",
                                java.util.List.of(Object.class)),
                        OpCode.SAVE_ONE),
                // saveAll(Iterable<T> entities) - exact, must be Iterable
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("saveAll",
                                java.util.List.of(Iterable.class)),
                        OpCode.SAVE_ALL),
                // delete(T entity) - any entity type
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("delete",
                                java.util.List.of(Object.class)),
                        OpCode.DELETE_ONE),
                // deleteAll()
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("deleteAll",
                                java.util.List.of()),
                        OpCode.DELETE_ALL),
                // deleteById(ID id) - any ID type (using IdParam marker)
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("deleteById",
                                java.util.List.of(IdParam.class)),
                        OpCode.DELETE_BY_ID),
                // deleteAllById(Iterable<ID>) - Spring Data JPA compatible
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("deleteAllById",
                                java.util.List.of(Iterable.class)),
                        OpCode.DELETE_ALL_BY_ID),
                // findById(ID id) - any ID type (using IdParam marker)
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("findById",
                                java.util.List.of(IdParam.class)),
                        OpCode.FIND_BY_ID),
                // findAllById(Iterable<ID>) - Spring Data JPA compatible
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("findAllById",
                                java.util.List.of(Iterable.class)),
                        OpCode.FIND_ALL_BY_ID),
                // existsById(ID id) - any ID type (using IdParam marker)
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("existsById",
                                java.util.List.of(IdParam.class)),
                        OpCode.EXISTS_BY_ID),
                // findAll()
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("findAll", java.util.List.of()),
                        OpCode.FIND_ALL),
                // count()
                java.util.Map.entry(
                        new io.memris.repository.RepositoryMethodIntrospector.MethodKey("count", java.util.List.of()),
                        OpCode.COUNT_ALL));

        // Reserved built-ins (future Spring Data compatibility)
        // These are defined now to prevent the Object wildcard from accidentally
        // capturing them later.
        // They will throw "not yet implemented" at runtime until implemented.
        // Examples: findAll(Sort), queryByDsl(...)
        RESERVED_BUILT_INS = java.util.Map.ofEntries();
    }

    private QueryMethodLexer() {
    }

    /**
     * NOTE:
     * - Keep operators longest-first to avoid premature matches (e.g. GreaterThan
     * before GreaterThanEqual).
     * - Do not include And/Or in OPERATORS; they are handled as combinators.
     */
    private static final String[] OPERATORS = {
            "GreaterThanEqual", "LessThanEqual",
            "NotStartingWith", "NotEndingWith", "NotContaining",
            "StartingWith", "EndingWith", "Containing",
            "GreaterThan", "LessThan",
            "Between",
            "IgnoreCase",
            "NotLike",
            "NotIn",
            "NotNull",
            "NotEqual",
            "Not",
            "Like",
            "Equals",
            "Before",
            "After",
            "IsNull",
            "In",
            "Is",
            "True",
            "False"
    };

    /**
     * Spring-Data-like prefixes (case-insensitive matching).
     * <p>
     * Includes "save" for prefix extraction. Built-in methods are matched
     * by exact name first, then derived queries use prefix matching.
     */
    static final String[] PREFIXES = { "find", "read", "query", "count", "exists", "delete", "save", "get" };

    private static final ConcurrentMap<Class<?>, EntityMetadata> ENTITY_METADATA_CACHE = new ConcurrentHashMap<>();

    /**
     * @param fields             key: lower-case field name
     * @param relatedEntityTypes key: lower-case field name -> fqcn
     */
    private record EntityMetadata(Map<String, Field> fields, Map<String, String> relatedEntityTypes) {

        Field getField(String name) {
                return fields.get(name.toLowerCase());
            }

            boolean hasField(String name) {
                return fields.containsKey(name.toLowerCase());
            }

            String getRelatedEntityType(String fieldName) {
                return relatedEntityTypes.get(fieldName.toLowerCase());
            }
        }

    private static EntityMetadata extractEntityMetadata(Class<?> entityClass) {
        return ENTITY_METADATA_CACHE.computeIfAbsent(entityClass, clazz -> {
            Map<String, Field> fields = new ConcurrentHashMap<>();
            Map<String, String> relatedTypes = new ConcurrentHashMap<>();

            for (Field field : getAllFields(clazz)) {
                String fieldName = field.getName();
                fields.put(fieldName.toLowerCase(), field);

                Class<?> fieldType = field.getType();

                // Relationship / traversal support: check annotations on FIELD (not on type)
                boolean isRelationship = field.isAnnotationPresent(ManyToOne.class) ||
                        field.isAnnotationPresent(OneToOne.class) ||
                        field.isAnnotationPresent(jakarta.persistence.OneToMany.class) ||
                        field.isAnnotationPresent(jakarta.persistence.ManyToMany.class) ||
                        field.isAnnotationPresent(io.memris.core.ManyToOne.class) ||
                        field.isAnnotationPresent(io.memris.core.OneToOne.class) ||
                        field.isAnnotationPresent(io.memris.core.OneToMany.class) ||
                        field.isAnnotationPresent(io.memris.core.ManyToMany.class);

                // Embedded/value-object traversal support
                boolean isEmbedded = field.isAnnotationPresent(Embedded.class) ||
                        fieldType.isAnnotationPresent(Embeddable.class);

                // Entity traversal support (type-level annotation)
                boolean isEntityType = fieldType.isAnnotationPresent(Entity.class)
                        || fieldType.isAnnotationPresent(io.memris.core.Entity.class);

                if (isRelationship || isEmbedded || isEntityType) {
                    Class<?> relatedType = resolveRelatedType(field, fieldType);
                    if (relatedType != null) {
                        relatedTypes.put(fieldName.toLowerCase(), relatedType.getName());
                    }
                }
            }

            return new EntityMetadata(fields, relatedTypes);
        });
    }

    private static Field[] getAllFields(Class<?> clazz) {
        List<Field> fields = new ArrayList<>();
        Class<?> current = clazz;

        while (current != null && current != Object.class) {
            Field[] declared = current.getDeclaredFields();
            Collections.addAll(fields, declared);
            current = current.getSuperclass();
        }

        return fields.toArray(new Field[0]);
    }

    private static Class<?> resolveRelatedType(Field field, Class<?> fieldType) {
        if (java.util.Collection.class.isAssignableFrom(fieldType)) {
            java.lang.reflect.Type genericType = field.getGenericType();
            if (genericType instanceof java.lang.reflect.ParameterizedType pt) {
                java.lang.reflect.Type[] args = pt.getActualTypeArguments();
                if (args.length > 0 && args[0] instanceof Class<?>) {
                    return (Class<?>) args[0];
                }
            }
            return null;
        }
        return fieldType;
    }

    public static List<QueryMethodToken> tokenize(String methodName) {
        return tokenize(null, methodName);
    }

    public static List<QueryMethodToken> tokenize(Class<?> entityClass, String methodName) {
        if (methodName == null || methodName.isBlank()) {
            throw new IllegalArgumentException("methodname required");
        }

        // Check for parameterless built-ins first
        // These are common operations that can be detected by name alone
        OpCode builtIn = getParameterlessBuiltIn(methodName);
        if (builtIn != null) {
            return List.of(new QueryMethodToken(QueryMethodTokenType.OPERATION, builtIn.name(), 0, methodName.length(),
                    false));
        }

        // Derived query parsing (findByXxx, countByXxx, deleteByXxx, etc.)
        return tokenizeDerived(entityClass, methodName);
    }

    /**
     * Check if a method name is a parameterless built-in operation.
     * <p>
     * These operations have no parameters and can be detected by name alone:
     * findAll(), count(), deleteAll()
     *
     * @param methodName the method name to check
     * @return the OpCode if it's a parameterless built-in, null otherwise
     */
    private static OpCode getParameterlessBuiltIn(String methodName) {
        return switch (methodName) {
            case "findAll" -> OpCode.FIND_ALL;
            case "count" -> OpCode.COUNT_ALL;
            case "deleteAll" -> OpCode.DELETE_ALL;
            default -> null;
        };
    }

    /**
     * Tokenize derived query methods (findByXxx, countByXxx, deleteByXxx, etc.)
     * <p>
     * Built-in methods (save, findById, findAll, etc.) are detected by QueryPlanner
     * before calling this method, using MethodKey for exact signature matching.
     */
    private static List<QueryMethodToken> tokenizeDerived(Class<?> entityClass, String methodName) {
        List<QueryMethodToken> tokens = new ArrayList<>();

        // Extract prefix (case-insensitive)
        String prefix = extractPrefix(methodName);
        String remaining = methodName.substring(prefix.length());

        // Generic operation classification based on prefix + remaining
        if (entityClass != null) {
            QueryMethodTokenType operationType = classifyOperation(prefix, remaining);
            tokens.add(new QueryMethodToken(operationType, prefix, 0, prefix.length(), false));
        }

        if (remaining.isEmpty()) {
            return tokens;
        }

        // If remaining is "All", we're done (operation type already classified)
        if ("All".equals(remaining)) {
            return tokens;
        }

        // Process the remaining part (predicates, OrderBy, etc.)
        return processRemaining(tokens, remaining, prefix.length(), entityClass);
    }

    /**
     * Classify the operation type based on prefix and what follows.
     * <p>
     * This is for DERIVED queries only. Built-in detection is handled by
     * QueryPlanner
     * using MethodKey for exact signature matching.
     * <p>
     * The lexer is lenient - it tokenizes what it sees and lets the planner
     * handle semantic validation. This allows the planner to distinguish between
     * built-ins (save, delete, etc.) and derived queries using full method
     * signatures.
     */
    private static QueryMethodTokenType classifyOperation(String prefix, String remaining) {
        boolean hasBy = remaining.startsWith("By");
        boolean isAll = "All".equals(remaining);
        boolean isAllStarting = remaining.startsWith("All");

        return switch (prefix.toLowerCase()) {
            case "find", "read", "query", "get" -> {
                // findAll() or findByXxx()
                if (isAll || hasBy || remaining.isEmpty() || isAllStarting)
                    yield QueryMethodTokenType.FIND_BY;
                throw new IllegalArgumentException("Invalid find method: expected 'By' or 'All', got: " + remaining);
            }
            case "count" -> {
                // count() or countByXxx()
                yield QueryMethodTokenType.COUNT_BY;
            }
            case "exists" -> {
                // exists() or existsByXxx()
                yield QueryMethodTokenType.EXISTS_BY;
            }
            case "delete" -> {
                // delete(), deleteAll(), or deleteByXxx()
                if (hasBy)
                    yield QueryMethodTokenType.DELETE_BY;
                yield QueryMethodTokenType.DELETE; // delete() or deleteAll()
            }
            case "save" -> {
                // save() or saveAll()
                yield QueryMethodTokenType.DELETE; // Reuse DELETE token as placeholder (no SAVE token type)
            }
            default -> throw new IllegalArgumentException("Unknown prefix: " + prefix);
        };
    }

    /**
     * Process the remaining part after the prefix.
     */
    private static List<QueryMethodToken> processRemaining(
            List<QueryMethodToken> tokens,
            String remaining,
            int baseOffset,
            Class<?> entityClass) {

        // Check for "All" (special case)
        // Note: "findAll" and "count" are handled as built-ins, but we may still
        // reach here for derived queries that shouldn't have "All" alone
        if ("All".equals(remaining)) {
            // This shouldn't happen for derived queries (findBy...All doesn't exist)
            // But if it does, treat as a property for now
            return tokens;
        }

        // Extract OrderBy (if any) before parsing predicates
        String orderBy = extractOrderBy(remaining);
        boolean hasOrderBy = (orderBy != null);
        String predicatePart = removeOrderBy(remaining, orderBy);

        EntityMetadata metadata = (entityClass != null) ? extractEntityMetadata(entityClass) : null;

        // Parse "By..." for query methods
        if (predicatePart.startsWith("By")) {
            int byOffset = baseOffset + 2;
            String input = predicatePart.substring(2);

            int pos = 0;
            while (pos < input.length()) {
                int tokenEnd = findTokenEnd(input, pos);
                if (tokenEnd <= pos) {
                    tokenEnd = input.length();
                }

                String tokenValue = input.substring(pos, tokenEnd);
                if (!tokenValue.isEmpty()) {
                    QueryMethodToken token = createToken(
                            pos,
                            tokenEnd,
                            tokenValue,
                            metadata,
                            byOffset // absolute base for spans
                    );
                    tokens.add(token);
                    pos = tokenEnd;
                } else {
                    // Safety to avoid infinite loops
                    pos++;
                }
            }
        }

        if (hasOrderBy) {
            // base for orderBy is at the start of "OrderBy" in remaining
            int orderByStartInRemaining = remaining.indexOf("OrderBy");
            int orderByAbsBase = baseOffset + Math.max(orderByStartInRemaining, 0);
            addOrderByTokens(tokens, orderBy, metadata, orderByAbsBase);
        }

        return tokens;
    }

    private static QueryMethodToken createToken(
            int start,
            int end,
            String value,
            EntityMetadata metadata,
            int absoluteBaseOffset) {
        QueryMethodTokenType type = determineTokenType(value);
        boolean ignoreCase = (type == QueryMethodTokenType.OPERATOR && "IgnoreCase".equals(value));
        String normalizedValue = normalizePropertyValue(type, value, metadata);

        int absStart = absoluteBaseOffset + start;
        int absEnd = absoluteBaseOffset + end;

        return new QueryMethodToken(type, normalizedValue, absStart, absEnd, ignoreCase);
    }

    private static QueryMethodTokenType determineTokenType(String value) {
        if (isCombinatorToken(value)) {
            return "And".equals(value) ? QueryMethodTokenType.AND : QueryMethodTokenType.OR;
        }
        if (isOperatorToken(value)) {
            return QueryMethodTokenType.OPERATOR;
        }
        if (isAscendingToken(value)) {
            return QueryMethodTokenType.ASC;
        }
        if (isDescendingToken(value)) {
            return QueryMethodTokenType.DESC;
        }
        return QueryMethodTokenType.PROPERTY_PATH;
    }

    private static String normalizePropertyValue(QueryMethodTokenType type, String value, EntityMetadata metadata) {
        if (type != QueryMethodTokenType.PROPERTY_PATH) {
            return value;
        }
        if (metadata == null) {
            return value.toLowerCase();
        }
        return resolvePropertyPath(metadata, value);
    }

    /**
     * Attempts Spring-Data-like resolution:
     * - First try to match the longest field name prefix on current metadata
     * - If that field is a related/embedded/entity type, continue resolving
     * remainder against that type metadata
     * - If no match exists, fall back to lowercasing the remainder
     *
     * Also supports "_" as an explicit segment boundary (common in your examples).
     */
    private static String resolvePropertyPath(EntityMetadata root, String propertyPath) {
        if (propertyPath == null || propertyPath.isEmpty()) {
            return propertyPath;
        }

        String[] underscoreParts = propertyPath.split("_");
        StringBuilder resolved = new StringBuilder();

        EntityMetadata current = root;
        boolean firstOut = true;

        for (String part : underscoreParts) {
            String remaining = part;

            while (!remaining.isEmpty()) {
                Match m = longestFieldPrefixMatch(current, remaining);

                if (m == null) {
                    // Could not resolve; append the rest as a single segment (lowercased) and stop
                    // this part
                    if (!firstOut)
                        resolved.append('.');
                    resolved.append(remaining.toLowerCase());
                    firstOut = false;
                    break;
                }

                if (!firstOut)
                    resolved.append('.');
                resolved.append(m.fieldName);
                firstOut = false;

                // Move into related type if present (for nested paths)
                String relType = current.getRelatedEntityType(m.fieldName);
                if (relType != null && !m.rest.isEmpty()) {
                    current = safeLoadMetadata(relType, current);
                }

                remaining = m.rest;
            }
        }

        return resolved.toString();
    }

    private static EntityMetadata safeLoadMetadata(String fqcn, EntityMetadata fallback) {
        try {
            Class<?> related = Class.forName(fqcn);
            return extractEntityMetadata(related);
        } catch (ClassNotFoundException e) {
            return fallback;
        }
    }

    private record Match(String fieldName, String rest) {
    }

    private static Match longestFieldPrefixMatch(EntityMetadata metadata, String camel) {
        if (metadata == null || camel == null || camel.isEmpty()) {
            return null;
        }

        String lower = camel.toLowerCase();
        String bestKey = null;
        String bestFieldName = null;

        // longest prefix match across known fields
        for (Map.Entry<String, Field> entry : metadata.fields.entrySet()) {
            String fieldNameLower = entry.getKey(); // lowercase key
            if (lower.startsWith(fieldNameLower)) {
                if (bestKey == null || fieldNameLower.length() > bestKey.length()) {
                    bestKey = fieldNameLower;
                    bestFieldName = entry.getValue().getName(); // actual field name with proper case
                }
            }
        }

        if (bestKey == null) {
            return null;
        }

        String rest = camel.substring(bestKey.length());

        // If remainder exists, it must start with a capital letter or be empty
        // This prevents matching "age" in "age2" and leaving "2" as remainder
        if (!rest.isEmpty() && !Character.isUpperCase(rest.charAt(0))) {
            return null;
        }

        return new Match(bestFieldName, rest);
    }

    private static String extractPrefix(String methodName) {
        String lower = methodName.toLowerCase();
        for (String prefix : PREFIXES) {
            if (lower.startsWith(prefix)) {
                return methodName.substring(0, prefix.length());
            }
        }
        // no prefix match -> treat entire name as "prefix" (caller will likely fail on
        // mapPrefixToType)
        return methodName;
    }

    private static String extractOrderBy(String methodNameSuffix) {
        int orderByIdx = methodNameSuffix.indexOf("OrderBy");
        if (orderByIdx == -1) {
            return null;
        }
        return methodNameSuffix.substring(orderByIdx); // includes "OrderBy..."
    }

    private static String removeOrderBy(String methodNameSuffix, String orderBy) {
        if (orderBy == null) {
            return methodNameSuffix;
        }
        int orderByIdx = methodNameSuffix.indexOf("OrderBy");
        if (orderByIdx == -1) {
            return methodNameSuffix;
        }
        return methodNameSuffix.substring(0, orderByIdx);
    }

    private static int findTokenEnd(String input, int start) {
        // If we're at the start of a known token (combinator/operator), return its
        // length
        String firstToken = findTokenAtStart(input, start);
        if (firstToken != null) {
            return start + firstToken.length();
        }

        // Otherwise we're in a property - find where it ends (next known token or end
        // of string)
        return findNextTokenStart(input, start);
    }

    /**
     * Find the position of the next known token (combinator or operator) after
     * start.
     * Used to determine where a property path ends.
     */
    private static int findNextTokenStart(String input, int start) {
        int earliest = input.length();

        // Check for OrderBy first (before checking for "Or")
        int orderByIdx = input.indexOf("OrderBy", start);
        if (orderByIdx != -1) {
            earliest = orderByIdx;
        }

        // Check for "And" combinator - only valid if followed by uppercase or end
        int andIdx = findCombinatorIndex(input, start, "And");
        if (andIdx != -1 && andIdx < earliest) {
            earliest = andIdx;
        }

        // Check for "Or" combinator - only valid if followed by uppercase or end
        // This prevents "Order" from matching "Or" in the middle
        int orIdx = findCombinatorIndex(input, start, "Or");
        if (orIdx != -1 && orIdx < earliest) {
            earliest = orIdx;
        }

        // Check for operators
        for (String operator : OPERATORS) {
            int idx = input.indexOf(operator, start);
            if (idx != -1 && idx < earliest) {
                earliest = idx;
            }
        }

        return earliest;
    }

    /**
     * Find the index of a combinator ("And" or "Or") only if it's valid.
     * A combinator is valid only if followed by an uppercase letter or end of
     * string.
     * This prevents "Order" from matching "Or" or "Anderson" from matching "And".
     */
    private static int findCombinatorIndex(String input, int start, String combinator) {
        int idx = input.indexOf(combinator, start);
        if (idx == -1) {
            return -1;
        }

        int afterCombinator = idx + combinator.length();
        // Valid if at end of string or followed by uppercase letter
        if (afterCombinator >= input.length() || Character.isUpperCase(input.charAt(afterCombinator))) {
            return idx;
        }

        // Not a valid combinator - find next occurrence
        return findCombinatorIndex(input, idx + 1, combinator);
    }

    private static int findMinIndex(int... idxs) {
        int min = Integer.MAX_VALUE;
        boolean found = false;
        for (int idx : idxs) {
            if (idx >= 0 && idx < min) {
                min = idx;
                found = true;
            }
        }
        return found ? min : -1;
    }

    private static String findTokenAtStart(String input, int start) {
        // Check for OrderBy first (must be before "Or" check)
        if (input.startsWith("OrderBy", start))
            return "OrderBy";

        // Check for "And" - only if followed by uppercase or end of string
        if (input.startsWith("And", start)) {
            int afterAnd = start + 3;
            if (afterAnd >= input.length() || Character.isUpperCase(input.charAt(afterAnd))) {
                return "And";
            }
        }

        // Check for "Or" - only if followed by uppercase or end of string
        // This prevents "Order" from being split as "Or" + "der"
        if (input.startsWith("Or", start)) {
            int afterOr = start + 2;
            if (afterOr >= input.length() || Character.isUpperCase(input.charAt(afterOr))) {
                return "Or";
            }
        }

        for (String operator : OPERATORS) {
            if (input.startsWith(operator, start))
                return operator;
        }
        return null;
    }

    private static boolean isOperatorToken(String value) {
        for (String operator : OPERATORS) {
            if (value.equals(operator)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isCombinatorToken(String value) {
        return "And".equals(value) || "Or".equals(value);
    }

    private static boolean isAscendingToken(String value) {
        return "Asc".equals(value);
    }

    private static boolean isDescendingToken(String value) {
        return "Desc".equals(value);
    }

    private static void addOrderByTokens(
            List<QueryMethodToken> tokens,
            String orderBy,
            EntityMetadata metadata,
            int orderByAbsoluteStart) {
        if (orderBy == null || orderBy.isEmpty()) {
            return;
        }

        // "OrderBy" is 7 chars
        tokens.add(new QueryMethodToken(QueryMethodTokenType.OPERATOR, "OrderBy",
                orderByAbsoluteStart, orderByAbsoluteStart + 7, false));

        String remaining = orderBy.substring(7); // after "OrderBy"
        int base = orderByAbsoluteStart + 7;

        if (remaining.isEmpty()) {
            return;
        }

        int pos = 0;
        int segmentStart = 0;

        while (pos < remaining.length()) {
            // Support multiple sort segments separated by And
            if (remaining.startsWith("And", pos)) {
                tokens.add(new QueryMethodToken(QueryMethodTokenType.AND, "And",
                        base + pos, base + pos + 3, false));
                pos += 3;
                segmentStart = pos;
                continue;
            }

            int ascIdx = remaining.indexOf("Asc", pos);
            int descIdx = remaining.indexOf("Desc", pos);

            int nextDir = findMinIndex(ascIdx, descIdx);

            if (nextDir == -1) {
                // No direction: treat rest as property
                String property = remaining.substring(segmentStart);
                String resolvedProperty = (metadata != null) ? resolvePropertyPath(metadata, property)
                        : property.toLowerCase();
                tokens.add(new QueryMethodToken(QueryMethodTokenType.PROPERTY_PATH, resolvedProperty,
                        base + segmentStart, base + remaining.length(), false));
                break;
            }

            // Property part up to direction
            String property = remaining.substring(segmentStart, nextDir);
            String resolvedProperty = (metadata != null) ? resolvePropertyPath(metadata, property)
                    : property.toLowerCase();
            tokens.add(new QueryMethodToken(QueryMethodTokenType.PROPERTY_PATH, resolvedProperty,
                    base + segmentStart, base + nextDir, false));

            // Direction token
            if (nextDir == ascIdx) {
                tokens.add(new QueryMethodToken(QueryMethodTokenType.ASC, "Asc",
                        base + nextDir, base + nextDir + 3, false));
                pos = nextDir + 3;
            } else {
                tokens.add(new QueryMethodToken(QueryMethodTokenType.DESC, "Desc",
                        base + nextDir, base + nextDir + 4, false));
                pos = nextDir + 4;
            }

            segmentStart = pos;
        }
    }

}
