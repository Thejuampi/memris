package io.memris.spring.plan;

import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToOne;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class QueryMethodLexer {

    private QueryMethodLexer() { }

    /**
     * NOTE:
     * - Keep operators longest-first to avoid premature matches (e.g. GreaterThan before GreaterThanEqual).
     * - Do not include And/Or in OPERATORS; they are handled as combinators.
     */
    private static final String[] OPERATORS = {
            "GreaterThanEqual", "LessThanEqual",
            "StartingWith", "EndingWith", "Containing",
            "GreaterThan", "LessThan",
            "Between",
            "IgnoreCase",
            "NotLike",
            "NotIn",
            "NotNull",
            "NotEqual",
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

    /** Spring-Data-like prefixes (case-insensitive matching) */
    private static final String[] PREFIXES = {"find", "read", "query", "count", "exists", "delete", "get"};

    private static final ConcurrentMap<Class<?>, EntityMetadata> ENTITY_METADATA_CACHE = new ConcurrentHashMap<>();

    private static final class EntityMetadata {
        final Map<String, Field> fields;              // key: lower-case field name
        final Map<String, String> relatedEntityTypes; // key: lower-case field name -> fqcn

        EntityMetadata(Map<String, Field> fields, Map<String, String> relatedEntityTypes) {
            this.fields = fields;
            this.relatedEntityTypes = relatedEntityTypes;
        }

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
                boolean isRelationship =
                        field.isAnnotationPresent(ManyToOne.class) ||
                        field.isAnnotationPresent(OneToOne.class);

                // Embedded/value-object traversal support
                boolean isEmbedded =
                        field.isAnnotationPresent(Embedded.class) ||
                        fieldType.isAnnotationPresent(Embeddable.class);

                // Entity traversal support (type-level annotation)
                boolean isEntityType = fieldType.isAnnotationPresent(Entity.class);

                if (isRelationship || isEmbedded || isEntityType) {
                    relatedTypes.put(fieldName.toLowerCase(), fieldType.getName());
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
            for (Field f : declared) {
                fields.add(f);
            }
            current = current.getSuperclass();
        }

        return fields.toArray(new Field[0]);
    }

    public static List<QueryMethodToken> tokenize(String methodName) {
        return tokenize(null, methodName);
    }

    public static List<QueryMethodToken> tokenize(Class<?> entityClass, String methodName) {
        if (methodName == null || methodName.isBlank()) {
            throw new IllegalArgumentException("methodName required");
        }

        List<QueryMethodToken> tokens = new ArrayList<>();

        // Prefix is case-insensitive but we preserve original substring for spans/value
        String prefix = extractPrefix(methodName);
        String remaining = methodName.substring(prefix.length());
        int baseOffset = prefix.length(); // absolute offset in original methodName

        // Only add prefix token if entity class is provided (context-aware mode)
        // Legacy mode (entityClass == null) returns only predicate tokens for backward compatibility
        if (entityClass != null) {
            QueryMethodTokenType prefixType = mapPrefixToType(prefix);
            tokens.add(new QueryMethodToken(prefixType, prefix, 0, prefix.length(), false));
        }

        if (remaining.isEmpty()) {
            return tokens;
        }

        // Extract OrderBy (if any) before parsing predicates
        String orderBy = extractOrderBy(remaining);
        boolean hasOrderBy = (orderBy != null);
        String predicatePart = removeOrderBy(remaining, orderBy);

        if ("All".equals(predicatePart)) {
            tokens.add(new QueryMethodToken(QueryMethodTokenType.FIND_ALL, "All", baseOffset, baseOffset + 3, false));
            // OrderBy after All is unusual but allow it
            if (hasOrderBy) {
                addOrderByTokens(tokens, orderBy, entityClass != null ? extractEntityMetadata(entityClass) : null, baseOffset + predicatePart.length());
            }
            return tokens;
        }

        EntityMetadata metadata = (entityClass != null) ? extractEntityMetadata(entityClass) : null;

        // Parse "By..."
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
                            input,
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
            String input,
            int start,
            int end,
            String value,
            EntityMetadata metadata,
            int absoluteBaseOffset
    ) {
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
     * - If that field is a related/embedded/entity type, continue resolving remainder against that type metadata
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
                    // Could not resolve; append the rest as a single segment (lowercased) and stop this part
                    if (!firstOut) resolved.append('.');
                    resolved.append(remaining.toLowerCase());
                    firstOut = false;
                    break;
                }

                if (!firstOut) resolved.append('.');
                resolved.append(m.fieldNameLower);
                firstOut = false;

                // Move into related type if present (for nested paths)
                String relType = current.getRelatedEntityType(m.fieldNameLower);
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

    private record Match(String fieldNameLower, String rest) { }

    private static Match longestFieldPrefixMatch(EntityMetadata metadata, String camel) {
        if (metadata == null || camel == null || camel.isEmpty()) {
            return null;
        }

        String lower = camel.toLowerCase();
        String best = null;

        // longest prefix match across known fields
        for (String fieldNameLower : metadata.fields.keySet()) {
            if (lower.startsWith(fieldNameLower)) {
                if (best == null || fieldNameLower.length() > best.length()) {
                    best = fieldNameLower;
                }
            }
        }

        if (best == null) {
            return null;
        }

        String rest = camel.substring(best.length());

        // If remainder exists, it must start with a capital letter or be empty
        // This prevents matching "age" in "age2" and leaving "2" as remainder
        if (!rest.isEmpty() && !Character.isUpperCase(rest.charAt(0))) {
            return null;
        }

        return new Match(best, rest);
    }

    private static String extractPrefix(String methodName) {
        String lower = methodName.toLowerCase();
        for (String prefix : PREFIXES) {
            if (lower.startsWith(prefix)) {
                return methodName.substring(0, prefix.length());
            }
        }
        // no prefix match -> treat entire name as "prefix" (caller will likely fail on mapPrefixToType)
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
        String firstToken = findTokenAtStart(input, start);
        if (firstToken != null) {
            return start + firstToken.length();
        }

        int earliest = input.length();

        // Find the earliest operator occurrence after start
        for (String operator : OPERATORS) {
            int idx = input.indexOf(operator, start);
            if (idx != -1 && idx < earliest) {
                earliest = idx;
            }
        }

        // If no operator found, stop at next And/Or or end
        if (earliest == input.length()) {
            return findPropertyEnd(input, start);
        }

        // If operator begins at start, it's the token
        if (earliest == start) {
            return start + findTokenAtStart(input, start).length();
        }

        // Otherwise property runs until operator/combinator
        return earliest;
    }

    private static String findTokenAtStart(String input, int start) {
        if (input.startsWith("And", start)) return "And";
        if (input.startsWith("Or", start)) return "Or";
        if (input.startsWith("OrderBy", start)) return "OrderBy";

        for (String operator : OPERATORS) {
            if (input.startsWith(operator, start)) return operator;
        }
        return null;
    }

    private static int findPropertyEnd(String input, int start) {
        int andIdx = input.indexOf("And", start);
        int orIdx = input.indexOf("Or", start);
        int end = findMinIndex(andIdx, orIdx);
        return (end == -1) ? input.length() : end;
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
            int orderByAbsoluteStart
    ) {
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
                String resolvedProperty = (metadata != null) ? resolvePropertyPath(metadata, property) : property.toLowerCase();
                tokens.add(new QueryMethodToken(QueryMethodTokenType.PROPERTY_PATH, resolvedProperty,
                        base + segmentStart, base + remaining.length(), false));
                break;
            }

            // Property part up to direction
            String property = remaining.substring(segmentStart, nextDir);
            String resolvedProperty = (metadata != null) ? resolvePropertyPath(metadata, property) : property.toLowerCase();
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

    private static QueryMethodTokenType mapPrefixToType(String prefix) {
        return switch (prefix.toLowerCase()) {
            case "find", "read", "query", "get" -> QueryMethodTokenType.FIND_BY;
            case "count" -> QueryMethodTokenType.COUNT;
            case "exists" -> QueryMethodTokenType.EXISTS_BY;
            case "delete" -> QueryMethodTokenType.DELETE_BY;
            default -> throw new IllegalArgumentException("Unknown prefix: " + prefix);
        };
    }
}
