package io.memris.query;

import io.memris.core.EntityMetadata;
import io.memris.core.MetadataExtractor;
import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.query.jpql.JpqlAst;
import io.memris.query.jpql.JpqlLexer;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class JpqlQueryParser {
    private JpqlQueryParser() {
    }

    public static LogicalQuery parse(Method method, Class<?> entityClass) {
        Query annotation = method.getAnnotation(Query.class);
        if (annotation == null) {
            throw new IllegalArgumentException("@Query annotation required");
        }
        if (annotation.nativeQuery()) {
            throw new UnsupportedOperationException("nativeQuery is not supported");
        }
        String jpql = annotation.value();
        Parser parser = new Parser(jpql);
        JpqlAst.Statement statement = parser.parseStatement();

        if (statement instanceof JpqlAst.Query ast) {
            validateEntity(ast.entityName(), entityClass);

            Map<String, String> aliasMap = buildAliasMap(ast);
            ProjectionSpec projectionSpec = resolveProjectionSpec(method, ast, aliasMap, entityClass);

            LogicalQuery.ReturnKind returnKind = projectionSpec != null
                    ? projectionSpec.returnKind
                    : resolveReturnKind(method);
            if (ast.count() && returnKind != LogicalQuery.ReturnKind.COUNT_LONG) {
                throw new IllegalArgumentException("@Query count must return long: " + method.getName());
            }
            if (!ast.count() && returnKind == LogicalQuery.ReturnKind.COUNT_LONG) {
                throw new IllegalArgumentException("@Query return type long requires count: " + method.getName());
            }

            LogicalQuery.Join[] joins = buildJoins(ast, aliasMap, entityClass);
            BindingContext bindingContext = new BindingContext(method);
            List<LogicalQuery.Condition> flattened = flattenConditions(ast.where(), aliasMap, bindingContext);

            LogicalQuery.OrderBy[] orderBy = null;
            if (!ast.orderBy().isEmpty()) {
                orderBy = new LogicalQuery.OrderBy[ast.orderBy().size()];
                for (int i = 0; i < ast.orderBy().size(); i++) {
                    JpqlAst.OrderBy order = ast.orderBy().get(i);
                    String path = resolvePath(order.path(), aliasMap);
                    orderBy[i] = LogicalQuery.OrderBy.of(path, order.ascending());
                }
            }

            OpCode opCode = switch (returnKind) {
                case COUNT_LONG -> flattened.isEmpty() ? OpCode.COUNT_ALL : OpCode.COUNT;
                case EXISTS_BOOL -> OpCode.EXISTS;
                default -> OpCode.FIND;
            };

            LogicalQuery.Condition[] conditions = flattened.toArray(new LogicalQuery.Condition[0]);
            Object[] boundValues = bindingContext.boundValues();
            int[] parameterIndices = bindingContext.parameterIndices();
            int arity = parameterIndices.length;

            // Extract limit from method name (e.g., findTop3By...)
            int limit = parseLimitFromMethodName(method.getName());

            return LogicalQuery.of(
                    opCode,
                    returnKind,
                    conditions,
                    new LogicalQuery.UpdateAssignment[0],
                    projectionSpec != null ? projectionSpec.projection : null,
                    joins,
                    orderBy,
                    limit,
                    ast.distinct(),
                    boundValues,
                    parameterIndices,
                    arity);
        }

        if (statement instanceof JpqlAst.Delete(String name, String alias, JpqlAst.Expression where1)) {
            validateEntity(name, entityClass);
            ensureModifying(method);

            BindingContext bindingContext = new BindingContext(method);
            Map<String, String> aliasMap = Map.of(alias, "");
            List<LogicalQuery.Condition> flattened = flattenConditions(where1, aliasMap, bindingContext);

            LogicalQuery.ReturnKind returnKind = resolveModifyingReturnKind(method);
            LogicalQuery.Condition[] conditions = flattened.toArray(new LogicalQuery.Condition[0]);
            Object[] boundValues = bindingContext.boundValues();
            int[] parameterIndices = bindingContext.parameterIndices();
            int arity = parameterIndices.length;

            return LogicalQuery.of(
                    OpCode.DELETE_QUERY,
                    returnKind,
                    conditions,
                    new LogicalQuery.UpdateAssignment[0],
                    null,
                    new LogicalQuery.Join[0],
                    null,
                    0,
                    false,
                    boundValues,
                    parameterIndices,
                    arity);
        }

        if (statement instanceof JpqlAst.Update(
                String entityName, String rootAlias, List<JpqlAst.Assignment> assignments1, JpqlAst.Expression where
        )) {
            validateEntity(entityName, entityClass);
            ensureModifying(method);
            if (assignments1 == null || assignments1.isEmpty()) {
                throw new IllegalArgumentException("@Query update requires SET assignments: " + method.getName());
            }

            BindingContext bindingContext = new BindingContext(method);
            Map<String, String> aliasMap = Map.of(rootAlias, "");
            List<LogicalQuery.Condition> flattened = flattenConditions(where, aliasMap, bindingContext);

            List<LogicalQuery.UpdateAssignment> assignments = new ArrayList<>(assignments1.size());
            EntityMetadata<?> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
            for (JpqlAst.Assignment assignment : assignments1) {
                String path = resolvePath(assignment.path(), aliasMap);
                if (path.contains(".")) {
                    throw new IllegalArgumentException(
                            "@Query update does not support nested paths: " + assignment.path());
                }
                if (path.equals(metadata.idColumnName())) {
                    throw new IllegalArgumentException("@Query update cannot modify ID column: " + assignment.path());
                }
                int argIndex = bindingContext.bindValue(assignment.value());
                assignments.add(new LogicalQuery.UpdateAssignment(path, argIndex));
            }

            LogicalQuery.ReturnKind returnKind = resolveModifyingReturnKind(method);
            LogicalQuery.Condition[] conditions = flattened.toArray(new LogicalQuery.Condition[0]);
            Object[] boundValues = bindingContext.boundValues();
            int[] parameterIndices = bindingContext.parameterIndices();
            int arity = parameterIndices.length;

            return LogicalQuery.of(
                    OpCode.UPDATE_QUERY,
                    returnKind,
                    conditions,
                    assignments.toArray(new LogicalQuery.UpdateAssignment[0]),
                    null,
                    new LogicalQuery.Join[0],
                    null,
                    0,
                    false,
                    boundValues,
                    parameterIndices,
                    arity);
        }

        throw new IllegalArgumentException("Unsupported @Query statement: " + jpql);
    }

    private static int parseLimitFromMethodName(String methodName) {
        // Check for TopN or FirstN patterns (similar to QueryPlanner)
        String[] prefixes = { "find", "read", "query", "get", "search" };
        for (String prefix : prefixes) {
            if (methodName.startsWith(prefix)) {
                String remaining = methodName.substring(prefix.length());
                if (remaining.startsWith("Top")) {
                    return extractNumber(remaining, 3);
                }
                if (remaining.startsWith("First")) {
                    return extractNumber(remaining, 5);
                }
            }
        }
        return 0;
    }

    private static int extractNumber(String str, int prefixLength) {
        int idx = prefixLength;
        int start = idx;
        while (idx < str.length() && Character.isDigit(str.charAt(idx))) {
            idx++;
        }
        if (idx > start) {
            return Integer.parseInt(str.substring(start, idx));
        }
        return 1; // First or Top without number defaults to 1
    }

    private static LogicalQuery.ReturnKind resolveReturnKind(Method method) {
        Class<?> returnType = method.getReturnType();
        if (Optional.class.equals(returnType)) {
            return LogicalQuery.ReturnKind.ONE_OPTIONAL;
        }
        if (List.class.isAssignableFrom(returnType)) {
            return LogicalQuery.ReturnKind.MANY_LIST;
        }
        if (java.util.Set.class.isAssignableFrom(returnType)) {
            return LogicalQuery.ReturnKind.MANY_SET;
        }
        if (returnType == boolean.class || returnType == Boolean.class) {
            return LogicalQuery.ReturnKind.EXISTS_BOOL;
        }
        if (returnType == long.class || returnType == Long.class) {
            return LogicalQuery.ReturnKind.COUNT_LONG;
        }
        return LogicalQuery.ReturnKind.MANY_LIST;
    }

    private static ProjectionSpec resolveProjectionSpec(Method method,
            JpqlAst.Query ast,
            Map<String, String> aliasMap,
            Class<?> entityClass) {
        ProjectionReturn projectionReturn = resolveProjectionReturn(method);
        if (projectionReturn == null) {
            validateSelectItemsForNonProjection(ast, method);
            return null;
        }

        if (ast.count()) {
            throw new IllegalArgumentException("@Query count cannot return a projection: " + method.getName());
        }

        List<JpqlAst.SelectItem> selectItems = ast.selectItems();
        if (selectItems == null || selectItems.isEmpty()) {
            throw new IllegalArgumentException("@Query projection requires a select list: " + method.getName());
        }

        Map<String, JpqlAst.SelectItem> byAlias = new HashMap<>();
        for (JpqlAst.SelectItem item : selectItems) {
            if (item.alias() == null || item.alias().isBlank()) {
                throw new IllegalArgumentException(
                        "@Query projection requires aliases for select items: " + method.getName());
            }
            if (byAlias.putIfAbsent(item.alias(), item) != null) {
                throw new IllegalArgumentException("Duplicate projection alias: " + item.alias());
            }
        }

        RecordComponent[] components = projectionReturn.projectionType.getRecordComponents();
        LogicalQuery.ProjectionItem[] projectionItems = new LogicalQuery.ProjectionItem[components.length];
        EntityMetadata<?> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        for (int i = 0; i < components.length; i++) {
            RecordComponent component = components[i];
            JpqlAst.SelectItem item = byAlias.remove(component.getName());
            if (item == null) {
                throw new IllegalArgumentException("Missing select alias for record component: " + component.getName());
            }
            String resolved = resolvePath(item.path(), aliasMap);
            if (resolved.isBlank()) {
                throw new IllegalArgumentException(
                        "Projection select item cannot target the root entity: " + item.path());
            }
            EntityMetadata.FieldMapping field = resolveProjectionField(metadata, resolved);
            if (!isCompatible(component.getType(), field.javaType())) {
                throw new IllegalArgumentException("Projection type mismatch for component " + component.getName());
            }
            projectionItems[i] = new LogicalQuery.ProjectionItem(component.getName(), resolved);
        }

        if (!byAlias.isEmpty()) {
            throw new IllegalArgumentException("Unknown projection aliases: " + byAlias.keySet());
        }

        return new ProjectionSpec(projectionReturn.returnKind,
                new LogicalQuery.Projection(projectionReturn.projectionType, projectionItems));
    }

    private static void validateSelectItemsForNonProjection(JpqlAst.Query ast, Method method) {
        if (ast.selectItems() == null || ast.selectItems().isEmpty()) {
            return;
        }
        if (ast.selectItems().size() > 1) {
            throw new IllegalArgumentException(
                    "@Query select list requires a projection return type: " + method.getName());
        }
        JpqlAst.SelectItem item = ast.selectItems().get(0);
        if (item.alias() != null && !item.alias().isBlank()) {
            throw new IllegalArgumentException(
                    "@Query select alias requires a projection return type: " + method.getName());
        }
        if (!item.path().equals(ast.rootAlias()) && !item.path().equals(ast.entityName())) {
            throw new IllegalArgumentException(
                    "@Query select list requires a projection return type: " + method.getName());
        }
    }

    private static ProjectionReturn resolveProjectionReturn(Method method) {
        Class<?> rawType = method.getReturnType();
        if (rawType.isRecord()) {
            throw new IllegalArgumentException(
                    "Record projections must be returned as List, Set, or Optional: " + method.getName());
        }

        java.lang.reflect.Type generic = method.getGenericReturnType();
        if (!(generic instanceof java.lang.reflect.ParameterizedType parameterized)) {
            return null;
        }
        java.lang.reflect.Type[] args = parameterized.getActualTypeArguments();
        if (args.length != 1 || !(args[0] instanceof Class<?> argClass)) {
            return null;
        }
        if (!argClass.isRecord()) {
            return null;
        }

        if (Optional.class.equals(rawType)) {
            return new ProjectionReturn(argClass, LogicalQuery.ReturnKind.ONE_OPTIONAL);
        }
        if (List.class.isAssignableFrom(rawType)) {
            return new ProjectionReturn(argClass, LogicalQuery.ReturnKind.MANY_LIST);
        }
        if (java.util.Set.class.isAssignableFrom(rawType)) {
            return new ProjectionReturn(argClass, LogicalQuery.ReturnKind.MANY_SET);
        }
        return null;
    }

    private static EntityMetadata.FieldMapping resolveProjectionField(EntityMetadata<?> metadata, String path) {
        String[] segments = path.split("\\.");
        EntityMetadata<?> current = metadata;
        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];
            EntityMetadata.FieldMapping field = findField(current, segment);
            if (field == null) {
                throw new IllegalArgumentException("Unknown projection path: " + path);
            }
            if (i < segments.length - 1) {
                if (!field.isRelationship()) {
                    throw new IllegalArgumentException("Projection path must traverse relationships: " + path);
                }
                if (field.isCollection()) {
                    throw new IllegalArgumentException("Projection paths cannot traverse collections: " + path);
                }
                current = MetadataExtractor.extractEntityMetadata(field.targetEntity());
                continue;
            }
            if (field.isRelationship()) {
                throw new IllegalArgumentException("Projection field must be a column: " + path);
            }
            return field;
        }
        throw new IllegalArgumentException("Invalid projection path: " + path);
    }

    private static EntityMetadata.FieldMapping findField(EntityMetadata<?> metadata, String name) {
        for (EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        return null;
    }

    private static boolean isCompatible(Class<?> componentType, Class<?> fieldType) {
        Class<?> boxedComponent = box(componentType);
        Class<?> boxedField = box(fieldType);
        return boxedComponent.isAssignableFrom(boxedField);
    }

    private static Class<?> box(Class<?> type) {
        if (!type.isPrimitive()) {
            return type;
        }
        return switch (type.getName()) {
            case "int" -> Integer.class;
            case "long" -> Long.class;
            case "boolean" -> Boolean.class;
            case "byte" -> Byte.class;
            case "short" -> Short.class;
            case "float" -> Float.class;
            case "double" -> Double.class;
            case "char" -> Character.class;
            default -> type;
        };
    }

    private record ProjectionReturn(Class<?> projectionType, LogicalQuery.ReturnKind returnKind) {
    }

    private record ProjectionSpec(LogicalQuery.ReturnKind returnKind, LogicalQuery.Projection projection) {
    }

    private static LogicalQuery.ReturnKind resolveModifyingReturnKind(Method method) {
        Class<?> returnType = method.getReturnType();
        if (returnType == void.class || returnType == Void.class) {
            return LogicalQuery.ReturnKind.MODIFYING_VOID;
        }
        if (returnType == int.class || returnType == Integer.class) {
            return LogicalQuery.ReturnKind.MODIFYING_INT;
        }
        if (returnType == long.class || returnType == Long.class) {
            return LogicalQuery.ReturnKind.MODIFYING_LONG;
        }
        throw new IllegalArgumentException("@Modifying query must return void, int, or long: " + method.getName());
    }

    private static void ensureModifying(Method method) {
        if (hasAnnotationByName(method, "io.memris.core.Modifying",
                "org.springframework.data.jpa.repository.Modifying")) {
            return;
        }
        throw new IllegalArgumentException("@Query update/delete requires @Modifying: " + method.getName());
    }

    private static boolean hasAnnotationByName(Method method, String... names) {
        for (var annotation : method.getAnnotations()) {
            String type = annotation.annotationType().getName();
            for (String name : names) {
                if (name.equals(type)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void validateEntity(String entityName, Class<?> entityClass) {
        String simple = entityClass.getSimpleName();
        String full = entityClass.getName();
        if (!entityName.equals(simple) && !entityName.equals(full)) {
            throw new IllegalArgumentException("Unknown entity in @Query: " + entityName);
        }
    }

    private static Map<String, String> buildAliasMap(JpqlAst.Query query) {
        Map<String, String> aliases = new HashMap<>();
        aliases.put(query.rootAlias(), "");
        for (JpqlAst.Join join : query.joins()) {
            String resolved = resolvePath(join.path(), aliases);
            if (join.alias() != null && !join.alias().isBlank()) {
                aliases.put(join.alias(), resolved);
            }
        }
        return aliases;
    }

    private static LogicalQuery.Join[] buildJoins(JpqlAst.Query query,
            Map<String, String> aliasMap,
            Class<?> entityClass) {
        if (query.joins().isEmpty()) {
            return new LogicalQuery.Join[0];
        }

        EntityMetadata<?> metadata = MetadataExtractor.extractEntityMetadata(entityClass);
        List<LogicalQuery.Join> joins = new ArrayList<>();
        for (JpqlAst.Join join : query.joins()) {
            String resolvedPath = resolvePath(join.path(), aliasMap);
            String joinProperty = resolvedPath.contains(".") ? resolvedPath.substring(0, resolvedPath.indexOf('.'))
                    : resolvedPath;

            EntityMetadata.FieldMapping relationship = findRelationship(metadata, joinProperty);
            if (relationship == null) {
                throw new IllegalArgumentException("Unknown join property: " + joinProperty);
            }

            Class<?> targetEntity = relationship.targetEntity();
            EntityMetadata<?> targetMetadata = MetadataExtractor.extractEntityMetadata(targetEntity);
            String referencedColumn = relationship.referencedColumnName() != null
                    ? relationship.referencedColumnName()
                    : targetMetadata.idColumnName();

            LogicalQuery.Join.JoinType joinType = join.type() == JpqlAst.JoinType.LEFT
                    ? LogicalQuery.Join.JoinType.LEFT
                    : LogicalQuery.Join.JoinType.INNER;

            joins.add(new LogicalQuery.Join(
                    joinProperty,
                    targetEntity,
                    relationship.columnName(),
                    referencedColumn,
                    joinType));
        }

        return joins.toArray(new LogicalQuery.Join[0]);
    }

    private static EntityMetadata.FieldMapping findRelationship(EntityMetadata<?> metadata, String property) {
        for (EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.isRelationship() && field.name().equals(property)) {
                return field;
            }
        }
        return null;
    }

    private static List<LogicalQuery.Condition> flattenConditions(JpqlAst.Expression expression,
            Map<String, String> aliasMap,
            BindingContext bindingContext) {
        if (expression == null) {
            return List.of();
        }
        JpqlAst.Expression normalized = normalize(expression);
        List<List<ConditionSpec>> dnf = toDnf(normalized, aliasMap, bindingContext);

        List<LogicalQuery.Condition> flattened = new ArrayList<>();
        for (int i = 0; i < dnf.size(); i++) {
            List<ConditionSpec> group = dnf.get(i);
            for (int j = 0; j < group.size(); j++) {
                ConditionSpec spec = group.get(j);
                LogicalQuery.Combinator combinator = (i < dnf.size() - 1 && j == group.size() - 1)
                        ? LogicalQuery.Combinator.OR
                        : LogicalQuery.Combinator.AND;
                flattened.add(spec.withCombinator(combinator).toCondition());
            }
        }
        return flattened;
    }

    private static JpqlAst.Expression normalize(JpqlAst.Expression expression) {
        if (expression instanceof JpqlAst.Not(JpqlAst.Expression expression1)) {
            return negate(expression1);
        }
        if (expression instanceof JpqlAst.And(JpqlAst.Expression left1, JpqlAst.Expression right1)) {
            return new JpqlAst.And(normalize(left1), normalize(right1));
        }
        if (expression instanceof JpqlAst.Or(JpqlAst.Expression left, JpqlAst.Expression right)) {
            return new JpqlAst.Or(normalize(left), normalize(right));
        }
        return expression;
    }

    private static JpqlAst.Expression negate(JpqlAst.Expression expression) {
        if (expression instanceof JpqlAst.Not(JpqlAst.Expression expression1)) {
            return normalize(expression1);
        }
        if (expression instanceof JpqlAst.And(JpqlAst.Expression left1, JpqlAst.Expression right1)) {
            return new JpqlAst.Or(negate(left1), negate(right1));
        }
        if (expression instanceof JpqlAst.Or(JpqlAst.Expression left, JpqlAst.Expression right)) {
            return new JpqlAst.And(negate(left), negate(right));
        }
        if (expression instanceof JpqlAst.PredicateExpr(JpqlAst.Predicate predicate)) {
            return new JpqlAst.PredicateExpr(negatePredicate(predicate));
        }
        throw new IllegalArgumentException("Unsupported NOT expression");
    }

    private static JpqlAst.Predicate negatePredicate(JpqlAst.Predicate predicate) {
        if (predicate instanceof JpqlAst.IsNull(String path2, boolean negated2)) {
            return new JpqlAst.IsNull(path2, !negated2);
        }
        if (predicate instanceof JpqlAst.In(String path1, List<JpqlAst.Value> values, boolean negated1)) {
            return new JpqlAst.In(path1, values, !negated1);
        }
        if (predicate instanceof JpqlAst.Between) {
            throw new UnsupportedOperationException("NOT BETWEEN is not supported");
        }
        if (predicate instanceof JpqlAst.Comparison(String path, JpqlAst.ComparisonOp op, JpqlAst.Value value)) {
            JpqlAst.ComparisonOp negated = switch (op) {
                case EQ -> JpqlAst.ComparisonOp.NE;
                case NE -> JpqlAst.ComparisonOp.EQ;
                case GT -> JpqlAst.ComparisonOp.LTE;
                case GTE -> JpqlAst.ComparisonOp.LT;
                case LT -> JpqlAst.ComparisonOp.GTE;
                case LTE -> JpqlAst.ComparisonOp.GT;
                case LIKE -> JpqlAst.ComparisonOp.NOT_LIKE;
                case ILIKE -> JpqlAst.ComparisonOp.NOT_ILIKE;
                case NOT_LIKE -> JpqlAst.ComparisonOp.LIKE;
                case NOT_ILIKE -> JpqlAst.ComparisonOp.ILIKE;
            };
            return new JpqlAst.Comparison(path, negated, value);
        }
        throw new IllegalArgumentException("Unsupported predicate negation");
    }

    private static List<List<ConditionSpec>> toDnf(JpqlAst.Expression expression,
            Map<String, String> aliasMap,
            BindingContext bindingContext) {
        if (expression instanceof JpqlAst.PredicateExpr(JpqlAst.Predicate predicate)) {
            ConditionSpec spec = toConditionSpec(predicate, aliasMap, bindingContext);
            return List.of(List.of(spec));
        }
        if (expression instanceof JpqlAst.And(JpqlAst.Expression left2, JpqlAst.Expression right2)) {
            List<List<ConditionSpec>> left = toDnf(left2, aliasMap, bindingContext);
            List<List<ConditionSpec>> right = toDnf(right2, aliasMap, bindingContext);
            List<List<ConditionSpec>> merged = new ArrayList<>();
            for (List<ConditionSpec> l : left) {
                for (List<ConditionSpec> r : right) {
                    List<ConditionSpec> combined = new ArrayList<>(l.size() + r.size());
                    combined.addAll(l);
                    combined.addAll(r);
                    merged.add(combined);
                }
            }
            return merged;
        }
        if (expression instanceof JpqlAst.Or(JpqlAst.Expression left1, JpqlAst.Expression right1)) {
            List<List<ConditionSpec>> left = toDnf(left1, aliasMap, bindingContext);
            List<List<ConditionSpec>> right = toDnf(right1, aliasMap, bindingContext);
            List<List<ConditionSpec>> merged = new ArrayList<>(left.size() + right.size());
            merged.addAll(left);
            merged.addAll(right);
            return merged;
        }
        throw new IllegalArgumentException("Unsupported expression in WHERE");
    }

    private static ConditionSpec toConditionSpec(JpqlAst.Predicate predicate,
            Map<String, String> aliasMap,
            BindingContext bindingContext) {
        if (predicate instanceof JpqlAst.IsNull(String path4, boolean negated1)) {
            LogicalQuery.Operator op = negated1 ? LogicalQuery.Operator.NOT_NULL
                    : LogicalQuery.Operator.IS_NULL;
            String path = resolvePath(path4, aliasMap);
            return new ConditionSpec(path, op, bindingContext.bindLiteral(null), false, LogicalQuery.Combinator.AND);
        }
        if (predicate instanceof JpqlAst.In(String path3, List<JpqlAst.Value> values, boolean negated)) {
            String path = resolvePath(path3, aliasMap);
            LogicalQuery.Operator op = negated ? LogicalQuery.Operator.NOT_IN : LogicalQuery.Operator.IN;
            int argIndex = bindingContext.bindInValues(values);
            return new ConditionSpec(path, op, argIndex, false, LogicalQuery.Combinator.AND);
        }
        if (predicate instanceof JpqlAst.Between(String path2, JpqlAst.Value lower, JpqlAst.Value upper)) {
            String path = resolvePath(path2, aliasMap);
            int argIndex = bindingContext.bindBetween(lower, upper);
            return new ConditionSpec(path, LogicalQuery.Operator.BETWEEN, argIndex, false, LogicalQuery.Combinator.AND);
        }
        if (predicate instanceof JpqlAst.Comparison(String path1, JpqlAst.ComparisonOp op, JpqlAst.Value value)) {
            String path = resolvePath(path1, aliasMap);
            ComparisonMapping mapping = mapComparison(op);
            int argIndex = bindingContext.bindValue(value);
            return new ConditionSpec(path, mapping.operator, argIndex, mapping.ignoreCase, LogicalQuery.Combinator.AND);
        }
        throw new IllegalArgumentException("Unsupported predicate");
    }

    private static ComparisonMapping mapComparison(JpqlAst.ComparisonOp op) {
        return switch (op) {
            case EQ -> new ComparisonMapping(LogicalQuery.Operator.EQ, false);
            case NE -> new ComparisonMapping(LogicalQuery.Operator.NE, false);
            case GT -> new ComparisonMapping(LogicalQuery.Operator.GT, false);
            case GTE -> new ComparisonMapping(LogicalQuery.Operator.GTE, false);
            case LT -> new ComparisonMapping(LogicalQuery.Operator.LT, false);
            case LTE -> new ComparisonMapping(LogicalQuery.Operator.LTE, false);
            case LIKE -> new ComparisonMapping(LogicalQuery.Operator.LIKE, false);
            case ILIKE -> new ComparisonMapping(LogicalQuery.Operator.LIKE, true);
            case NOT_LIKE -> new ComparisonMapping(LogicalQuery.Operator.NOT_LIKE, false);
            case NOT_ILIKE -> new ComparisonMapping(LogicalQuery.Operator.NOT_LIKE, true);
        };
    }

    private static String resolvePath(String rawPath, Map<String, String> aliasMap) {
        String[] segments = rawPath.split("\\.");
        if (segments.length == 0) {
            throw new IllegalArgumentException("Invalid path: " + rawPath);
        }
        String alias = segments[0];
        if (aliasMap.containsKey(alias)) {
            String prefix = aliasMap.get(alias);
            String rest = joinSegments(segments, 1);
            if (prefix.isEmpty()) {
                return rest;
            }
            if (rest.isEmpty()) {
                return prefix;
            }
            return prefix + "." + rest;
        }
        return rawPath;
    }

    private static String joinSegments(String[] segments, int start) {
        if (start >= segments.length) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = start; i < segments.length; i++) {
            if (sb.length() > 0) {
                sb.append('.');
            }
            sb.append(segments[i]);
        }
        return sb.toString();
    }

    private record ComparisonMapping(LogicalQuery.Operator operator, boolean ignoreCase) {
    }

    private record ConditionSpec(
            String path,
            LogicalQuery.Operator operator,
            int argumentIndex,
            boolean ignoreCase,
            LogicalQuery.Combinator combinator) {
        LogicalQuery.Condition toCondition() {
            return LogicalQuery.Condition.of(path, operator, argumentIndex, ignoreCase, combinator);
        }

        ConditionSpec withCombinator(LogicalQuery.Combinator combinator) {
            return new ConditionSpec(path, operator, argumentIndex, ignoreCase, combinator);
        }
    }

    private static final class BindingContext {
        private final Map<String, Integer> namedParams;
        private final int paramCount;
        private final List<Integer> parameterIndices;
        private final List<Object> boundValues;

        BindingContext(Method method) {
            this.namedParams = resolveNamedParams(method);
            this.paramCount = method.getParameterCount();
            this.parameterIndices = new ArrayList<>();
            this.boundValues = new ArrayList<>();
        }

        int bindValue(JpqlAst.Value value) {
            if (value instanceof JpqlAst.Parameter param) {
                return bindParameter(param);
            }
            if (value instanceof JpqlAst.Literal(Object value1)) {
                return bindLiteral(value1);
            }
            throw new IllegalArgumentException("Unsupported value in predicate");
        }

        int bindBetween(JpqlAst.Value lower, JpqlAst.Value upper) {
            int first = bindValue(lower);
            int second = bindValue(upper);
            if (second != first + 1) {
                throw new IllegalArgumentException("BETWEEN values must be adjacent in query order");
            }
            return first;
        }

        int bindInValues(List<JpqlAst.Value> values) {
            if (values.size() == 1 && values.get(0) instanceof JpqlAst.Parameter) {
                return bindValue(values.get(0));
            }
            List<Object> literalValues = new ArrayList<>(values.size());
            for (JpqlAst.Value value : values) {
                if (value instanceof JpqlAst.Literal(Object value1)) {
                    literalValues.add(value1);
                } else {
                    throw new IllegalArgumentException("IN list must use a single parameter or literal list");
                }
            }
            return bindLiteral(literalValues.toArray());
        }

        int bindParameter(JpqlAst.Parameter param) {
            int methodIndex = resolveParameterIndex(param);
            return registerSlot(methodIndex, null);
        }

        int bindLiteral(Object value) {
            return registerSlot(-1, value);
        }

        private int registerSlot(int methodIndex, Object value) {
            int slot = parameterIndices.size();
            parameterIndices.add(methodIndex);
            boundValues.add(value);
            return slot;
        }

        private int resolveParameterIndex(JpqlAst.Parameter parameter) {
            if (parameter.position() != null) {
                int position = parameter.position();
                if (position <= 0 || position > paramCount) {
                    throw new IllegalArgumentException("Positional parameter out of range: ?" + position);
                }
                return position - 1;
            }
            Integer index = namedParams.get(parameter.name());
            if (index == null) {
                throw new IllegalArgumentException("Unknown param: " + parameter.name());
            }
            return index;
        }

        private Map<String, Integer> resolveNamedParams(Method method) {
            Map<String, Integer> mapping = new HashMap<>();
            Parameter[] parameters = method.getParameters();
            for (int i = 0; i < parameters.length; i++) {
                Param param = parameters[i].getAnnotation(Param.class);
                if (param != null) {
                    mapping.put(param.value(), i);
                    continue;
                }
                if (parameters[i].isNamePresent()) {
                    mapping.put(parameters[i].getName(), i);
                }
            }
            return mapping;
        }

        Object[] boundValues() {
            return boundValues.toArray(new Object[0]);
        }

        int[] parameterIndices() {
            int[] indices = new int[parameterIndices.size()];
            for (int i = 0; i < indices.length; i++) {
                indices[i] = parameterIndices.get(i);
            }
            return indices;
        }
    }

    private static final class Parser {
        private final List<JpqlLexer.Token> tokens;
        private int position;

        Parser(String jpql) {
            this.tokens = JpqlLexer.tokenize(jpql);
            this.position = 0;
        }

        JpqlAst.Statement parseStatement() {
            if (peek(JpqlLexer.TokenType.SELECT)) {
                return parseQuery();
            }
            if (peek(JpqlLexer.TokenType.UPDATE)) {
                return parseUpdate();
            }
            if (peek(JpqlLexer.TokenType.DELETE)) {
                return parseDelete();
            }
            throw new IllegalArgumentException("Unsupported @Query statement");
        }

        private JpqlAst.Query parseQuery() {
            expect(JpqlLexer.TokenType.SELECT);
            boolean distinct = match(JpqlLexer.TokenType.DISTINCT);
            boolean count = false;
            List<JpqlAst.SelectItem> selectItems = List.of();
            if (match(JpqlLexer.TokenType.COUNT)) {
                count = true;
                expect(JpqlLexer.TokenType.LPAREN);
                if (match(JpqlLexer.TokenType.IDENT)) {
                    // ignore identifier inside count
                } else {
                    throw new IllegalArgumentException("COUNT requires an alias");
                }
                expect(JpqlLexer.TokenType.RPAREN);
            } else {
                selectItems = parseSelectItems();
            }

            expect(JpqlLexer.TokenType.FROM);
            String entity = expectIdent();
            String alias;
            if (match(JpqlLexer.TokenType.AS)) {
                alias = expectIdent();
            } else if (peek(JpqlLexer.TokenType.IDENT)) {
                alias = expectIdent();
            } else {
                alias = entity;
            }

            List<JpqlAst.Join> joins = new ArrayList<>();
            while (peek(JpqlLexer.TokenType.JOIN) || peek(JpqlLexer.TokenType.LEFT)) {
                JpqlAst.JoinType joinType = JpqlAst.JoinType.INNER;
                if (match(JpqlLexer.TokenType.LEFT)) {
                    joinType = JpqlAst.JoinType.LEFT;
                }
                expect(JpqlLexer.TokenType.JOIN);
                match(JpqlLexer.TokenType.FETCH);
                String path = parsePath();
                String joinAlias = null;
                if (match(JpqlLexer.TokenType.AS)) {
                    joinAlias = expectIdent();
                } else if (peek(JpqlLexer.TokenType.IDENT)) {
                    joinAlias = expectIdent();
                }
                joins.add(new JpqlAst.Join(joinType, path, joinAlias));
            }

            JpqlAst.Expression where = null;
            if (match(JpqlLexer.TokenType.WHERE)) {
                where = parseExpression();
            }

            List<JpqlAst.OrderBy> orderBy = new ArrayList<>();
            if (match(JpqlLexer.TokenType.ORDER)) {
                expect(JpqlLexer.TokenType.BY);
                orderBy.add(parseOrderBy());
                while (match(JpqlLexer.TokenType.COMMA)) {
                    orderBy.add(parseOrderBy());
                }
            }

            expect(JpqlLexer.TokenType.EOF);
            return new JpqlAst.Query(count, distinct, selectItems, entity, alias, joins, where, orderBy);
        }

        private List<JpqlAst.SelectItem> parseSelectItems() {
            List<JpqlAst.SelectItem> items = new ArrayList<>();
            items.add(parseSelectItem());
            while (match(JpqlLexer.TokenType.COMMA)) {
                items.add(parseSelectItem());
            }
            return items;
        }

        private JpqlAst.SelectItem parseSelectItem() {
            String path = parsePath();
            String alias = null;
            if (match(JpqlLexer.TokenType.AS)) {
                alias = expectIdent();
            } else if (peek(JpqlLexer.TokenType.IDENT)) {
                if (peekNext(JpqlLexer.TokenType.COMMA)
                        || peekNext(JpqlLexer.TokenType.FROM)
                        || peekNext(JpqlLexer.TokenType.WHERE)
                        || peekNext(JpqlLexer.TokenType.ORDER)) {
                    alias = expectIdent();
                }
            }
            return new JpqlAst.SelectItem(path, alias);
        }

        private JpqlAst.Update parseUpdate() {
            expect(JpqlLexer.TokenType.UPDATE);
            String entity = expectIdent();
            String alias;
            if (match(JpqlLexer.TokenType.AS)) {
                alias = expectIdent();
            } else if (peek(JpqlLexer.TokenType.IDENT)) {
                alias = expectIdent();
            } else {
                alias = entity;
            }

            expect(JpqlLexer.TokenType.SET);
            List<JpqlAst.Assignment> assignments = new ArrayList<>();
            assignments.add(parseAssignment());
            while (match(JpqlLexer.TokenType.COMMA)) {
                assignments.add(parseAssignment());
            }

            JpqlAst.Expression where = null;
            if (match(JpqlLexer.TokenType.WHERE)) {
                where = parseExpression();
            }

            expect(JpqlLexer.TokenType.EOF);
            return new JpqlAst.Update(entity, alias, assignments, where);
        }

        private JpqlAst.Delete parseDelete() {
            expect(JpqlLexer.TokenType.DELETE);
            if (match(JpqlLexer.TokenType.FROM)) {
                // optional FROM
            }
            String entity = expectIdent();
            String alias;
            if (match(JpqlLexer.TokenType.AS)) {
                alias = expectIdent();
            } else if (peek(JpqlLexer.TokenType.IDENT)) {
                alias = expectIdent();
            } else {
                alias = entity;
            }

            JpqlAst.Expression where = null;
            if (match(JpqlLexer.TokenType.WHERE)) {
                where = parseExpression();
            }

            expect(JpqlLexer.TokenType.EOF);
            return new JpqlAst.Delete(entity, alias, where);
        }

        private JpqlAst.Assignment parseAssignment() {
            String path = parsePath();
            expect(JpqlLexer.TokenType.EQ);
            JpqlAst.Value value = parseValue();
            return new JpqlAst.Assignment(path, value);
        }

        private JpqlAst.OrderBy parseOrderBy() {
            String path = parsePath();
            boolean ascending = true;
            if (match(JpqlLexer.TokenType.ASC)) {
                ascending = true;
            } else if (match(JpqlLexer.TokenType.DESC)) {
                ascending = false;
            }
            return new JpqlAst.OrderBy(path, ascending);
        }

        private JpqlAst.Expression parseExpression() {
            return parseOr();
        }

        private JpqlAst.Expression parseOr() {
            JpqlAst.Expression left = parseAnd();
            while (match(JpqlLexer.TokenType.OR)) {
                JpqlAst.Expression right = parseAnd();
                left = new JpqlAst.Or(left, right);
            }
            return left;
        }

        private JpqlAst.Expression parseAnd() {
            JpqlAst.Expression left = parseNot();
            while (match(JpqlLexer.TokenType.AND)) {
                JpqlAst.Expression right = parseNot();
                left = new JpqlAst.And(left, right);
            }
            return left;
        }

        private JpqlAst.Expression parseNot() {
            if (match(JpqlLexer.TokenType.NOT)) {
                return new JpqlAst.Not(parseNot());
            }
            return parsePrimary();
        }

        private JpqlAst.Expression parsePrimary() {
            if (match(JpqlLexer.TokenType.LPAREN)) {
                JpqlAst.Expression expr = parseExpression();
                expect(JpqlLexer.TokenType.RPAREN);
                return expr;
            }
            return new JpqlAst.PredicateExpr(parsePredicate());
        }

        private JpqlAst.Predicate parsePredicate() {
            String path = parsePath();

            if (match(JpqlLexer.TokenType.NOT)) {
                if (match(JpqlLexer.TokenType.LIKE)) {
                    JpqlAst.Value value = parseValue();
                    return new JpqlAst.Comparison(path, JpqlAst.ComparisonOp.NOT_LIKE, value);
                }
                if (match(JpqlLexer.TokenType.ILIKE)) {
                    JpqlAst.Value value = parseValue();
                    return new JpqlAst.Comparison(path, JpqlAst.ComparisonOp.NOT_ILIKE, value);
                }
                if (match(JpqlLexer.TokenType.IN)) {
                    List<JpqlAst.Value> values = parseInList();
                    return new JpqlAst.In(path, values, true);
                }
                if (match(JpqlLexer.TokenType.BETWEEN)) {
                    throw new UnsupportedOperationException("NOT BETWEEN is not supported");
                }
                throw new IllegalArgumentException("Expected operator after NOT for path: " + path);
            }

            if (match(JpqlLexer.TokenType.BETWEEN)) {
                JpqlAst.Value lower = parseValue();
                expect(JpqlLexer.TokenType.AND);
                JpqlAst.Value upper = parseValue();
                return new JpqlAst.Between(path, lower, upper);
            }

            if (match(JpqlLexer.TokenType.IN)) {
                List<JpqlAst.Value> values = parseInList();
                return new JpqlAst.In(path, values, false);
            }

            if (match(JpqlLexer.TokenType.IS)) {
                boolean negated = match(JpqlLexer.TokenType.NOT);
                expect(JpqlLexer.TokenType.NULL);
                return new JpqlAst.IsNull(path, negated);
            }

            if (peek(JpqlLexer.TokenType.EQ)
                    || peek(JpqlLexer.TokenType.NE)
                    || peek(JpqlLexer.TokenType.GT)
                    || peek(JpqlLexer.TokenType.GTE)
                    || peek(JpqlLexer.TokenType.LT)
                    || peek(JpqlLexer.TokenType.LTE)
                    || peek(JpqlLexer.TokenType.LIKE)
                    || peek(JpqlLexer.TokenType.ILIKE)) {
                JpqlAst.ComparisonOp op = parseComparisonOp();
                JpqlAst.Value value = parseValue();
                return new JpqlAst.Comparison(path, op, value);
            }

            throw new IllegalArgumentException("Expected operator after path: " + path);
        }

        private List<JpqlAst.Value> parseInList() {
            if (match(JpqlLexer.TokenType.LPAREN)) {
                List<JpqlAst.Value> values = new ArrayList<>();
                values.add(parseValue());
                while (match(JpqlLexer.TokenType.COMMA)) {
                    values.add(parseValue());
                }
                expect(JpqlLexer.TokenType.RPAREN);
                return values;
            }
            return List.of(parseValue());
        }

        private JpqlAst.ComparisonOp parseComparisonOp() {
            if (match(JpqlLexer.TokenType.EQ)) {
                return JpqlAst.ComparisonOp.EQ;
            }
            if (match(JpqlLexer.TokenType.NE)) {
                return JpqlAst.ComparisonOp.NE;
            }
            if (match(JpqlLexer.TokenType.GTE)) {
                return JpqlAst.ComparisonOp.GTE;
            }
            if (match(JpqlLexer.TokenType.GT)) {
                return JpqlAst.ComparisonOp.GT;
            }
            if (match(JpqlLexer.TokenType.LTE)) {
                return JpqlAst.ComparisonOp.LTE;
            }
            if (match(JpqlLexer.TokenType.LT)) {
                return JpqlAst.ComparisonOp.LT;
            }
            if (match(JpqlLexer.TokenType.LIKE)) {
                return JpqlAst.ComparisonOp.LIKE;
            }
            if (match(JpqlLexer.TokenType.ILIKE)) {
                return JpqlAst.ComparisonOp.ILIKE;
            }
            throw new IllegalArgumentException("Expected comparison operator");
        }

        private JpqlAst.Value parseValue() {
            if (match(JpqlLexer.TokenType.PARAM_NAMED)) {
                JpqlLexer.Token prev = previous();
                return new JpqlAst.Parameter(prev.text(), null);
            }
            if (match(JpqlLexer.TokenType.PARAM_POSITIONAL)) {
                JpqlLexer.Token prev = previous();
                return new JpqlAst.Parameter(null, (Integer) prev.literal());
            }
            if (match(JpqlLexer.TokenType.STRING)) {
                JpqlLexer.Token prev = previous();
                return new JpqlAst.Literal(prev.literal());
            }
            if (match(JpqlLexer.TokenType.NUMBER)) {
                JpqlLexer.Token prev = previous();
                return new JpqlAst.Literal(prev.literal());
            }
            if (match(JpqlLexer.TokenType.TRUE)) {
                return new JpqlAst.Literal(Boolean.TRUE);
            }
            if (match(JpqlLexer.TokenType.FALSE)) {
                return new JpqlAst.Literal(Boolean.FALSE);
            }
            if (match(JpqlLexer.TokenType.NULL)) {
                return new JpqlAst.Literal(null);
            }
            throw new IllegalArgumentException("Expected value");
        }

        private String parsePath() {
            StringBuilder sb = new StringBuilder();
            sb.append(expectIdent());
            while (match(JpqlLexer.TokenType.DOT)) {
                sb.append('.').append(expectIdent());
            }
            return sb.toString();
        }

        private boolean match(JpqlLexer.TokenType type) {
            if (peek(type)) {
                position++;
                return true;
            }
            return false;
        }

        private boolean peek(JpqlLexer.TokenType type) {
            return tokens.get(position).type() == type;
        }

        private boolean peekNext(JpqlLexer.TokenType type) {
            int next = position + 1;
            if (next >= tokens.size()) {
                return false;
            }
            return tokens.get(next).type() == type;
        }

        private void expect(JpqlLexer.TokenType type) {
            if (!match(type)) {
                JpqlLexer.Token token = tokens.get(position);
                throw new IllegalArgumentException("Expected " + type + " at position " + token.position());
            }
        }

        private String expectIdent() {
            // Accept IDENT or certain keywords that can also be valid entity/field names
            if (match(JpqlLexer.TokenType.IDENT)) {
                return previous().text();
            }
            // Allow ORDER, BY, etc. to be used as identifiers (entity/field names)
            if (match(JpqlLexer.TokenType.ORDER)
                    || match(JpqlLexer.TokenType.BY)
                    || match(JpqlLexer.TokenType.AS)
                    || match(JpqlLexer.TokenType.SET)
                    || match(JpqlLexer.TokenType.IN)
                    || match(JpqlLexer.TokenType.IS)
                    || match(JpqlLexer.TokenType.NULL)
                    || match(JpqlLexer.TokenType.COUNT)
                    || match(JpqlLexer.TokenType.DISTINCT)
                    || match(JpqlLexer.TokenType.TRUE)
                    || match(JpqlLexer.TokenType.FALSE)
                    || match(JpqlLexer.TokenType.FETCH)
                    || match(JpqlLexer.TokenType.LEFT)) {
                return previous().text();
            }
            JpqlLexer.Token token = tokens.get(position);
            throw new IllegalArgumentException("Expected identifier at position " + token.position());
        }

        private JpqlLexer.Token previous() {
            return tokens.get(position - 1);
        }
    }
}
