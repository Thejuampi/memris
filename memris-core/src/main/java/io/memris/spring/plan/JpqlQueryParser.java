package io.memris.spring.plan;

import io.memris.spring.Param;
import io.memris.spring.Query;
import io.memris.spring.plan.jpql.JpqlAst;
import io.memris.spring.plan.jpql.JpqlLexer;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
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
        JpqlAst.Query ast = parser.parseQuery();

        validateEntity(ast.entityName(), entityClass);
        if (ast.distinct()) {
            throw new UnsupportedOperationException("DISTINCT is not supported in @Query");
        }

        LogicalQuery.ReturnKind returnKind = resolveReturnKind(method);
        if (ast.count() && returnKind != LogicalQuery.ReturnKind.COUNT_LONG) {
            throw new IllegalArgumentException("@Query count must return long: " + method.getName());
        }
        if (!ast.count() && returnKind == LogicalQuery.ReturnKind.COUNT_LONG) {
            throw new IllegalArgumentException("@Query return type long requires count: " + method.getName());
        }

        Map<String, String> aliasMap = buildAliasMap(ast);
        BindingContext bindingContext = new BindingContext(method);
        List<LogicalQuery.Condition> flattened = flattenConditions(ast.where(), aliasMap, bindingContext);

        LogicalQuery.OrderBy orderBy = null;
        if (!ast.orderBy().isEmpty()) {
            if (ast.orderBy().size() > 1) {
                throw new UnsupportedOperationException("Multiple ORDER BY clauses are not supported");
            }
            JpqlAst.OrderBy order = ast.orderBy().get(0);
            String path = resolvePath(order.path(), aliasMap);
            orderBy = LogicalQuery.OrderBy.of(path, order.ascending());
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

        return LogicalQuery.of(
            opCode,
            returnKind,
            conditions,
            new LogicalQuery.Join[0],
            orderBy,
            0,
            boundValues,
            parameterIndices,
            arity
        );
    }

    private static LogicalQuery.ReturnKind resolveReturnKind(Method method) {
        Class<?> returnType = method.getReturnType();
        if (Optional.class.equals(returnType)) {
            return LogicalQuery.ReturnKind.ONE_OPTIONAL;
        }
        if (List.class.isAssignableFrom(returnType)) {
            return LogicalQuery.ReturnKind.MANY_LIST;
        }
        if (returnType == boolean.class || returnType == Boolean.class) {
            return LogicalQuery.ReturnKind.EXISTS_BOOL;
        }
        if (returnType == long.class || returnType == Long.class) {
            return LogicalQuery.ReturnKind.COUNT_LONG;
        }
        return LogicalQuery.ReturnKind.MANY_LIST;
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
        if (expression instanceof JpqlAst.Not not) {
            return negate(not.expression());
        }
        if (expression instanceof JpqlAst.And and) {
            return new JpqlAst.And(normalize(and.left()), normalize(and.right()));
        }
        if (expression instanceof JpqlAst.Or or) {
            return new JpqlAst.Or(normalize(or.left()), normalize(or.right()));
        }
        return expression;
    }

    private static JpqlAst.Expression negate(JpqlAst.Expression expression) {
        if (expression instanceof JpqlAst.Not not) {
            return normalize(not.expression());
        }
        if (expression instanceof JpqlAst.And and) {
            return new JpqlAst.Or(negate(and.left()), negate(and.right()));
        }
        if (expression instanceof JpqlAst.Or or) {
            return new JpqlAst.And(negate(or.left()), negate(or.right()));
        }
        if (expression instanceof JpqlAst.PredicateExpr predicateExpr) {
            return new JpqlAst.PredicateExpr(negatePredicate(predicateExpr.predicate()));
        }
        throw new IllegalArgumentException("Unsupported NOT expression");
    }

    private static JpqlAst.Predicate negatePredicate(JpqlAst.Predicate predicate) {
        if (predicate instanceof JpqlAst.IsNull isNull) {
            return new JpqlAst.IsNull(isNull.path(), !isNull.negated());
        }
        if (predicate instanceof JpqlAst.In in) {
            return new JpqlAst.In(in.path(), in.values(), !in.negated());
        }
        if (predicate instanceof JpqlAst.Between) {
            throw new UnsupportedOperationException("NOT BETWEEN is not supported");
        }
        if (predicate instanceof JpqlAst.Comparison comparison) {
            JpqlAst.ComparisonOp negated = switch (comparison.op()) {
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
            return new JpqlAst.Comparison(comparison.path(), negated, comparison.value());
        }
        throw new IllegalArgumentException("Unsupported predicate negation");
    }

    private static List<List<ConditionSpec>> toDnf(JpqlAst.Expression expression,
                                                   Map<String, String> aliasMap,
                                                   BindingContext bindingContext) {
        if (expression instanceof JpqlAst.PredicateExpr predicateExpr) {
            ConditionSpec spec = toConditionSpec(predicateExpr.predicate(), aliasMap, bindingContext);
            return List.of(List.of(spec));
        }
        if (expression instanceof JpqlAst.And and) {
            List<List<ConditionSpec>> left = toDnf(and.left(), aliasMap, bindingContext);
            List<List<ConditionSpec>> right = toDnf(and.right(), aliasMap, bindingContext);
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
        if (expression instanceof JpqlAst.Or or) {
            List<List<ConditionSpec>> left = toDnf(or.left(), aliasMap, bindingContext);
            List<List<ConditionSpec>> right = toDnf(or.right(), aliasMap, bindingContext);
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
        if (predicate instanceof JpqlAst.IsNull isNull) {
            LogicalQuery.Operator op = isNull.negated() ? LogicalQuery.Operator.NOT_NULL : LogicalQuery.Operator.IS_NULL;
            String path = resolvePath(isNull.path(), aliasMap);
            return new ConditionSpec(path, op, bindingContext.bindLiteral(null), false, LogicalQuery.Combinator.AND);
        }
        if (predicate instanceof JpqlAst.In in) {
            String path = resolvePath(in.path(), aliasMap);
            LogicalQuery.Operator op = in.negated() ? LogicalQuery.Operator.NOT_IN : LogicalQuery.Operator.IN;
            int argIndex = bindingContext.bindInValues(in.values());
            return new ConditionSpec(path, op, argIndex, false, LogicalQuery.Combinator.AND);
        }
        if (predicate instanceof JpqlAst.Between between) {
            String path = resolvePath(between.path(), aliasMap);
            int argIndex = bindingContext.bindBetween(between.lower(), between.upper());
            return new ConditionSpec(path, LogicalQuery.Operator.BETWEEN, argIndex, false, LogicalQuery.Combinator.AND);
        }
        if (predicate instanceof JpqlAst.Comparison comparison) {
            String path = resolvePath(comparison.path(), aliasMap);
            ComparisonMapping mapping = mapComparison(comparison.op());
            int argIndex = bindingContext.bindValue(comparison.value());
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
            LogicalQuery.Combinator combinator
    ) {
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
            if (value instanceof JpqlAst.Literal literal) {
                return bindLiteral(literal.value());
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
                if (value instanceof JpqlAst.Literal literal) {
                    literalValues.add(literal.value());
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

        JpqlAst.Query parseQuery() {
            expect(JpqlLexer.TokenType.SELECT);
            boolean distinct = match(JpqlLexer.TokenType.DISTINCT);
            boolean count = false;
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
                expect(JpqlLexer.TokenType.IDENT);
            }

            expect(JpqlLexer.TokenType.FROM);
            String entity = expectIdent();
            String alias = null;
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
            return new JpqlAst.Query(count, distinct, entity, alias, joins, where, orderBy);
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

        private void expect(JpqlLexer.TokenType type) {
            if (!match(type)) {
                JpqlLexer.Token token = tokens.get(position);
                throw new IllegalArgumentException("Expected " + type + " at position " + token.position());
            }
        }

        private String expectIdent() {
            if (!match(JpqlLexer.TokenType.IDENT)) {
                JpqlLexer.Token token = tokens.get(position);
                throw new IllegalArgumentException("Expected identifier at position " + token.position());
            }
            return previous().text();
        }

        private JpqlLexer.Token previous() {
            return tokens.get(position - 1);
        }
    }
}
