package io.memris.spring.plan;

import io.memris.kernel.Predicate;

/**
 * Logical query representation after parsing method name.
 * <p>
 * This is the result of QueryPlanner.parse() and represents
 * the semantic meaning of a query method without any execution details.
 * <p>
 * Contains:
 * - Return kind (what type of result: list, optional, boolean, long)
 * - Conditions (what to filter: column + operator)
 * - Order by (optional sorting)
 * <p>
 * This is an intermediate representation that gets compiled to CompiledQuery.
 *
 * @see QueryCompiler
 * @see CompiledQuery
 */
public final class LogicalQuery {

    private final String methodName;
    private final ReturnKind returnKind;
    private final Condition[] conditions;
    private final OrderBy orderBy;

    private LogicalQuery(
            String methodName,
            ReturnKind returnKind,
            Condition[] conditions,
            OrderBy orderBy) {
        this.methodName = methodName;
        this.returnKind = returnKind;
        this.conditions = conditions;
        this.orderBy = orderBy;
    }

    public static LogicalQuery of(
            String methodName,
            ReturnKind returnKind,
            Condition[] conditions,
            OrderBy orderBy) {
        return new LogicalQuery(methodName, returnKind, conditions, orderBy);
    }

    public String methodName() {
        return methodName;
    }

    public ReturnKind returnKind() {
        return returnKind;
    }

    public Condition[] conditions() {
        return conditions;
    }

    public OrderBy orderBy() {
        return orderBy;
    }

    public int arity() {
        return conditions.length;
    }

    /**
     * What kind of result this query returns.
     */
    public enum ReturnKind {
        /** Single entity, nullable (findById) */
        ONE_OPTIONAL,
        /** Multiple entities (findBy*, findAll) */
        MANY_LIST,
        /** Boolean existence check (existsById) */
        EXISTS_BOOL,
        /** Count (count, countBy*) */
        COUNT_LONG
    }

    /**
     * A single condition in the query.
     * <p>
     * Examples:
     * - "name = :arg0" → propertyPath="name", operator=EQ, argumentIndex=0
     * - "age > :arg1" → propertyPath="age", operator=GT, argumentIndex=1
     * - "address.zip IN :arg2" → propertyPath="address.zip", operator=IN, argumentIndex=2
     */
    public static final class Condition {
        private final String propertyPath;   // e.g., "address.zip"
        private final Operator operator;     // EQ, GT, IN, etc.
        private final int argumentIndex;     // which method parameter
        private final boolean ignoreCase;

        private Condition(String propertyPath, Operator operator, int argumentIndex, boolean ignoreCase) {
            this.propertyPath = propertyPath;
            this.operator = operator;
            this.argumentIndex = argumentIndex;
            this.ignoreCase = ignoreCase;
        }

        public static Condition of(String propertyPath, Operator operator, int argumentIndex) {
            return new Condition(propertyPath, operator, argumentIndex, false);
        }

        public static Condition of(String propertyPath, Operator operator, int argumentIndex, boolean ignoreCase) {
            return new Condition(propertyPath, operator, argumentIndex, ignoreCase);
        }

        public String propertyPath() {
            return propertyPath;
        }

        public Operator operator() {
            return operator;
        }

        public int argumentIndex() {
            return argumentIndex;
        }

        public boolean ignoreCase() {
            return ignoreCase;
        }
    }

    /**
     * Operator for a condition.
     * <p>
     * Maps to Predicate.Operator for execution.
     */
    public enum Operator {
        EQ(Predicate.Operator.EQ),
        NE(Predicate.Operator.NEQ),
        GT(Predicate.Operator.GT),
        GTE(Predicate.Operator.GTE),
        LT(Predicate.Operator.LT),
        LTE(Predicate.Operator.LTE),
        IN(Predicate.Operator.IN),
        NOT_IN(Predicate.Operator.NOT_IN),
        BETWEEN(Predicate.Operator.BETWEEN),
        CONTAINING(Predicate.Operator.CONTAINING),
        STARTING_WITH(Predicate.Operator.STARTING_WITH),
        ENDING_WITH(Predicate.Operator.ENDING_WITH),
        LIKE(Predicate.Operator.LIKE),
        NOT_LIKE(Predicate.Operator.NOT_LIKE),
        IGNORE_CASE_EQ(Predicate.Operator.IGNORE_CASE),
        IGNORE_CASE_LIKE(Predicate.Operator.IGNORE_CASE),
        IS_NULL(Predicate.Operator.IS_NULL),
        NOT_NULL(Predicate.Operator.IS_NOT_NULL),
        IS_TRUE(Predicate.Operator.IS_TRUE),
        IS_FALSE(Predicate.Operator.IS_FALSE),
        BEFORE(Predicate.Operator.BEFORE),
        AFTER(Predicate.Operator.AFTER);

        private final Predicate.Operator predicateOperator;

        Operator(Predicate.Operator predicateOperator) {
            this.predicateOperator = predicateOperator;
        }

        public Predicate.Operator toPredicateOperator() {
            return predicateOperator;
        }
    }

    /**
     * Order by clause (optional).
     */
    public static final class OrderBy {
        private final String propertyPath;
        private final boolean ascending;

        private OrderBy(String propertyPath, boolean ascending) {
            this.propertyPath = propertyPath;
            this.ascending = ascending;
        }

        public static OrderBy of(String propertyPath, boolean ascending) {
            return new OrderBy(propertyPath, ascending);
        }

        public static OrderBy asc(String propertyPath) {
            return new OrderBy(propertyPath, true);
        }

        public static OrderBy desc(String propertyPath) {
            return new OrderBy(propertyPath, false);
        }

        public String propertyPath() {
            return propertyPath;
        }

        public boolean ascending() {
            return ascending;
        }
    }
}
