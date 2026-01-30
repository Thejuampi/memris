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
public record LogicalQuery(
        OpCode opCode,
        ReturnKind returnKind,
        Condition[] conditions,
        Join[] joins,
        OrderBy orderBy,
        int parameterCount
) {

    public static LogicalQuery of(
            OpCode opCode,
            ReturnKind returnKind,
            Condition[] conditions,
            OrderBy orderBy) {
        return new LogicalQuery(opCode, returnKind, conditions, new Join[0], orderBy, conditions.length);
    }

    public static LogicalQuery of(
            OpCode opCode,
            ReturnKind returnKind,
            Condition[] conditions,
            Join[] joins,
            OrderBy orderBy,
            int parameterCount) {
        return new LogicalQuery(opCode, returnKind, conditions, joins, orderBy, parameterCount);
    }

    /**
     * Create a simple query without joins.
     */
    public static LogicalQuery of(
            OpCode opCode,
            ReturnKind returnKind,
            Condition[] conditions,
            OrderBy orderBy,
            int parameterCount) {
        return new LogicalQuery(opCode, returnKind, conditions, new Join[0], orderBy, parameterCount);
    }

    /**
     * Create a LogicalQuery for a CRUD operation.
     * <p>
     * CRUD operations have no conditions and are identified by their OpCode.
     *
     * @param opCode   the operation code (SAVE_ONE, DELETE_ALL, etc.)
     * @param returnKind   the CRUD return kind (SAVE, SAVE_ALL, DELETE, DELETE_ALL)
     * @param parameterCount the number of parameters (1 for save(T), 0 for deleteAll())
     * @return a LogicalQuery for the CRUD operation
     */
    public static LogicalQuery crud(OpCode opCode, ReturnKind returnKind, int parameterCount) {
        return new LogicalQuery(opCode, returnKind, new Condition[0], new Join[0], null, parameterCount);
    }

    /**
     * Returns the number of method parameters.
     * <p>
     * For query methods, this equals the number of conditions.
     * For CRUD operations, this includes entity parameters even if there are no conditions.
     */
    public int arity() {
        return parameterCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogicalQuery that = (LogicalQuery) o;
        return parameterCount == that.parameterCount
                && opCode == that.opCode
                && returnKind == that.returnKind
                && java.util.Arrays.equals(conditions, that.conditions)
                && java.util.Arrays.equals(joins, that.joins)
                && java.util.Objects.equals(orderBy, that.orderBy);
    }

    @Override
    public int hashCode() {
        int result = java.util.Objects.hash(opCode, returnKind, orderBy, parameterCount);
        result = 31 * result + java.util.Arrays.hashCode(conditions);
        result = 31 * result + java.util.Arrays.hashCode(joins);
        return result;
    }

    /**
     * What kind of result this query returns.
     * <p>
     * ReturnKind describes both the operation type (query vs CRUD) and the return type.
     * This allows unified compilation pipeline for all repository methods.
     */
    public enum ReturnKind {
        /** Query: Single entity, nullable (findById) */
        ONE_OPTIONAL,
        /** Query: Multiple entities (findBy*, findAll) */
        MANY_LIST,
        /** Query: Boolean existence check (existsById) */
        EXISTS_BOOL,
        /** Query: Count (count, countBy*) */
        COUNT_LONG,
        /** CRUD: Save single entity (save) */
        SAVE,
        /** CRUD: Save multiple entities (saveAll) */
        SAVE_ALL,
        /** CRUD: Delete single entity (delete) */
        DELETE,
        /** CRUD: Delete all entities (deleteAll) */
        DELETE_ALL,
        /** CRUD: Delete by ID (deleteById) */
        DELETE_BY_ID
    }

    /**
     * A single condition in the query.
     * <p>
     * Examples:
     * - "name = :arg0" → propertyPath="name", operator=EQ, argumentIndex=0
     * - "age > :arg1" → propertyPath="age", operator=GT, argumentIndex=1
     * - "address.zip IN :arg2" → propertyPath="address.zip", operator=IN, argumentIndex=2
     * <p>
     * Special case: ID conditions use the special "$ID" marker which resolves
     * to the entity's ID column at compile time (not necessarily a field named "id").
     */
    public record Condition(
            String propertyPath,
            Operator operator,
            int argumentIndex,
            boolean ignoreCase
    ) {
        /** Special marker for the entity's ID property. Resolved at compile time. */
        public static final String ID_PROPERTY = "$ID";

        public static Condition of(String propertyPath, Operator operator, int argumentIndex) {
            return new Condition(propertyPath, operator, argumentIndex, false);
        }

        public static Condition of(String propertyPath, Operator operator, int argumentIndex, boolean ignoreCase) {
            return new Condition(propertyPath, operator, argumentIndex, ignoreCase);
        }

        /**
         * Create a condition on the entity's ID property.
         * <p>
         * The special $ID marker resolves to the entity's actual ID column
         * at compile time (via @Id or @javax.persistence.Id annotation).
         */
        public static Condition idCondition(int argumentIndex) {
            return new Condition(ID_PROPERTY, Operator.EQ, argumentIndex, false);
        }

        /** Returns true if this condition is on the entity's ID property. */
        public boolean isIdCondition() {
            return ID_PROPERTY.equals(propertyPath);
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
        NOT_CONTAINING(Predicate.Operator.NOT_CONTAINING),
        STARTING_WITH(Predicate.Operator.STARTING_WITH),
        NOT_STARTING_WITH(Predicate.Operator.NOT_STARTING_WITH),
        ENDING_WITH(Predicate.Operator.ENDING_WITH),
        NOT_ENDING_WITH(Predicate.Operator.NOT_ENDING_WITH),
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
    public record OrderBy(
            String propertyPath,
            boolean ascending
    ) {
        public static OrderBy of(String propertyPath, boolean ascending) {
            return new OrderBy(propertyPath, ascending);
        }

        public static OrderBy asc(String propertyPath) {
            return new OrderBy(propertyPath, true);
        }

        public static OrderBy desc(String propertyPath) {
            return new OrderBy(propertyPath, false);
        }
    }

    /**
     * Join specification for cross-table queries.
     * <p>
     * Represents a relationship join that needs to be resolved at query time.
     * The join connects the primary entity to a related entity via foreign key.
     * <p>
     * Example: Order.customer -> Customer
     * - propertyPath: "customer"
     * - targetEntity: Customer.class
     * - joinType: INNER
     */
    public record Join(
            String propertyPath,      // The relationship property name (e.g., "customer")
            Class<?> targetEntity,    // The target entity class (e.g., Customer.class)
            String joinColumn,        // Foreign key column name (e.g., "customer_id")
            String referencedColumn,  // Referenced column in target (e.g., "id")
            JoinType joinType         // INNER, LEFT, RIGHT, FULL
    ) {
        public enum JoinType {
            INNER,   // Only matching rows from both tables
            LEFT,    // All rows from left table, matching from right
            RIGHT,   // Matching from left, all from right table
            FULL     // All rows from both tables
        }

        public static Join inner(String propertyPath, Class<?> targetEntity, String joinColumn) {
            return new Join(propertyPath, targetEntity, joinColumn, "id", JoinType.INNER);
        }

        public static Join left(String propertyPath, Class<?> targetEntity, String joinColumn) {
            return new Join(propertyPath, targetEntity, joinColumn, "id", JoinType.LEFT);
        }
    }
}
