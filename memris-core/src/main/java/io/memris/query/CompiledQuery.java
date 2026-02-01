package io.memris.query;

import io.memris.query.LogicalQuery.Operator;

/**
 * A pre-compiled query ready for execution.
 * <p>
 * This is the output of QueryCompiler.compile() and contains everything
 * needed to execute a query without any runtime parsing or reflection.
 * <p>
 * Key optimization: All string lookups have been resolved to column indices.
 * The opCode allows switch-based dispatch instead of string comparisons.
 * <p>
 * This object is stored in RepositoryPlan and indexed by queryId.
 *
 * @see QueryCompiler
 * @see LogicalQuery
 */
public record CompiledQuery(
        /** Operation code for unified dispatch (SAVE_ONE, FIND, COUNT, etc.) */
        OpCode opCode,
        /** What kind of result this query returns */
        LogicalQuery.ReturnKind returnKind,
        /** Pre-compiled conditions with resolved column indices */
        CompiledCondition[] conditions,
        /** Pre-compiled update assignments */
        CompiledUpdateAssignment[] updateAssignments,
        /** Pre-compiled projection */
        CompiledProjection projection,
        /** Pre-compiled joins */
        CompiledJoin[] joins,
        /** Pre-compiled order by (optional) */
        CompiledOrderBy[] orderBy,
        /** Grouping configuration for Map return types (optional) */
        CompiledGrouping grouping,
        /** HAVING conditions for grouped queries (optional) */
        CompiledCondition[] havingConditions,
        /** Limit for Top/First queries (0 = none) */
        int limit,
        /** DISTINCT flag for result de-duplication */
        boolean distinct,
        /** Bound literal values (appended after method arguments) */
        Object[] boundValues,
        /** Parameter index mapping for query argument slots */
        int[] parameterIndices,
        /** Number of method parameters */
        int arity
) {

    /**
     * Create a CompiledQuery.
     *
     * @param opCode     the operation code
     * @param returnKind the return kind
     * @param conditions the compiled conditions
     * @return a new CompiledQuery
     */
    public static CompiledQuery of(
            OpCode opCode,
            LogicalQuery.ReturnKind returnKind,
            CompiledCondition[] conditions) {
        return new CompiledQuery(opCode, returnKind, conditions, new CompiledUpdateAssignment[0], null,
                new CompiledJoin[0], null, null, null, 0, false, new Object[0], new int[0], conditions.length);
    }

    public static CompiledQuery of(
            OpCode opCode,
            LogicalQuery.ReturnKind returnKind,
            CompiledCondition[] conditions,
            CompiledJoin[] joins,
            CompiledOrderBy[] orderBy,
            int limit,
            int arity) {
        return new CompiledQuery(opCode, returnKind, conditions, new CompiledUpdateAssignment[0], null, joins, orderBy,
                null, null, limit, false, new Object[0], new int[0], arity);
    }

    public static CompiledQuery of(
            OpCode opCode,
            LogicalQuery.ReturnKind returnKind,
            CompiledCondition[] conditions,
            CompiledUpdateAssignment[] updateAssignments,
            CompiledProjection projection,
            CompiledJoin[] joins,
            CompiledOrderBy[] orderBy,
            CompiledGrouping grouping,
            int limit,
            boolean distinct,
            Object[] boundValues,
            int[] parameterIndices,
            int arity,
            CompiledCondition[] havingConditions) {
        return new CompiledQuery(opCode, returnKind, conditions, updateAssignments, projection, joins, orderBy, grouping,
                havingConditions, limit, distinct, boundValues, parameterIndices, arity);
    }

    public static CompiledQuery of(
            OpCode opCode,
            LogicalQuery.ReturnKind returnKind,
            CompiledCondition[] conditions,
            CompiledUpdateAssignment[] updateAssignments,
            CompiledProjection projection,
            CompiledJoin[] joins,
            CompiledOrderBy[] orderBy,
            CompiledGrouping grouping,
            int limit,
            boolean distinct,
            Object[] boundValues,
            int[] parameterIndices,
            int arity) {
        return new CompiledQuery(opCode, returnKind, conditions, updateAssignments, projection, joins, orderBy, grouping,
                null, limit, distinct, boundValues, parameterIndices, arity);
    }

    public CompiledQuery withJoins(CompiledJoin[] joins) {
        return new CompiledQuery(opCode, returnKind, conditions, updateAssignments, projection, joins, orderBy, grouping,
                havingConditions, limit, distinct, boundValues, parameterIndices, arity);
    }

    /**
     * Pre-compiled update assignment.
     */
    public record CompiledUpdateAssignment(
            int columnIndex,
            int argumentIndex
    ) {
    }

    public record CompiledProjection(
            Class<?> projectionType,
            CompiledProjectionItem[] items
    ) {
    }

    public record CompiledProjectionItem(
            String alias,
            CompiledProjectionStep[] steps,
            Class<?> fieldEntity,
            int columnIndex,
            byte typeCode,
            String fieldName
    ) {
    }

    public record CompiledProjectionStep(
            Class<?> sourceEntity,
            Class<?> targetEntity,
            int sourceColumnIndex,
            int targetColumnIndex,
            boolean targetColumnIsId,
            byte fkTypeCode
    ) {
    }

    /**
     * Pre-compiled order by clause.
     */
    public record CompiledOrderBy(
            int columnIndex,
            boolean ascending
    ) {
    }

    /**
     * A pre-compiled condition with resolved column index.
     * <p>
     * The compiler resolves propertyPath → columnIndex once, so
     * the runtime can use direct array access instead of map lookups.
     */
    public static final class CompiledCondition {
        private final int columnIndex;       // resolved from propertyPath (build-time)
        private final Operator operator;     // from LogicalQuery
        private final int argumentIndex;     // which method parameter
        private final boolean ignoreCase;
        private final LogicalQuery.Combinator nextCombinator; // how to combine with next condition

        private CompiledCondition(int columnIndex, Operator operator, int argumentIndex, boolean ignoreCase, LogicalQuery.Combinator nextCombinator) {
            this.columnIndex = columnIndex;
            this.operator = operator;
            this.argumentIndex = argumentIndex;
            this.ignoreCase = ignoreCase;
            this.nextCombinator = nextCombinator;
        }

        public static CompiledCondition of(int columnIndex, Operator operator, int argumentIndex) {
            return new CompiledCondition(columnIndex, operator, argumentIndex, false, LogicalQuery.Combinator.AND);
        }

        public static CompiledCondition of(int columnIndex, Operator operator, int argumentIndex, boolean ignoreCase) {
            return new CompiledCondition(columnIndex, operator, argumentIndex, ignoreCase, LogicalQuery.Combinator.AND);
        }

        public static CompiledCondition of(int columnIndex, Operator operator, int argumentIndex, boolean ignoreCase, LogicalQuery.Combinator nextCombinator) {
            return new CompiledCondition(columnIndex, operator, argumentIndex, ignoreCase, nextCombinator);
        }

        public int columnIndex() {
            return columnIndex;
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

        public LogicalQuery.Combinator nextCombinator() {
            return nextCombinator;
        }
    }

    /**
     * Pre-compiled join metadata.
     */
    public record CompiledJoin(
            String joinPath,
            Class<?> sourceEntity,
            Class<?> targetEntity,
            int sourceColumnIndex,
            int targetColumnIndex,
            boolean targetColumnIsId,
            byte fkTypeCode,
            LogicalQuery.Join.JoinType joinType,
            String relationshipFieldName,
            CompiledJoinPredicate[] predicates,
            io.memris.storage.GeneratedTable targetTable,
            io.memris.runtime.HeapRuntimeKernel targetKernel,
            io.memris.runtime.EntityMaterializer<?> targetMaterializer,
            io.memris.runtime.JoinExecutor executor,
            io.memris.runtime.JoinMaterializer materializer
    ) {
        public CompiledJoin {
            if (joinPath == null || joinPath.isBlank()) {
                throw new IllegalArgumentException("joinPath required");
            }
        }

        public CompiledJoin withRuntime(io.memris.storage.GeneratedTable targetTable,
                                        io.memris.runtime.HeapRuntimeKernel targetKernel,
                                        io.memris.runtime.EntityMaterializer<?> targetMaterializer,
                                        io.memris.runtime.JoinExecutor executor,
                                        io.memris.runtime.JoinMaterializer materializer) {
            return new CompiledJoin(joinPath, sourceEntity, targetEntity, sourceColumnIndex, targetColumnIndex, targetColumnIsId,
                    fkTypeCode, joinType, relationshipFieldName, predicates, targetTable, targetKernel, targetMaterializer, executor, materializer);
        }
    }

    /**
     * Pre-compiled predicate that applies to a joined target entity.
     */
    public record CompiledJoinPredicate(
            String joinPath,
            int columnIndex,
            LogicalQuery.Operator operator,
            int argumentIndex,
            boolean ignoreCase
    ) {
    }

    /**
     * Pre-compiled grouping configuration for Map return types.
     */
    public record CompiledGrouping(
            int[] columnIndices,
            byte[] typeCodes,
            String[] keyProperties,
            Class<?> keyType,
            java.lang.invoke.MethodHandle keyConstructor,
            LogicalQuery.Grouping.GroupValueType valueType
    ) {
    }
}
