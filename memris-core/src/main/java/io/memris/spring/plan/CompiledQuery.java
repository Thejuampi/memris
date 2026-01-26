package io.memris.spring.plan;

import io.memris.spring.plan.LogicalQuery.Operator;

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
        return new CompiledQuery(opCode, returnKind, conditions, conditions.length);
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

        private CompiledCondition(int columnIndex, Operator operator, int argumentIndex, boolean ignoreCase) {
            this.columnIndex = columnIndex;
            this.operator = operator;
            this.argumentIndex = argumentIndex;
            this.ignoreCase = ignoreCase;
        }

        public static CompiledCondition of(int columnIndex, Operator operator, int argumentIndex) {
            return new CompiledCondition(columnIndex, operator, argumentIndex, false);
        }

        public static CompiledCondition of(int columnIndex, Operator operator, int argumentIndex, boolean ignoreCase) {
            return new CompiledCondition(columnIndex, operator, argumentIndex, ignoreCase);
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
    }
}
