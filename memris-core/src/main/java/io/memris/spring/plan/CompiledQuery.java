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
 * This object is stored in RepositoryRuntime and indexed by queryId.
 *
 * @see QueryCompiler
 * @see LogicalQuery
 */
public final class CompiledQuery {

    private final String methodName;
    private final LogicalQuery.ReturnKind returnKind;
    private final CompiledCondition[] conditions;
    private final int arity;

    private CompiledQuery(
            String methodName,
            LogicalQuery.ReturnKind returnKind,
            CompiledCondition[] conditions) {
        this.methodName = methodName;
        this.returnKind = returnKind;
        this.conditions = conditions;
        this.arity = conditions.length;
    }

    public static CompiledQuery of(
            String methodName,
            LogicalQuery.ReturnKind returnKind,
            CompiledCondition[] conditions) {
        return new CompiledQuery(methodName, returnKind, conditions);
    }

    public String methodName() {
        return methodName;
    }

    public LogicalQuery.ReturnKind returnKind() {
        return returnKind;
    }

    public CompiledCondition[] conditions() {
        return conditions;
    }

    public int arity() {
        return arity;
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

    /**
     * OpCodes for query execution.
     * <p>
     * These allow switch-based dispatch instead of string comparisons.
     * Combined with column indices, this enables zero-allocation execution.
     */
    public enum OpCode {
        SCAN_EQ,
        SCAN_NE,
        SCAN_GT,
        SCAN_GTE,
        SCAN_LT,
        SCAN_LTE,
        SCAN_IN,
        SCAN_NOT_IN,
        SCAN_BETWEEN,
        INDEX_EQ,
        INDEX_IN,
        SCAN_IGNORE_CASE_EQ
    }
}
