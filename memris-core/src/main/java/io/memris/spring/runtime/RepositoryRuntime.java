package io.memris.spring.runtime;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.memris.spring.EntityMetadata;
import io.memris.spring.MemrisRepositoryFactory;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.CompiledQuery.CompiledCondition;
import io.memris.spring.plan.LogicalQuery.Operator;
import io.memris.spring.plan.LogicalQuery.ReturnKind;
import io.memris.storage.ffm.FfmTable;

/**
 * Hot-path query execution engine.
 * <p>
 * RepositoryRuntime owns the RepositoryPlan (built ONCE at repository creation)
 * and provides typed entrypoints for query execution.
 * <p>
 * <b>Zero reflection at runtime:</b>
 * - No Method object usage
 * - No method name parsing
 * - No String map lookups
 * - Direct array access for field metadata via RuntimeKernel
 * <p>
 * <b>Hot-path requirements:</b>
 * - Runtime MUST use RuntimeKernel.columnAt(int) for column access
 * - Runtime MUST use pre-resolved column indices from CompiledCondition
 * - Runtime MUST NOT call FfmTable.getX(String, ...) methods
 *
 * @param <T> the entity type
 */
public final class RepositoryRuntime<T> {

    private final RepositoryPlan<T> plan;
    private final MemrisRepositoryFactory factory;

    /**
     * Create a RepositoryRuntime from a RepositoryPlan.
     * <p>
     * This is the correct, non-deprecated constructor.
     *
     * @param plan  the compiled repository plan
     * @param factory the repository factory (for index queries)
     */
    public RepositoryRuntime(RepositoryPlan<T> plan, MemrisRepositoryFactory factory) {
        if (plan == null) {
            throw new IllegalArgumentException("plan required");
        }
        this.plan = plan;
        this.factory = factory;
    }

    // ========================================================================
    // Legacy constructor for backward compatibility
    // TODO: Remove once all callers migrate to RepositoryPlan
    // ========================================================================

    /**
     * @deprecated Use {@link #RepositoryRuntime(RepositoryPlan, MemrisRepositoryFactory)}
     */
    @Deprecated
    public RepositoryRuntime(
            FfmTable table,
            MemrisRepositoryFactory factory,
            Class<T> entityClass,
            String idColumnName,
            CompiledQuery[] compiledQueries,
            MethodHandle entityConstructor,
            String[] columnNames,
            byte[] typeCodes,
            TypeConverter<?, ?>[] converters,
            MethodHandle[] setters) {
        // Delegate to RepositoryPlan.fromLegacy(), then call the proper constructor
        this.plan = RepositoryPlan.fromLegacy(table, entityClass, idColumnName,
                compiledQueries, entityConstructor, columnNames, typeCodes, converters, setters);
        this.factory = factory;
    }

    // ========================================================================
    // Typed Entrypoints (called from generated repository stubs)
    // ========================================================================

    /**
     * Execute a query with no arguments.
     * Examples: findAll(), count()
     */
    public List<T> list0(int queryId) {
        CompiledQuery cq = plan.queries()[queryId];
        int[] rows = executeQuery(cq, new Object[0]);
        return materialize(rows);
    }

    /**
     * Execute a query with one argument.
     * Examples: findById(id), findByName(name), countByAge(age), findAllById(Iterable)
     */
    public List<T> list1(int queryId, Object arg0) {
        CompiledQuery cq = plan.queries()[queryId];

        // Special handling for FIND_ALL_BY_ID
        if (cq.opCode() == io.memris.spring.plan.OpCode.FIND_ALL_BY_ID) {
            return executeFindAllById(cq, arg0);
        }

        int[] rows = executeQuery(cq, new Object[]{arg0});
        return materialize(rows);
    }

    /**
     * Execute a query with two arguments.
     * Examples: findByNameAndAge(name, age)
     */
    public List<T> list2(int queryId, Object arg0, Object arg1) {
        CompiledQuery cq = plan.queries()[queryId];
        int[] rows = executeQuery(cq, new Object[]{arg0, arg1});
        return materialize(rows);
    }

    /**
     * Execute a query returning Optional.
     * Examples: findById(id)
     */
    @SuppressWarnings("unchecked")
    public Optional<T> optional1(int queryId, Object arg0) {
        CompiledQuery cq = plan.queries()[queryId];
        int[] rows = executeQuery(cq, new Object[]{arg0});
        if (rows.length == 0) {
            return Optional.empty();
        }
        return Optional.of((T) materializeOne(rows[0]));
    }

    /**
     * Execute a query returning boolean.
     * Examples: existsById(id)
     */
    public boolean exists1(int queryId, Object arg0) {
        CompiledQuery cq = plan.queries()[queryId];
        int[] rows = executeQuery(cq, new Object[]{arg0});
        return rows.length > 0;
    }

    /**
     * Execute a count query with no arguments.
     * Examples: count()
     */
    public long count0(int queryId) {
        CompiledQuery cq = plan.queries()[queryId];
        ReturnKind kind = cq.returnKind();
        if (kind == ReturnKind.COUNT_LONG && cq.arity() == 0) {
            return plan.kernel().rowCount();
        }
        return executeQuery(cq, new Object[0]).length;
    }

    /**
     * Execute a count query with one argument.
     * Examples: countByAge(age)
     */
    public long count1(int queryId, Object arg0) {
        CompiledQuery cq = plan.queries()[queryId];
        return executeQuery(cq, new Object[]{arg0}).length;
    }

    /**
     * Execute a count query with two arguments.
     * Examples: countByNameAndAge(name, age)
     */
    public long count2(int queryId, Object arg0, Object arg1) {
        CompiledQuery cq = plan.queries()[queryId];
        return executeQuery(cq, new Object[]{arg0, arg1}).length;
    }

    // ========================================================================
    // Unified CRUD/Query Entrypoints
    // ========================================================================

    /**
     * Execute operation with no arguments.
     * Handles: findAll(), deleteAll(), count()
     */
    public void execute0(int queryId) {
        CompiledQuery cq = plan.queries()[queryId];
        ReturnKind kind = cq.returnKind();

        // TODO: Implement actual CRUD operations
        if (kind == ReturnKind.DELETE_ALL) {
            throw new UnsupportedOperationException("DELETE_ALL not yet implemented");
        }
        // For now, let other operations fall through to query execution
        executeQuery(cq, new Object[0]);
    }

    /**
     * Execute operation with one argument.
     * Handles: findById(), save(), delete(), deleteById(), countBy(), existsBy(), saveAll(Iterable), deleteAllById(Iterable)
     */
    public void execute1(int queryId, Object arg0) {
        CompiledQuery cq = plan.queries()[queryId];
        ReturnKind kind = cq.returnKind();

        // Dispatch based on opcode (unified path)
        switch (cq.opCode()) {
            case SAVE_ALL -> executeSaveAll(arg0);
            case DELETE_ALL_BY_ID -> executeDeleteAllById(cq, arg0);
            case SAVE_ONE -> throw new UnsupportedOperationException("SAVE_ONE not yet implemented");
            case DELETE_ONE -> throw new UnsupportedOperationException("DELETE_ONE not yet implemented");
            case DELETE_BY_ID -> throw new UnsupportedOperationException("DELETE_BY_ID not yet implemented");
            default -> {
                // Query operations - execute normally (countBy, existsBy, etc.)
                executeQuery(cq, new Object[]{arg0});
            }
        }
    }

    /**
     * Execute operation with two arguments.
     * Handles: findByXxxAndYyy(), saveAll()
     */
    public void execute2(int queryId, Object arg0, Object arg1) {
        CompiledQuery cq = plan.queries()[queryId];
        ReturnKind kind = cq.returnKind();

        // TODO: Implement actual CRUD operations
        if (kind == ReturnKind.SAVE_ALL) {
            throw new UnsupportedOperationException("SAVE_ALL not yet implemented");
        }
        // Query operations - execute normally
        executeQuery(cq, new Object[]{arg0, arg1});
    }

    // ========================================================================
    // Query Execution
    // ========================================================================

    /**
     * Execute a compiled query with arguments.
     * <p>
     * This is the core hot-path method. It uses pre-compiled conditions
     * with resolved column indices, avoiding all string lookups.
     */
    private int[] executeQuery(CompiledQuery cq, Object[] args) {
        CompiledCondition[] conditions = cq.conditions();

        if (conditions.length == 0) {
            // findAll() - return all rows
            int rowCount = (int) plan.kernel().rowCount();
            int[] rows = new int[rowCount];
            for (int i = 0; i < rowCount; i++) {
                rows[i] = i;
            }
            return rows;
        }

        // Start with first condition
        int[] result = executeCondition(conditions[0], args);

        // Apply remaining conditions (AND logic)
        for (int i = 1; i < conditions.length; i++) {
            int[] conditionRows = executeCondition(conditions[i], args);
            result = intersect(result, conditionRows);
        }

        return result;
    }

    /**
     * Execute a single condition.
     * <p>
     * Uses index if available, otherwise falls back to table scan.
     * Column index is pre-compiled, enabling direct array access via RuntimeKernel.
     */
    private int[] executeCondition(CompiledCondition condition, Object[] args) {
        int columnIndex = condition.columnIndex();
        Operator operator = condition.operator();
        Object value = args[condition.argumentIndex()];

        // Try index first (O(1))
        // Note: hasIndex is package-private, so we skip index usage for now
        // TODO: Make hasIndex public or add a public wrapper
        // if (factory != null && factory.hasIndex(entityClass, columnName)) {
        //     if (operator == Operator.EQ) {
        //         return factory.queryIndex(entityClass, columnName, mapOperator(operator), value);
        //     }
        // }

        // Fall back to table scan using column index (SIMD-optimized)
        return scanTableByColumnIndex(columnIndex, operator, value, condition.ignoreCase());
    }

    /**
     * Map LogicalQuery.Operator to Predicate.Operator for index queries.
     */
    private io.memris.kernel.Predicate.Operator mapOperator(Operator op) {
        return switch (op) {
            case EQ -> io.memris.kernel.Predicate.Operator.EQ;
            case NE -> io.memris.kernel.Predicate.Operator.NEQ;
            case GT -> io.memris.kernel.Predicate.Operator.GT;
            case GTE -> io.memris.kernel.Predicate.Operator.GTE;
            case LT -> io.memris.kernel.Predicate.Operator.LT;
            case LTE -> io.memris.kernel.Predicate.Operator.LTE;
            case IN -> io.memris.kernel.Predicate.Operator.IN;
            case NOT_IN -> io.memris.kernel.Predicate.Operator.NOT_IN;
            case IGNORE_CASE_EQ -> io.memris.kernel.Predicate.Operator.IGNORE_CASE;
            default -> throw new UnsupportedOperationException("Operator: " + op);
        };
    }

    /**
     * Scan table by column index with given predicate (optimized hot path).
     * <p>
     * Uses RuntimeKernel.columnAt(int) for index-based access.
     * This is the ONLY way to access columns in the hot path.
     */
    private int[] scanTableByColumnIndex(int columnIndex, Operator operator, Object value, boolean ignoreCase) {
        int rowCount = (int) plan.kernel().rowCount();
        List<Integer> matches = new ArrayList<>();

        // Get column accessor via RuntimeKernel (index-based, no String lookup)
        RuntimeKernel.FfmColumnAccessor accessor = plan.kernel().columnAt(columnIndex);

        for (int row = 0; row < rowCount; row++) {
            Object rowValue = accessor.getValue(row);
            if (matchesOperator(rowValue, operator, value, ignoreCase)) {
                matches.add(row);
            }
        }

        int[] result = new int[matches.size()];
        for (int i = 0; i < matches.size(); i++) {
            result[i] = matches.get(i);
        }
        return result;
    }

    /**
     * Check if value matches operator.
     */
    private boolean matchesOperator(Object rowValue, Operator operator, Object target, boolean ignoreCase) {
        return switch (operator) {
            case EQ -> eq(rowValue, target, ignoreCase);
            case NE -> !eq(rowValue, target, ignoreCase);
            case GT -> compare(rowValue, target) > 0;
            case GTE -> compare(rowValue, target) >= 0;
            case LT -> compare(rowValue, target) < 0;
            case LTE -> compare(rowValue, target) <= 0;
            case IN -> inCollection(rowValue, target);
            case NOT_IN -> !inCollection(rowValue, target);
            case IGNORE_CASE_EQ -> eq(rowValue, target, true);
            default -> throw new UnsupportedOperationException("Operator: " + operator);
        };
    }

    @SuppressWarnings("unchecked")
    private boolean eq(Object a, Object b, boolean ignoreCase) {
        if (ignoreCase && a instanceof String && b instanceof String) {
            return ((String) a).equalsIgnoreCase((String) b);
        }
        return a != null ? a.equals(b) : b == null;
    }

    @SuppressWarnings("unchecked")
    private int compare(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable<Object>) a).compareTo(b);
        }
        throw new IllegalArgumentException("Cannot compare " + a.getClass() + " with " + b.getClass());
    }

    private boolean inCollection(Object rowValue, Object target) {
        if (!(target instanceof java.util.Collection<?> collection)) {
            return false;
        }
        return collection.contains(rowValue);
    }

    private int[] intersect(int[] a, int[] b) {
        // Simple implementation using sets
        // TODO: Optimize for sorted arrays
        java.util.Set<Integer> setA = new java.util.HashSet<>();
        for (int x : a) setA.add(x);

        List<Integer> result = new ArrayList<>();
        for (int x : b) {
            if (setA.contains(x)) {
                result.add(x);
            }
        }

        int[] arr = new int[result.size()];
        for (int i = 0; i < result.size(); i++) {
            arr[i] = result.get(i);
        }
        return arr;
    }

    // ========================================================================
    // Entity Materialization (Zero Reflection)
    // ========================================================================

    /**
     * Materialize multiple entities.
     */
    private List<T> materialize(int[] rows) {
        List<T> result = new ArrayList<>(rows.length);
        for (int row : rows) {
            result.add(materializeOne(row));
        }
        return result;
    }

    /**
     * Materialize a single entity from a row.
     * <p>
     * Uses the EntityMaterializer from the RepositoryPlan.
     * The materializer is generated bytecode with zero reflection.
     * <p>
     * This is a thin delegation wrapper - entity-specific logic lives in the materializer.
     */
    @SuppressWarnings("unchecked")
    private T materializeOne(int row) {
        // Delegate to plan's materializer, passing the kernel
        return plan.materializer().materialize(plan.kernel(), row);
    }

    // ========================================================================
    // CRUD Operations (SAVE_ALL, FIND_ALL_BY_ID, DELETE_ALL_BY_ID)
    // ========================================================================

    /**
     * Execute SAVE_ALL operation.
     * <p>
     * Extracts field values from each entity using EntityExtractor,
     * then inserts them into the table using FfmTable.insert().
     */
    @SuppressWarnings("unchecked")
    private void executeSaveAll(Object arg0) {
        if (!(arg0 instanceof Iterable<?> it)) {
            throw new IllegalArgumentException("SAVE_ALL expects Iterable, got: " + arg0.getClass());
        }

        FfmTable table = plan.kernel().table();
        EntityExtractor<T> extractor = plan.extractor();

        for (Object o : it) {
            T entity = (T) o;
            // Extract field values in column index order
            Object[] values = extractor.extract(entity);
            // Insert into table (varargs expansion)
            table.insert(values);
        }
    }

    /**
     * Execute FIND_ALL_BY_ID operation.
     * <p>
     * Uses the compiled IN condition to scan for matching IDs,
     * then materializes the matching entities.
     */
    @SuppressWarnings("unchecked")
    private List<T> executeFindAllById(CompiledQuery cq, Object arg0) {
        if (!(arg0 instanceof Iterable<?> ids)) {
            throw new IllegalArgumentException("FIND_ALL_BY_ID expects Iterable, got: " + arg0.getClass());
        }

        // The planner should have compiled this to a single IN condition on the ID column
        CompiledCondition[] conditions = cq.conditions();
        if (conditions.length != 1) {
            throw new IllegalStateException("FIND_ALL_BY_ID expects exactly 1 condition, got: " + conditions.length);
        }

        CompiledCondition cc = conditions[0];
        int columnIndex = cc.columnIndex();

        // Scan table for IDs in the collection
        int[] rows = scanIn(columnIndex, ids);

        // Materialize matching entities
        return materialize(rows);
    }

    /**
     * Execute DELETE_ALL_BY_ID operation.
     * <p>
     * NOTE: This operation requires storage-level delete semantics (tombstone or physical delete).
     * Throwing until delete semantics are designed and implemented in FfmTable/RuntimeKernel.
     */
    @SuppressWarnings("unchecked")
    private void executeDeleteAllById(CompiledQuery cq, Object arg0) {
        throw new UnsupportedOperationException(
                "DELETE_ALL_BY_ID not yet implemented - requires storage-level delete semantics " +
                "(tombstone bitmap or physical delete with index update)");
    }

    /**
     * Scan table for values IN a collection.
     * <p>
     * Returns all row indices where the column value matches any value in the collection.
     * Uses the RuntimeKernel for index-based column access.
     */
    private int[] scanIn(int columnIndex, Iterable<?> values) {
        // Convert to HashSet for O(1) lookups
        java.util.Set<Object> valueSet = new java.util.HashSet<>();
        for (Object v : values) {
            valueSet.add(v);
        }

        int rowCount = (int) plan.kernel().rowCount();
        List<Integer> matches = new ArrayList<>();

        // Get column accessor via RuntimeKernel (index-based, no String lookup)
        RuntimeKernel.FfmColumnAccessor accessor = plan.kernel().columnAt(columnIndex);

        for (int row = 0; row < rowCount; row++) {
            Object rowValue = accessor.getValue(row);
            if (valueSet.contains(rowValue)) {
                matches.add(row);
            }
        }

        int[] result = new int[matches.size()];
        for (int i = 0; i < matches.size(); i++) {
            result[i] = matches.get(i);
        }
        return result;
    }

    // ========================================================================
    // Getters (for testing and scaffolding)
    // ========================================================================

    public RepositoryPlan<T> plan() {
        return plan;
    }

    public FfmTable table() {
        return plan.kernel().table();
    }

    public MemrisRepositoryFactory factory() {
        return factory;
    }

    public CompiledQuery[] compiledQueries() {
        return plan.queries();
    }

    public RuntimeKernel kernel() {
        return plan.kernel();
    }
}
