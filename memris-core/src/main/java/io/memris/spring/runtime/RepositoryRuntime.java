package io.memris.spring.runtime;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.memris.spring.EntityMetadata;
import io.memris.spring.EntityMetadata.FieldMapping;
import io.memris.spring.MemrisRepositoryFactory;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.CompiledQuery.CompiledCondition;
import io.memris.spring.plan.LogicalQuery.Operator;
import io.memris.spring.plan.LogicalQuery.ReturnKind;
import io.memris.storage.ffm.FfmTable;
import io.memris.kernel.Predicate;

/**
 * Hot-path query execution engine.
 * <p>
 * RepositoryRuntime owns all pre-compiled data needed for query execution:
 * - Table reference
 * - Factory reference (for index queries)
 * - CompiledQuery[] indexed by queryId
 * - Dense arrays for materialization (no maps in hot path)
 * - MethodHandles for entity field access
 * <p>
 * All query methods in generated repositories delegate to this runtime
 * via thin stub methods that call typed entrypoints with constant queryId.
 * <p>
 * <b>Zero reflection at runtime:</b>
 * - No Method object usage
 * - No method name parsing
 * - No String map lookups
 * - Direct array access for field metadata
 *
 * @param <T> the entity type
 */
public final class RepositoryRuntime<T> {

    private final FfmTable table;
    private final MemrisRepositoryFactory factory;
    private final Class<T> entityClass;
    private final String idColumnName;
    private final CompiledQuery[] compiledQueries;
    private final MethodHandle entityConstructor;
    private final String[] columnNames;           // dense array, indexed by column position
    private final byte[] typeCodes;               // dense array, indexed by column position
    private final TypeConverter<?, ?>[] converters;  // dense array, nullable entries
    private final MethodHandle[] setters;         // dense array, indexed by column position

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
        this.table = table;
        this.factory = factory;
        this.entityClass = entityClass;
        this.idColumnName = idColumnName;
        this.compiledQueries = compiledQueries;
        this.entityConstructor = entityConstructor;
        this.columnNames = columnNames;
        this.typeCodes = typeCodes;
        this.converters = converters;
        this.setters = setters;
    }

    // ========================================================================
    // Typed Entrypoints (called from generated repository stubs)
    // ========================================================================

    /**
     * Execute a query with no arguments.
     * Examples: findAll(), count()
     */
    public List<T> list0(int queryId) {
        CompiledQuery cq = compiledQueries[queryId];
        int[] rows = executeQuery(cq, new Object[0]);
        return materialize(rows);
    }

    /**
     * Execute a query with one argument.
     * Examples: findById(id), findByName(name), countByAge(age)
     */
    public List<T> list1(int queryId, Object arg0) {
        CompiledQuery cq = compiledQueries[queryId];
        int[] rows = executeQuery(cq, new Object[]{arg0});
        return materialize(rows);
    }

    /**
     * Execute a query with two arguments.
     * Examples: findByNameAndAge(name, age)
     */
    public List<T> list2(int queryId, Object arg0, Object arg1) {
        CompiledQuery cq = compiledQueries[queryId];
        int[] rows = executeQuery(cq, new Object[]{arg0, arg1});
        return materialize(rows);
    }

    /**
     * Execute a query returning Optional.
     * Examples: findById(id)
     */
    @SuppressWarnings("unchecked")
    public Optional<T> optional1(int queryId, Object arg0) {
        CompiledQuery cq = compiledQueries[queryId];
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
        CompiledQuery cq = compiledQueries[queryId];
        int[] rows = executeQuery(cq, new Object[]{arg0});
        return rows.length > 0;
    }

    /**
     * Execute a count query with no arguments.
     * Examples: count()
     */
    public long count0(int queryId) {
        CompiledQuery cq = compiledQueries[queryId];
        ReturnKind kind = cq.returnKind();
        if (kind == ReturnKind.COUNT_LONG && cq.arity() == 0) {
            return table.rowCount();
        }
        return executeQuery(cq, new Object[0]).length;
    }

    /**
     * Execute a count query with one argument.
     * Examples: countByAge(age)
     */
    public long count1(int queryId, Object arg0) {
        CompiledQuery cq = compiledQueries[queryId];
        return executeQuery(cq, new Object[]{arg0}).length;
    }

    /**
     * Execute a count query with two arguments.
     * Examples: countByNameAndAge(name, age)
     */
    public long count2(int queryId, Object arg0, Object arg1) {
        CompiledQuery cq = compiledQueries[queryId];
        return executeQuery(cq, new Object[]{arg0, arg1}).length;
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
            int rowCount = (int) table.rowCount();
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
     * Column index is pre-compiled, enabling direct array access.
     */
    private int[] executeCondition(CompiledCondition condition, Object[] args) {
        int columnIndex = condition.columnIndex();
        Operator operator = condition.operator();
        Object value = args[condition.argumentIndex()];
        String columnName = columnNames[columnIndex];

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
    private Predicate.Operator mapOperator(Operator op) {
        return switch (op) {
            case EQ -> Predicate.Operator.EQ;
            case NE -> Predicate.Operator.NEQ;
            case GT -> Predicate.Operator.GT;
            case GTE -> Predicate.Operator.GTE;
            case LT -> Predicate.Operator.LT;
            case LTE -> Predicate.Operator.LTE;
            case IN -> Predicate.Operator.IN;
            case NOT_IN -> Predicate.Operator.NOT_IN;
            case IGNORE_CASE_EQ -> Predicate.Operator.IGNORE_CASE;
            default -> throw new UnsupportedOperationException("Operator: " + op);
        };
    }

    /**
     * Scan table by column index with given predicate (optimized hot path).
     * <p>
     * Uses pre-resolved column index for direct array access.
     * TODO: Hook into FfmTable.scan() with Predicate for SIMD optimization
     */
    private int[] scanTableByColumnIndex(int columnIndex, Operator operator, Object value, boolean ignoreCase) {
        int rowCount = (int) table.rowCount();
        List<Integer> matches = new ArrayList<>();

        for (int row = 0; row < rowCount; row++) {
            Object rowValue = getTableValue(columnIndex, row);
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
     * Scan table with given predicate (legacy - uses column name).
     * TODO: Hook into FfmTable.scan() with Predicate
     */
    private int[] scanTable(String columnName, Operator operator, Object value, boolean ignoreCase) {
        // For now, use a simple scan
        // TODO: Replace with FfmTable.scan(Predicate)
        int rowCount = (int) table.rowCount();
        List<Integer> matches = new ArrayList<>();

        for (int row = 0; row < rowCount; row++) {
            Object rowValue = getTableValue(columnName, row);
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
     * Get value from table by column index using typeCode dispatch.
     * <p>
     * This is the hot-path method that uses pre-resolved column index
     * and byte typeCode for zero-allocation type dispatch.
     */
    private Object getTableValue(int columnIndex, int row) {
        byte typeCode = typeCodes[columnIndex];
        String columnName = columnNames[columnIndex];

        // Switch on byte typeCode for zero-allocation dispatch
        return switch (typeCode) {
            case io.memris.spring.TypeCodes.TYPE_INT -> table.getInt(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_LONG -> table.getLong(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_BOOLEAN -> table.getBoolean(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_BYTE -> table.getByte(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_SHORT -> table.getShort(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_FLOAT -> table.getFloat(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_DOUBLE -> table.getDouble(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_CHAR -> table.getChar(columnName, row);
            case io.memris.spring.TypeCodes.TYPE_STRING -> table.getString(columnName, row);
            default -> throw new IllegalArgumentException("Unknown typeCode: " + typeCode);
        };
    }

    /**
     * Get value from table by column name (legacy - for scanTable only).
     * TODO: Remove once scanTable uses column index directly.
     */
    private Object getTableValue(String columnName, int row) {
        // Find column index by name
        int columnIndex = -1;
        for (int i = 0; i < columnNames.length; i++) {
            if (columnNames[i].equals(columnName)) {
                columnIndex = i;
                break;
            }
        }
        if (columnIndex == -1) {
            throw new IllegalArgumentException("Column not found: " + columnName);
        }
        return getTableValue(columnIndex, row);
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
     * Uses dense arrays for all metadata access - no maps, no reflection.
     * Pre-compiled MethodHandles enable direct field setting.
     */
    @SuppressWarnings("unchecked")
    private T materializeOne(int row) {
        try {
            T entity = (T) entityConstructor.invoke();

            for (int i = 0; i < columnNames.length; i++) {
                Object value = getTableValue(columnNames[i], row);

                // Apply converter if present
                TypeConverter<?, ?> converter = converters[i];
                if (converter != null) {
                    value = ((TypeConverter<Object, Object>) converter).fromStorage(value);
                }

                // Set field via MethodHandle
                MethodHandle setter = setters[i];
                if (setter != null) {
                    setter.invoke(entity, value);
                }
            }

            return entity;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to materialize entity", e);
        }
    }

    // ========================================================================
    // Getters (for testing)
    // ========================================================================

    public FfmTable table() {
        return table;
    }

    public MemrisRepositoryFactory factory() {
        return factory;
    }

    public CompiledQuery[] compiledQueries() {
        return compiledQueries;
    }
}
