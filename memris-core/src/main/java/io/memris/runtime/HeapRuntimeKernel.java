package io.memris.runtime;

import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.storage.SelectionImpl;

/**
 * Hot-path query execution kernel using type handlers for extensibility.
 *
 * <p>
 * This kernel uses the {@link TypeHandlerRegistry} to dispatch operations
 * to type-specific handlers. This design allows new types to be added without
 * modifying the kernel - simply register a new {@link TypeHandler}.
 *
 * <p>
 * The kernel supports all standard comparison operators:
 * <ul>
 * <li>Equality: EQ, NE</li>
 * <li>Comparison: GT, GTE, LT, LTE</li>
 * <li>Range: BETWEEN</li>
 * <li>Set: IN, NOT_IN</li>
 * <li>Null checks: IS_NULL, IS_NOT_NULL</li>
 * </ul>
 */
public record HeapRuntimeKernel(GeneratedTable table, TypeHandlerRegistry handlerRegistry) {

    /**
     * Create a kernel with the default handler registry.
     */
    public HeapRuntimeKernel(GeneratedTable table) {
        this(table, TypeHandlerRegistry.getDefault());
    }

    /**
     * Create a kernel with a custom handler registry.
     * Use this for testing or when you need custom type handlers.
     */
    public HeapRuntimeKernel {
    }

    public long rowCount() {
        return table.liveCount();
    }

    public long allocatedCount() {
        return table.allocatedCount();
    }

    public int columnCount() {
        return table.columnCount();
    }

    public byte typeCodeAt(int columnIndex) {
        return table.typeCodeAt(columnIndex);
    }

    /**
     * Get the handler registry for this kernel.
     * Can be used to register additional type handlers.
     */
    @Override
    public TypeHandlerRegistry handlerRegistry() {
        return handlerRegistry;
    }

    /**
     * Execute a compiled condition against the table.
     *
     * @param cc   the compiled condition
     * @param args the argument values from the method call
     * @return selection of matching rows
     * @throws IllegalArgumentException if no handler is registered for the column
     *                                  type
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Selection executeCondition(CompiledQuery.CompiledCondition cc, Object[] args) {
        var columnIndex = cc.columnIndex();
        var operator = cc.operator();
        Object value = null;
        if (operator != LogicalQuery.Operator.IS_NULL
                && operator != LogicalQuery.Operator.NOT_NULL
                && operator != LogicalQuery.Operator.IS_TRUE
                && operator != LogicalQuery.Operator.IS_FALSE) {
            value = args[cc.argumentIndex()];
        }

        if (operator == LogicalQuery.Operator.IN || operator == LogicalQuery.Operator.NOT_IN) {
            var inSelection = executeInList(columnIndex, cc.typeCode(), value);
            if (operator == LogicalQuery.Operator.NOT_IN) {
                var all = table.scanAll();
                return subtractSelections(all, inSelection);
            }
            return inSelection;
        }

        if (operator == LogicalQuery.Operator.BETWEEN) {
            var typeCode = cc.typeCode();
            return RuntimeExecutorGenerator.generateBetweenExecutor(columnIndex, typeCode)
                    .execute(table, cc.argumentIndex(), args);
        }

        var typeCode = cc.typeCode();
        var handler = handlerRegistry.getHandler(typeCode);

        if (handler == null) {
            throw new IllegalArgumentException(
                    "No handler registered for type code: " + typeCode +
                            " (column index: " + columnIndex + ")");
        }

        // Convert the value to the handler's type if needed
        var convertedValue = value != null ? handler.convertValue(value) : null;

        return handler.executeCondition(table, columnIndex, operator, convertedValue, cc.ignoreCase());
    }

    private Selection executeInList(int columnIndex, byte typeCode, Object value) {
        return RuntimeExecutorGenerator.generateInListExecutor(columnIndex, typeCode)
                .execute(table, value);
    }

    private Selection subtractSelections(int[] allRows, Selection toRemove) {
        return SelectionImpl.fromScanIndices(table, allRows).subtract(toRemove);
    }
}
