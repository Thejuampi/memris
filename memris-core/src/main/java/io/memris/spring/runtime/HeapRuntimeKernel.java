package io.memris.spring.runtime;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.LogicalQuery;

/**
 * Hot-path query execution kernel using type handlers for extensibility.
 * 
 * <p>This kernel uses the {@link TypeHandlerRegistry} to dispatch operations
 * to type-specific handlers. This design allows new types to be added without
 * modifying the kernel - simply register a new {@link TypeHandler}.
 * 
 * <p>The kernel supports all standard comparison operators:
 * <ul>
 *   <li>Equality: EQ, NE</li>
 *   <li>Comparison: GT, GTE, LT, LTE</li>
 *   <li>Range: BETWEEN</li>
 *   <li>Set: IN, NOT_IN</li>
 *   <li>Null checks: IS_NULL, IS_NOT_NULL</li>
 * </ul>
 */
public final class HeapRuntimeKernel {
    
    private final GeneratedTable table;
    private final TypeHandlerRegistry handlerRegistry;
    
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
    public HeapRuntimeKernel(GeneratedTable table, TypeHandlerRegistry handlerRegistry) {
        this.table = table;
        this.handlerRegistry = handlerRegistry;
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
    
    public GeneratedTable table() {
        return table;
    }
    
    /**
     * Get the handler registry for this kernel.
     * Can be used to register additional type handlers.
     */
    public TypeHandlerRegistry getHandlerRegistry() {
        return handlerRegistry;
    }
    
    /**
     * Execute a compiled condition against the table.
     * 
     * @param cc the compiled condition
     * @param args the argument values from the method call
     * @return selection of matching rows
     * @throws IllegalArgumentException if no handler is registered for the column type
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public Selection executeCondition(CompiledQuery.CompiledCondition cc, Object[] args) {
        int columnIndex = cc.columnIndex();
        LogicalQuery.Operator operator = cc.operator();
        Object value = args[cc.argumentIndex()];
        
        byte typeCode = table.typeCodeAt(columnIndex);
        TypeHandler handler = handlerRegistry.getHandler(typeCode);
        
        if (handler == null) {
            throw new IllegalArgumentException(
                "No handler registered for type code: " + typeCode + 
                " (column index: " + columnIndex + ")");
        }
        
        // Convert the value to the handler's type if needed
        Object convertedValue = value != null ? handler.convertValue(value) : null;
        
        return handler.executeCondition(table, columnIndex, operator, convertedValue, cc.ignoreCase());
    }
}
