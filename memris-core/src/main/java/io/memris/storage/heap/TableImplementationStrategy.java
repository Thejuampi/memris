package io.memris.storage.heap;

import net.bytebuddy.dynamic.DynamicType;

import java.util.List;

/**
 * Strategy interface for implementing GeneratedTable methods.
 * <p>
 * Two implementations available:
 * <ul>
 *   <li>{@link MethodHandleImplementation} - Default. Reliable, good performance (~5ns overhead)</li>
 *   <li>{@link BytecodeImplementation} - Preview. Maximum performance (~1ns overhead), may have bugs</li>
 * </ul>
 * <p>
 * Toggle via system property: {@code -Dmemris.table.impl=bytecode}
 * 
 * @see TableGenerator
 */
public interface TableImplementationStrategy {
    
    /**
     * Add all GeneratedTable interface methods to the builder.
     * 
     * @param builder ByteBuddy builder
     * @param columnFields list of column field information
     * @param idIndexType ID index class (LongIdIndex or StringIdIndex)
     * @return modified builder
     */
    DynamicType.Builder<AbstractTable> implementMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields,
            Class<?> idIndexType);
    
    /**
     * Column field information needed for implementation.
     */
    record ColumnFieldInfo(String fieldName, Class<?> columnType, byte typeCode, int index) {}
    
    /**
     * Factory to create the appropriate strategy based on configuration.
     */
    static TableImplementationStrategy create() {
        String impl = System.getProperty("memris.table.impl", "methodhandle");
        return switch (impl.toLowerCase()) {
            case "bytecode", "asm", "direct" -> {
                System.err.println("[Memris] Using BYTECODE table implementation (preview/beta)");
                yield new BytecodeImplementation();
            }
            case "methodhandle", "mh", "default" -> {
                System.out.println("[Memris] Using MethodHandle table implementation (default)");
                yield new MethodHandleImplementation();
            }
            default -> throw new IllegalArgumentException(
                    "Unknown table implementation: " + impl + ". Use 'methodhandle' or 'bytecode'");
        };
    }
}
