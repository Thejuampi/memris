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
 * Configure via {@link io.memris.spring.MemrisConfiguration}:
 * <pre>
 * MemrisConfiguration config = MemrisConfiguration.builder()
 *     .tableImplementation(TableImplementation.BYTECODE)
 *     .build();
 * </pre>
 *
 * @see TableGenerator
 * @see io.memris.spring.MemrisConfiguration
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
}
