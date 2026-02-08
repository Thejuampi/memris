package io.memris.storage.heap;

import net.bytebuddy.dynamic.DynamicType;
import java.util.List;

public class BytecodeImplementation implements TableImplementationStrategy {
    
    @Override
    public DynamicType.Builder<AbstractTable> implementMethods(
            DynamicType.Builder<AbstractTable> builder,
            List<ColumnFieldInfo> columnFields,
            Class<?> idIndexType) {
        return BytecodeTableGenerator.implementMethods(builder, columnFields);
    }
}
