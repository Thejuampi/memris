package io.memris.spring.runtime;

import io.memris.spring.TypeCodes;
import io.memris.storage.GeneratedTable;

import java.lang.invoke.MethodHandle;

public final class JoinMaterializerImpl implements JoinMaterializer {
    private final int sourceColumnIndex;
    private final byte fkTypeCode;
    private final MethodHandle setter;

    public JoinMaterializerImpl(int sourceColumnIndex, byte fkTypeCode, MethodHandle setter) {
        this.sourceColumnIndex = sourceColumnIndex;
        this.fkTypeCode = fkTypeCode;
        this.setter = setter;
    }

    @Override
    public void hydrate(Object sourceEntity, int sourceRowIndex, GeneratedTable sourceTable,
                        GeneratedTable targetTable, HeapRuntimeKernel targetKernel, EntityMaterializer<?> targetMaterializer) {
        if (setter == null || targetMaterializer == null) {
            return;
        }
        long fkValue = readFkValue(sourceTable, sourceRowIndex);
        long targetRef = targetTable.lookupById(fkValue);
        if (targetRef < 0) {
            return;
        }
        int targetRow = io.memris.storage.Selection.index(targetRef);
        Object related = targetMaterializer.materialize(targetKernel, targetRow);
        try {
            setter.invoke(sourceEntity, related);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set joined entity", e);
        }
    }

    private long readFkValue(GeneratedTable sourceTable, int rowIndex) {
        return switch (fkTypeCode) {
            case TypeCodes.TYPE_LONG -> sourceTable.readLong(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_INT -> sourceTable.readInt(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_SHORT -> sourceTable.readInt(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_BYTE -> sourceTable.readInt(sourceColumnIndex, rowIndex);
            default -> sourceTable.readLong(sourceColumnIndex, rowIndex);
        };
    }
}
