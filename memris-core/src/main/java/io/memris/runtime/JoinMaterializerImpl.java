package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;

import java.lang.invoke.MethodHandle;

public final class JoinMaterializerImpl implements JoinMaterializer {
    private final int sourceColumnIndex;
    private final int targetColumnIndex;
    private final boolean targetColumnIsId;
    private final byte fkTypeCode;
    private final MethodHandle setter;

    public JoinMaterializerImpl(int sourceColumnIndex, int targetColumnIndex, boolean targetColumnIsId, byte fkTypeCode, MethodHandle setter) {
        this.sourceColumnIndex = sourceColumnIndex;
        this.targetColumnIndex = targetColumnIndex;
        this.targetColumnIsId = targetColumnIsId;
        this.fkTypeCode = fkTypeCode;
        this.setter = setter;
    }

    @Override
    public void hydrate(Object sourceEntity, int sourceRowIndex, GeneratedTable sourceTable,
                        GeneratedTable targetTable, HeapRuntimeKernel targetKernel, EntityMaterializer<?> targetMaterializer) {
        if (setter == null || targetMaterializer == null) {
            return;
        }
        if (!sourceTable.isPresent(sourceColumnIndex, sourceRowIndex)) {
            return;
        }
        long fkValue = readFkValue(sourceTable, sourceRowIndex);
        int targetRow = findTargetRow(targetTable, fkValue);
        if (targetRow < 0) {
            return;
        }
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

    private int findTargetRow(GeneratedTable targetTable, long fkValue) {
        if (targetColumnIsId) {
            long targetRef = targetTable.lookupById(fkValue);
            return targetRef < 0 ? -1 : io.memris.storage.Selection.index(targetRef);
        }

        int[] matches = switch (fkTypeCode) {
            case TypeCodes.TYPE_LONG -> targetTable.scanEqualsLong(targetColumnIndex, fkValue);
            case TypeCodes.TYPE_INT -> targetTable.scanEqualsInt(targetColumnIndex, (int) fkValue);
            case TypeCodes.TYPE_SHORT -> targetTable.scanEqualsInt(targetColumnIndex, (int) fkValue);
            case TypeCodes.TYPE_BYTE -> targetTable.scanEqualsInt(targetColumnIndex, (int) fkValue);
            default -> targetTable.scanEqualsLong(targetColumnIndex, fkValue);
        };
        if (matches.length == 0) {
            return -1;
        }
        return matches[0];
    }
}
