package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.storage.GeneratedTable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public final class JoinCollectionMaterializer implements JoinMaterializer {
    private final int sourceColumnIndex;
    private final int targetColumnIndex;
    private final byte fkTypeCode;
    private final MethodHandle setter;
    private final MethodHandle postLoadHandle;
    private final boolean useSet;

    public JoinCollectionMaterializer(int sourceColumnIndex, int targetColumnIndex, byte fkTypeCode,
            MethodHandle setter, MethodHandle postLoadHandle, Class<?> collectionType) {
        this.sourceColumnIndex = sourceColumnIndex;
        this.targetColumnIndex = targetColumnIndex;
        this.fkTypeCode = fkTypeCode;
        this.setter = setter;
        this.postLoadHandle = postLoadHandle;
        this.useSet = Set.class.isAssignableFrom(collectionType);
    }

    @Override
    public void hydrate(Object sourceEntity, int sourceRowIndex, GeneratedTable sourceTable,
            GeneratedTable targetTable, EntityMaterializer<?> targetMaterializer) {
        if (setter == null || targetMaterializer == null) {
            return;
        }
        if (!sourceTable.isPresent(sourceColumnIndex, sourceRowIndex)) {
            return;
        }

        long key = readKey(sourceTable, sourceRowIndex);
        int[] matches = switch (fkTypeCode) {
            case TypeCodes.TYPE_LONG -> targetTable.scanEqualsLong(targetColumnIndex, key);
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_BYTE ->
                targetTable.scanEqualsInt(targetColumnIndex, (int) key);
            default -> targetTable.scanEqualsLong(targetColumnIndex, key);
        };

        Collection<Object> collection = createCollection(matches.length);
        for (int targetRow : matches) {
            Object related = targetMaterializer.materialize(targetTable, targetRow);
            invokePostLoad(related);
            collection.add(related);
        }

        try {
            setter.invoke(sourceEntity, collection);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set collection field", e);
        }
    }

    private long readKey(GeneratedTable sourceTable, int rowIndex) {
        return switch (fkTypeCode) {
            case TypeCodes.TYPE_LONG -> sourceTable.readLong(sourceColumnIndex, rowIndex);
            case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_BYTE ->
                sourceTable.readInt(sourceColumnIndex, rowIndex);
            default -> sourceTable.readLong(sourceColumnIndex, rowIndex);
        };
    }

    private Collection<Object> createCollection(int expectedSize) {
        if (useSet) {
            return new LinkedHashSet<>(Math.max(expectedSize, 16));
        }
        return new ArrayList<>(Math.max(expectedSize, 10));
    }

    private void invokePostLoad(Object entity) {
        if (postLoadHandle == null || entity == null) {
            return;
        }
        try {
            postLoadHandle.invoke(entity);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to invoke postLoad", e);
        }
    }
}
