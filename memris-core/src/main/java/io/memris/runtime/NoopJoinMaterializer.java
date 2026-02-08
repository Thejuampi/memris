package io.memris.runtime;

import io.memris.storage.GeneratedTable;

public final class NoopJoinMaterializer implements JoinMaterializer {
    @Override
    public void hydrate(Object sourceEntity, int sourceRowIndex, GeneratedTable sourceTable,
            GeneratedTable targetTable, EntityMaterializer<?> targetMaterializer) {
        // no-op
    }
}
