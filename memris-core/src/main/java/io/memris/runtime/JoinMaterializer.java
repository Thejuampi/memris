package io.memris.runtime;

import io.memris.storage.GeneratedTable;

public interface JoinMaterializer {
    void hydrate(Object sourceEntity,
            int sourceRowIndex,
            GeneratedTable sourceTable,
            GeneratedTable targetTable,
            EntityMaterializer<?> targetMaterializer);
}
