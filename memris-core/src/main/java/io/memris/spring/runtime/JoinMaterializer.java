package io.memris.spring.runtime;

import io.memris.storage.GeneratedTable;

public interface JoinMaterializer {
    void hydrate(Object sourceEntity,
                 int sourceRowIndex,
                 GeneratedTable sourceTable,
                 GeneratedTable targetTable,
                 HeapRuntimeKernel targetKernel,
                 EntityMaterializer<?> targetMaterializer);
}
