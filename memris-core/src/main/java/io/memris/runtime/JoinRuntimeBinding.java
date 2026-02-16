package io.memris.runtime;

import io.memris.storage.GeneratedTable;

/**
 * Runtime-only join wiring aligned by query index and join index.
 */
public record JoinRuntimeBinding(
        GeneratedTable targetTable,
        HeapRuntimeKernel targetKernel,
        EntityMaterializer<?> targetMaterializer,
        JoinExecutor executor,
        JoinMaterializer materializer
) {
}
