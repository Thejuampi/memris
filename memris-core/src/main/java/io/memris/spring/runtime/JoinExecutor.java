package io.memris.spring.runtime;

import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

/**
 * Blazing fast join executor with zero reflection at runtime.
 * 
 * <p>All join logic is pre-compiled using ByteBuddy at repository creation time.
 * This interface provides type-safe, direct method calls for join operations.
 * 
 * <p><b>Performance characteristics:</b>
 * <ul>
 *   <li>Zero reflection in hot path</li>
 *   <li>Direct method calls (no dynamic dispatch)</li>
 *   <li>Pre-computed column indices</li>
 *   <li>Specialized hash join implementations per relationship</li>
 * </ul>
 * 
 * @param <S> Source entity type (e.g., Order)
 * @param <T> Target entity type (e.g., Customer)
 */
public interface JoinExecutor<S, T> {

    /**
     * Filter source rows based on a target selection.
     *
     * @param sourceTable the source table
     * @param targetTable the target table
     * @param sourceSelection pre-filtered selection from source table (null for all rows)
     * @param targetSelection selection of target rows (null for all rows)
     * @return selection of source rows that match the join
     */
    Selection filterJoin(
        GeneratedTable sourceTable,
        GeneratedTable targetTable,
        Selection sourceSelection,
        Selection targetSelection
    );
}
