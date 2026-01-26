package io.memris.spring.plan;

import io.memris.spring.EntityMetadata;
import io.memris.spring.EntityMetadata.FieldMapping;
import io.memris.spring.plan.LogicalQuery.Condition;
import io.memris.spring.plan.LogicalQuery.Operator;

/**
 * Compiles LogicalQuery to CompiledQuery.
 * <p>
 * The compiler resolves all property paths to column indices, enabling
 * zero-allocation execution at runtime. The compiled query contains
 * integer indices instead of strings, allowing direct array access.
 * <p>
 * This is a build-time operation that happens once per query method
 * during repository creation.
 *
 * @see LogicalQuery
 * @see CompiledQuery
 */
public final class QueryCompiler {

    private final EntityMetadata<?> metadata;

    public QueryCompiler(EntityMetadata<?> metadata) {
        this.metadata = metadata;
    }

    /**
     * Compile a LogicalQuery to a CompiledQuery.
     * <p>
     * This resolves all property paths to column indices, enabling
     * direct array access at runtime without string lookups.
     *
     * @param logical the parsed query from QueryPlanner
     * @return a compiled query ready for execution
     */
    public CompiledQuery compile(LogicalQuery logical) {
        Condition[] logicalConditions = logical.conditions();
        CompiledQuery.CompiledCondition[] compiledConditions =
            new CompiledQuery.CompiledCondition[logicalConditions.length];

        for (int i = 0; i < logicalConditions.length; i++) {
            Condition cond = logicalConditions[i];
            int columnIndex = resolveColumnIndex(cond.propertyPath());
            compiledConditions[i] = CompiledQuery.CompiledCondition.of(
                columnIndex,
                cond.operator(),
                cond.argumentIndex(),
                cond.ignoreCase()
            );
        }

        return CompiledQuery.of(
            logical.opCode(),
            logical.returnKind(),
            compiledConditions
        );
    }

    /**
     * Resolve a property path to a column index.
     * <p>
     * Property paths can be:
     * - Simple: "name" → finds "name" column
     * - Nested: "address.zip" → finds "address_zip" column
     * - ID marker: "$ID" → finds the entity's ID column
     * <p>
     * Uses O(1) lookup maps pre-built in EntityMetadata.
     *
     * @param propertyPath the property path to resolve
     * @return the column index (0-based)
     * @throws IllegalArgumentException if property not found
     */
    private int resolveColumnIndex(String propertyPath) {
        // Special case: $ID marker resolves to the entity's ID column
        if (LogicalQuery.Condition.ID_PROPERTY.equals(propertyPath)) {
            return metadata.resolveColumnPosition(metadata.idColumnName());
        }
        // O(1) lookup using pre-built map
        return metadata.resolvePropertyPosition(propertyPath);
    }
}
