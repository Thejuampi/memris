package io.memris.spring.plan;

import io.memris.spring.EntityMetadata;

/**
 * Compiles logical queries into executable form.
 */
public class QueryCompiler {
    
    private final EntityMetadata<?> metadata;
    
    public QueryCompiler(EntityMetadata<?> metadata) {
        this.metadata = metadata;
    }
    
    /**
     * Compile a logical query.
     * 
     * @param logicalQuery the logical query to compile
     * @return the compiled query
     */
    public CompiledQuery compile(LogicalQuery logicalQuery) {
        LogicalQuery.Condition[] conditions = logicalQuery.conditions();
        CompiledQuery.CompiledCondition[] compiledConditions = new CompiledQuery.CompiledCondition[conditions.length];

        for (int i = 0; i < conditions.length; i++) {
            LogicalQuery.Condition condition = conditions[i];
            int columnIndex = resolveColumnIndex(condition.propertyPath());
            compiledConditions[i] = CompiledQuery.CompiledCondition.of(
                columnIndex,
                condition.operator(),
                condition.argumentIndex(),
                condition.ignoreCase()
            );
        }

        return CompiledQuery.of(
            logicalQuery.opCode(),
            logicalQuery.returnKind(),
            compiledConditions
        );
    }

    /**
     * Resolve a property path to a column index.
     * Handles the special $ID marker for entity ID columns.
     * 
     * @param propertyPath the property path (e.g., "name", "$ID")
     * @return the column index
     * @throws IllegalArgumentException if property not found
     */
    private int resolveColumnIndex(String propertyPath) {
        if (LogicalQuery.Condition.ID_PROPERTY.equals(propertyPath)) {
            // Resolve ID property to its column position
            return metadata.resolveColumnPosition(metadata.idColumnName());
        }
        // For now, assume propertyPath maps directly to a field name
        // TODO: Handle nested property paths (e.g., "address.zip")
        return metadata.resolvePropertyPosition(propertyPath);
    }
}
