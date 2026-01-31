package io.memris.query;

import io.memris.core.EntityMetadata;

/**
 * Compiles logical queries into executable form.
 */
public class QueryCompiler {
    
    private final EntityMetadata<?> metadata;
    private final java.util.Map<Class<?>, EntityMetadata<?>> metadataCache;
    
    public QueryCompiler(EntityMetadata<?> metadata) {
        this.metadata = metadata;
        this.metadataCache = new java.util.HashMap<>();
        this.metadataCache.put(metadata.entityClass(), metadata);
    }
    
    /**
     * Compile a logical query.
     * 
     * @param logicalQuery the logical query to compile
     * @return the compiled query
     */
    public CompiledQuery compile(LogicalQuery logicalQuery) {
        LogicalQuery.Condition[] conditions = logicalQuery.conditions();
        java.util.List<CompiledQuery.CompiledCondition> baseConditions = new java.util.ArrayList<>();
        java.util.List<CompiledQuery.CompiledJoinPredicate> joinPredicates = new java.util.ArrayList<>();
        java.util.Map<String, CompiledQuery.CompiledJoin> joinsByPath = new java.util.LinkedHashMap<>();
        java.util.Map<String, LogicalQuery.Join.JoinType> joinTypes = new java.util.HashMap<>();
        CompiledQuery.CompiledOrderBy[] compiledOrderBy = null;
        CompiledQuery.CompiledUpdateAssignment[] compiledUpdates = new CompiledQuery.CompiledUpdateAssignment[0];

        if (logicalQuery.joins() != null) {
            for (LogicalQuery.Join join : logicalQuery.joins()) {
                joinTypes.put(join.propertyPath(), join.joinType());
            }
        }

        for (LogicalQuery.Condition condition : conditions) {
            if (LogicalQuery.Condition.ID_PROPERTY.equals(condition.propertyPath())) {
                int columnIndex = resolveColumnIndex(condition.propertyPath(), metadata);
                baseConditions.add(CompiledQuery.CompiledCondition.of(
                    columnIndex,
                    condition.operator(),
                    condition.argumentIndex(),
                    condition.ignoreCase(),
                    condition.nextCombinator()
                ));
                continue;
            }

            String propertyPath = condition.propertyPath();
            if (!propertyPath.contains(".")) {
                int columnIndex = resolveColumnIndex(propertyPath, metadata);
                baseConditions.add(CompiledQuery.CompiledCondition.of(
                    columnIndex,
                    condition.operator(),
                    condition.argumentIndex(),
                    condition.ignoreCase(),
                    condition.nextCombinator()
                ));
                continue;
            }

            boolean handled = compileJoinPath(condition, propertyPath, joinsByPath, joinPredicates, joinTypes);
            if (!handled) {
                int columnIndex = resolveColumnIndex(propertyPath, metadata);
                baseConditions.add(CompiledQuery.CompiledCondition.of(
                    columnIndex,
                    condition.operator(),
                    condition.argumentIndex(),
                    condition.ignoreCase(),
                    condition.nextCombinator()
                ));
            }
        }

        LogicalQuery.OrderBy[] orderBy = logicalQuery.orderBy();
        if (orderBy != null && orderBy.length > 0) {
            compiledOrderBy = new CompiledQuery.CompiledOrderBy[orderBy.length];
            for (int i = 0; i < orderBy.length; i++) {
                int columnIndex = resolveColumnIndex(orderBy[i].propertyPath(), metadata);
                compiledOrderBy[i] = new CompiledQuery.CompiledOrderBy(columnIndex, orderBy[i].ascending());
            }
        }

        LogicalQuery.UpdateAssignment[] updates = logicalQuery.updateAssignments();
        if (updates != null && updates.length > 0) {
            compiledUpdates = new CompiledQuery.CompiledUpdateAssignment[updates.length];
            for (int i = 0; i < updates.length; i++) {
                LogicalQuery.UpdateAssignment update = updates[i];
                int columnIndex = resolveColumnIndex(update.propertyPath(), metadata);
                compiledUpdates[i] = new CompiledQuery.CompiledUpdateAssignment(columnIndex, update.argumentIndex());
            }
        }

        return CompiledQuery.of(
            logicalQuery.opCode(),
            logicalQuery.returnKind(),
            baseConditions.toArray(new CompiledQuery.CompiledCondition[0]),
            compiledUpdates,
            attachJoinPredicates(joinsByPath, joinPredicates),
            compiledOrderBy,
            logicalQuery.limit(),
            logicalQuery.distinct(),
            logicalQuery.boundValues(),
            logicalQuery.parameterIndices(),
            logicalQuery.arity()
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
    private int resolveColumnIndex(String propertyPath, EntityMetadata<?> entityMetadata) {
        if (LogicalQuery.Condition.ID_PROPERTY.equals(propertyPath)) {
            return entityMetadata.resolveColumnPosition(entityMetadata.idColumnName());
        }
        return entityMetadata.resolvePropertyPosition(propertyPath);
    }

    private boolean compileJoinPath(
        LogicalQuery.Condition condition,
        String propertyPath,
        java.util.Map<String, CompiledQuery.CompiledJoin> joinsByPath,
        java.util.List<CompiledQuery.CompiledJoinPredicate> joinPredicates,
        java.util.Map<String, LogicalQuery.Join.JoinType> joinTypes
    ) {
        String[] segments = propertyPath.split("\\.");
        EntityMetadata<?> currentMetadata = metadata;
        Class<?> currentEntity = metadata.entityClass();
        StringBuilder joinPathBuilder = new StringBuilder();
        int idx = 0;

        while (idx < segments.length - 1) {
            String segment = segments[idx];
            EntityMetadata.FieldMapping relationship = findRelationship(currentMetadata, segment);
            if (relationship == null) {
                break;
            }
            if (joinPathBuilder.length() > 0) {
                joinPathBuilder.append('.');
            }
            joinPathBuilder.append(segment);
            String joinPath = joinPathBuilder.toString();

            EntityMetadata<?> targetMetadata = resolveMetadata(relationship.targetEntity());
            int sourceColumnIndex;
            int targetColumnIndex;
            boolean targetColumnIsId;
            String referencedColumn;

            if (relationship.isCollection()) {
                sourceColumnIndex = currentMetadata.resolveColumnPosition(currentMetadata.idColumnName());
                if (relationship.relationshipType() == EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY) {
                    referencedColumn = targetMetadata.idColumnName();
                    targetColumnIndex = targetMetadata.resolveColumnPosition(referencedColumn);
                    targetColumnIsId = true;
                } else {
                    referencedColumn = relationship.columnName();
                    targetColumnIndex = targetMetadata.resolveColumnPosition(referencedColumn);
                    targetColumnIsId = false;
                }
            } else {
                sourceColumnIndex = currentMetadata.resolveColumnPosition(relationship.columnName());
                referencedColumn = relationship.referencedColumnName() != null
                    ? relationship.referencedColumnName()
                    : targetMetadata.idColumnName();
                targetColumnIndex = targetMetadata.resolveColumnPosition(referencedColumn);
                targetColumnIsId = referencedColumn.equals(targetMetadata.idColumnName());
            }

            byte fkTypeCode = relationship.typeCode();
            if (fkTypeCode != io.memris.core.TypeCodes.TYPE_LONG
                && fkTypeCode != io.memris.core.TypeCodes.TYPE_INT
                && fkTypeCode != io.memris.core.TypeCodes.TYPE_SHORT
                && fkTypeCode != io.memris.core.TypeCodes.TYPE_BYTE
                && (relationship.relationshipType() != EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY
                    || fkTypeCode != io.memris.core.TypeCodes.TYPE_STRING)) {
                throw new IllegalArgumentException("Unsupported FK type for join: " + fkTypeCode);
            }
            if (!joinsByPath.containsKey(joinPath)) {
                LogicalQuery.Join.JoinType joinType = joinTypes.getOrDefault(joinPath, LogicalQuery.Join.JoinType.INNER);
                joinsByPath.put(joinPath, new CompiledQuery.CompiledJoin(
                    joinPath,
                    currentEntity,
                    relationship.targetEntity(),
                    sourceColumnIndex,
                    targetColumnIndex,
                    targetColumnIsId,
                    fkTypeCode,
                    joinType,
                    segment,
                    new CompiledQuery.CompiledJoinPredicate[0],
                    null,
                    null,
                    null,
                    null,
                    null
                ));
            }

            currentMetadata = targetMetadata;
            currentEntity = relationship.targetEntity();
            idx++;
        }

        if (joinPathBuilder.length() == 0) {
            return false;
        }

        String joinPath = joinPathBuilder.toString();
        String remaining = String.join(".", java.util.Arrays.copyOfRange(segments, idx, segments.length));
        if (remaining.isEmpty()) {
            throw new IllegalArgumentException("Join property requires target field: " + propertyPath);
        }

        if (remaining.contains(".")) {
            throw new IllegalArgumentException("Nested join properties not yet supported beyond relationship chain: " + propertyPath);
        }

        int targetColumnIndex = resolveColumnIndex(remaining, currentMetadata);
        joinPredicates.add(new CompiledQuery.CompiledJoinPredicate(
            joinPath,
            targetColumnIndex,
            condition.operator(),
            condition.argumentIndex(),
            condition.ignoreCase()
        ));
        return true;
    }

    private CompiledQuery.CompiledJoin[] attachJoinPredicates(
        java.util.Map<String, CompiledQuery.CompiledJoin> joinsByPath,
        java.util.List<CompiledQuery.CompiledJoinPredicate> joinPredicates
    ) {
        if (joinsByPath.isEmpty()) {
            return new CompiledQuery.CompiledJoin[0];
        }

        java.util.Map<String, java.util.List<CompiledQuery.CompiledJoinPredicate>> byPath = new java.util.HashMap<>();
        for (CompiledQuery.CompiledJoinPredicate predicate : joinPredicates) {
            byPath.computeIfAbsent(predicate.joinPath(), key -> new java.util.ArrayList<>()).add(predicate);
        }

        java.util.List<CompiledQuery.CompiledJoin> joins = new java.util.ArrayList<>(joinsByPath.size());
        for (var entry : joinsByPath.entrySet()) {
            var join = entry.getValue();
            var preds = byPath.get(entry.getKey());
            CompiledQuery.CompiledJoinPredicate[] predArray =
                preds != null ? preds.toArray(new CompiledQuery.CompiledJoinPredicate[0]) : new CompiledQuery.CompiledJoinPredicate[0];
            joins.add(new CompiledQuery.CompiledJoin(
                join.joinPath(),
                join.sourceEntity(),
                join.targetEntity(),
                join.sourceColumnIndex(),
                join.targetColumnIndex(),
                join.targetColumnIsId(),
                join.fkTypeCode(),
                join.joinType(),
                join.relationshipFieldName(),
                predArray,
                join.targetTable(),
                join.targetKernel(),
                join.targetMaterializer(),
                join.executor(),
                join.materializer()
            ));
        }

        return joins.toArray(new CompiledQuery.CompiledJoin[0]);
    }

    private EntityMetadata.FieldMapping findRelationship(EntityMetadata<?> entityMetadata, String name) {
        for (EntityMetadata.FieldMapping field : entityMetadata.fields()) {
            if (field.isRelationship() && field.name().equals(name)) {
                return field;
            }
        }
        return null;
    }

    private EntityMetadata<?> resolveMetadata(Class<?> entityClass) {
        return metadataCache.computeIfAbsent(entityClass, io.memris.core.MetadataExtractor::extractEntityMetadata);
    }
}
