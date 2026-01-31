package io.memris.runtime;

import io.memris.core.EntityMetadata;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.TypeCodes;
import io.memris.kernel.Predicate;
import io.memris.core.converter.TypeConverter;
import io.memris.query.CompiledQuery;
import io.memris.query.LogicalQuery;
import io.memris.query.OpCode;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hot-path query execution engine.
 * <p>
 * RepositoryRuntime owns RepositoryPlan (built ONCE at repository creation)
 * and provides typed entrypoints for query execution.
 *
 * @param <T> the entity type
 */
public final class RepositoryRuntime<T> {

    private final RepositoryPlan<T> plan;
    private final MemrisRepositoryFactory factory;
    private final EntityMaterializer<T> materializer;
    private final EntityMetadata<T> metadata;
    private final java.util.Map<Class<?>, EntityMetadata<?>> relatedMetadata;
    private final EntitySaver<T> entitySaver;
    private static final AtomicLong idCounter = new AtomicLong(1L);

    /**
     * Create a RepositoryRuntime from a RepositoryPlan.
     *
     * @param plan  the compiled repository plan
     * @param factory the repository factory (for index queries)
     * @param metadata entity metadata for ID generation and field access
     */
    public RepositoryRuntime(RepositoryPlan<T> plan, MemrisRepositoryFactory factory, EntityMetadata<T> metadata) {
        if (plan == null) {
            throw new IllegalArgumentException("plan required");
        }
        this.plan = plan;
        this.factory = factory;
        this.metadata = metadata;
        if (metadata != null && plan.materializersByEntity() != null) {
            @SuppressWarnings("unchecked")
            EntityMaterializer<T> materializer = (EntityMaterializer<T>) plan.materializersByEntity().get(metadata.entityClass());
            this.materializer = materializer != null ? materializer : new EntityMaterializerImpl<>(metadata);
        } else {
            this.materializer = metadata != null ? new EntityMaterializerImpl<>(metadata) : null;
        }
        this.relatedMetadata = buildRelatedMetadata(metadata);
        this.entitySaver = plan.entitySaver();
    }

    private static java.util.Map<Class<?>, EntityMetadata<?>> buildRelatedMetadata(EntityMetadata<?> metadata) {
        if (metadata == null) {
            return java.util.Map.of();
        }
        java.util.Map<Class<?>, EntityMetadata<?>> related = new java.util.HashMap<>();
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.isRelationship() && field.targetEntity() != null) {
                related.putIfAbsent(field.targetEntity(), io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity()));
            }
        }
        return java.util.Map.copyOf(related);
    }

    /**
     * Execute a method call based on the compiled query plan.
     */
    public Object executeMethod(Method method, Object[] args) {
        if (Boolean.getBoolean("memris.fail.method.lookup")) {
            throw new IllegalStateException("Method lookup dispatch is disabled for tests");
        }
        int methodIndex = findMethodIndex(method);
        if (methodIndex < 0) {
            throw new UnsupportedOperationException("Method not found in plan: " + method.getName());
        }

        CompiledQuery query = plan.queries()[methodIndex];
        return executeCompiledQuery(query, args);
    }

    public Object executeMethodIndex(int methodIndex, Object[] args) {
        if (methodIndex < 0 || methodIndex >= plan.queries().length) {
            throw new UnsupportedOperationException("Method index out of range: " + methodIndex);
        }
        CompiledQuery query = plan.queries()[methodIndex];
        return executeCompiledQuery(query, args);
    }

    private int findMethodIndex(Method method) {
        java.lang.reflect.Method[] methods = io.memris.repository.RepositoryMethodIntrospector.extractQueryMethods(
            method.getDeclaringClass()
        );

        for (int i = 0; i < methods.length; i++) {
            if (methods[i].equals(method)) {
                return i;
            }
        }
        return -1;
    }

    private Object executeCompiledQuery(CompiledQuery query, Object[] args) {
        Object[] queryArgs = null;
        switch (query.opCode()) {
            case SAVE_ONE:
                return executeSaveOne(args);
            case SAVE_ALL:
                return executeSaveAll(args);
            case FIND_BY_ID:
                return executeFindById(args);
            case FIND_ALL:
                return executeFindAll();
            case FIND:
                queryArgs = buildQueryArgs(query, args);
                return executeFind(query, queryArgs);
            case COUNT:
                queryArgs = buildQueryArgs(query, args);
                return executeCountFast(query, queryArgs);
            case COUNT_ALL:
                return executeCountAll();
            case EXISTS:
                queryArgs = buildQueryArgs(query, args);
                return executeExistsFast(query, queryArgs);
            case EXISTS_BY_ID:
                return executeExistsById(args);
            case DELETE_ONE:
                executeDeleteOne(args);
                return null;
            case DELETE_ALL:
                executeDeleteAll();
                return null;
            case DELETE_BY_ID:
                executeDeleteById(args);
                return null;
            case DELETE_QUERY:
                queryArgs = buildQueryArgs(query, args);
                return formatModifyingResult(executeDeleteQuery(query, queryArgs), query.returnKind());
            case UPDATE_QUERY:
                queryArgs = buildQueryArgs(query, args);
                return formatModifyingResult(executeUpdateQuery(query, queryArgs), query.returnKind());
            case DELETE_ALL_BY_ID:
                return executeDeleteAllById(args);
            default:
                throw new UnsupportedOperationException("OpCode not implemented: " + query.opCode());
        }
    }

    private Object[] buildQueryArgs(CompiledQuery query, Object[] args) {
        int[] paramIndices = query.parameterIndices();
        Object[] boundValues = query.boundValues();
        int slotCount = 0;
        if (paramIndices != null) {
            slotCount = Math.max(slotCount, paramIndices.length);
        }
        if (boundValues != null) {
            slotCount = Math.max(slotCount, boundValues.length);
        }
        if (slotCount == 0) {
            return args != null ? args : new Object[0];
        }
        Object[] resolved = new Object[slotCount];
        for (int i = 0; i < slotCount; i++) {
            int methodIndex = (paramIndices != null && i < paramIndices.length) ? paramIndices[i] : -1;
            if (methodIndex >= 0) {
                resolved[i] = args[methodIndex];
                continue;
            }
            if (boundValues != null && i < boundValues.length) {
                resolved[i] = boundValues[i];
            }
        }
        return resolved;
    }

    private T executeSaveOne(Object[] args) {
        T entity = (T) args[0];
        GeneratedTable table = plan.table();

        // Use EntitySaver for ID extraction and field access
        Long currentId = entitySaver != null ? entitySaver.extractId(entity) : null;
        boolean isNew = (currentId == null) || isZeroId(currentId);

        Long id = currentId;
        if (isNew) {
            id = generateNextId();
            if (entitySaver != null) {
                entitySaver.setId(entity, id);
            }
            invokeLifecycle(metadata != null ? metadata.prePersistHandle() : null, entity);
            applyAuditFields(entity, true);
        } else {
            invokeLifecycle(metadata != null ? metadata.preUpdateHandle() : null, entity);
            applyAuditFields(entity, false);
        }

        // EntitySaver handles all field extraction, converters, and relationships
        T savedEntity = entitySaver != null ? entitySaver.save(entity, table, id) : entity;

        // Look up row index by ID for index updates
        long packedRef = table.lookupById(id);
        int rowIndex = io.memris.storage.Selection.index(packedRef);

        // Update indexes after save - read values back from table
        int maxColumnPos = metadata.fields().stream()
            .filter(f -> f.columnPosition() >= 0)
            .mapToInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition)
            .max()
            .orElse(0);
        Object[] indexValues = new Object[maxColumnPos + 1];
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() < 0) {
                continue;
            }
            indexValues[field.columnPosition()] = readIndexValue(field, rowIndex);
        }
        updateIndexesOnInsert(indexValues, rowIndex);

        persistManyToMany(entity);

        return savedEntity;
    }

    private Long generateNextId() {
        return idCounter.getAndIncrement();
    }

    private void applyPostLoad(T entity) {
        invokeLifecycle(metadata != null ? metadata.postLoadHandle() : null, entity);
    }

    private void invokeLifecycle(MethodHandle handle, Object entity) {
        if (handle == null || entity == null) {
            return;
        }
        try {
            handle.invoke(entity);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to invoke lifecycle method", e);
        }
    }

    private void applyAuditFields(T entity, boolean isNew) {
        if (metadata == null) {
            return;
        }
        var auditFields = metadata.auditFields();
        if (auditFields.isEmpty()) {
            return;
        }
        Object currentUser = currentAuditUser();
        for (io.memris.core.EntityMetadata.AuditField auditField : auditFields) {
            Object value = null;
            switch (auditField.type()) {
                case CREATED_DATE -> {
                    if (!isNew) {
                        continue;
                    }
                    if (hasValue(entity, auditField.name())) {
                        continue;
                    }
                    value = nowForType(auditField.javaType());
                }
                case LAST_MODIFIED_DATE -> value = nowForType(auditField.javaType());
                case CREATED_BY -> {
                    if (!isNew || currentUser == null) {
                        continue;
                    }
                    if (hasValue(entity, auditField.name())) {
                        continue;
                    }
                    value = coerceUser(currentUser, auditField.javaType());
                }
                case LAST_MODIFIED_BY -> {
                    if (currentUser == null) {
                        continue;
                    }
                    value = coerceUser(currentUser, auditField.javaType());
                }
            }
            if (value == null) {
                continue;
            }
            setFieldValue(entity, auditField.name(), value);
        }
    }

    private Object currentAuditUser() {
        if (factory == null) {
            return null;
        }
        io.memris.core.MemrisConfiguration config = factory.getConfiguration();
        if (config == null || config.auditProvider() == null) {
            return null;
        }
        return config.auditProvider().currentUser();
    }

    private boolean hasValue(Object entity, String fieldName) {
        MethodHandle getter = metadata.fieldGetters().get(fieldName);
        if (getter == null) {
            return false;
        }
        try {
            return getter.invoke(entity) != null;
        } catch (Throwable e) {
            return false;
        }
    }

    private void setFieldValue(Object entity, String fieldName, Object value) {
        MethodHandle setter = metadata.fieldSetters().get(fieldName);
        if (setter == null) {
            return;
        }
        try {
            setter.invoke(entity, value);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set audit field: " + fieldName, e);
        }
    }

    private Object nowForType(Class<?> type) {
        if (type == java.time.Instant.class) {
            return java.time.Instant.now();
        }
        if (type == java.time.LocalDate.class) {
            return java.time.LocalDate.now();
        }
        if (type == java.time.LocalDateTime.class) {
            return java.time.LocalDateTime.now();
        }
        if (type == java.util.Date.class) {
            return new java.util.Date();
        }
        if (type == long.class || type == Long.class) {
            return System.currentTimeMillis();
        }
        return null;
    }

    private Object coerceUser(Object user, Class<?> targetType) {
        if (user == null) {
            return null;
        }
        if (targetType.isAssignableFrom(user.getClass())) {
            return user;
        }
        if (targetType == String.class) {
            return user.toString();
        }
        return null;
    }

    private boolean isZeroId(Object id) {
        if (id == null) return true;
        return switch (id) {
            case Long l -> l == 0L;
            case Integer i -> i == 0;
            case Short s -> s == 0;
            case Byte b -> b == 0;
            case java.util.UUID uuid -> uuid.getMostSignificantBits() == 0 && uuid.getLeastSignificantBits() == 0;
            default -> false;
        };
    }

    private List<T> executeSaveAll(Object[] args) {
        Iterable<T> entities = (Iterable<T>) args[0];
        List<T> saved = new ArrayList<>();
        for (T entity : entities) {
            saved.add(executeSaveOne(new Object[]{entity}));
        }
        return saved;
    }

    private Optional<T> executeFindById(Object[] args) {
        Object id = args[0];
        GeneratedTable table = plan.table();

        long packedRef;
        if (id instanceof Long longId) {
            packedRef = table.lookupById(longId);
        } else if (id instanceof String stringId) {
            packedRef = table.lookupByIdString(stringId);
        } else {
            throw new IllegalArgumentException("Unsupported ID type: " + id.getClass());
        }

        if (packedRef < 0) {
            return Optional.empty();
        }

        int rowIndex = io.memris.storage.Selection.index(packedRef);
        T entity = materializer.materialize(plan.kernel(), rowIndex);
        applyPostLoad(entity);
        hydrateCollections(entity, rowIndex);
        return Optional.of(entity);
    }

    private List<T> executeFindAll() {
        GeneratedTable table = plan.table();
        int[] rowIndices = table.scanAll();

        List<T> results = new ArrayList<>(rowIndices.length);
        for (int rowIndex : rowIndices) {
            T entity = materializer.materialize(plan.kernel(), rowIndex);
            applyPostLoad(entity);
            hydrateCollections(entity, rowIndex);
            results.add(entity);
        }
        return results;
    }

    private Object executeFind(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);

        int[] rows = selection.toIntArray();
        if (query.distinct()) {
            rows = distinctRows(rows);
        }

        int limit = query.limit();
        // Use top-k optimization when limit is small relative to result set
        if (limit > 0 && limit < rows.length && query.orderBy() != null && query.orderBy().length > 0) {
            rows = applyTopKOrderBy(query, rows, limit);
        } else {
            rows = applyOrderBy(query, rows);
        }

        int max = (limit > 0 && limit < rows.length) ? limit : rows.length;

        List<T> results = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            int rowIndex = rows[i];
            T entity = materializer.materialize(plan.kernel(), rowIndex);
            applyPostLoad(entity);
            hydrateJoins(entity, rowIndex, query);
            hydrateCollections(entity, rowIndex);
            results.add(entity);
        }

        return switch (query.returnKind()) {
            case ONE_OPTIONAL -> results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
            case MANY_LIST -> results;
            default -> throw new IllegalStateException("Unexpected return kind for FIND: " + query.returnKind());
        };
    }

    private int[] distinctRows(int[] rows) {
        if (rows.length < 2) {
            return rows;
        }
        java.util.Arrays.sort(rows);
        int unique = 1;
        for (int i = 1; i < rows.length; i++) {
            if (rows[i] != rows[i - 1]) {
                rows[unique++] = rows[i];
            }
        }
        if (unique == rows.length) {
            return rows;
        }
        int[] result = new int[unique];
        System.arraycopy(rows, 0, result, 0, unique);
        return result;
    }

    private long executeCount(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        if (!query.distinct()) {
            return selection.size();
        }
        int[] rows = selection.toIntArray();
        return distinctRows(rows).length;
    }

    private long executeCountAll() {
        return plan.table().liveCount();
    }

    private boolean executeExists(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        return selection.size() > 0;
    }

    /**
     * Fast EXISTS that short-circuits without building full selections.
     * <p>
     * For AND groups: if any condition returns empty, immediately return false.
     * For OR groups: if any condition returns non-empty, immediately return true.
     */
    private boolean executeExistsFast(CompiledQuery query, Object[] args) {
        CompiledQuery.CompiledCondition[] conditions = query.conditions();

        if (conditions.length == 0) {
            // No conditions - check if table has any rows
            return plan.table().liveCount() > 0;
        }

        // For single condition, just check if it matches anything
        if (conditions.length == 1) {
            Selection selection = selectWithIndex(conditions[0], args);
            return selection.size() > 0;
        }

        // Process conditions with short-circuiting
        // Track rows matched by current AND group
        IntAccumulator currentGroup = null;

        for (int i = 0; i < conditions.length; i++) {
            CompiledQuery.CompiledCondition condition = conditions[i];
            Selection next = selectWithIndex(condition, args);
            int nextSize = next.size();

            if (currentGroup == null) {
                // First condition in this group
                if (nextSize == 0) {
                    // Empty result at start of group - check if this is standalone or AND group
                    // If next combinator is AND, the whole group will be empty
                    // If next combinator is OR, we can continue to next group
                    if (condition.nextCombinator() == LogicalQuery.Combinator.AND) {
                        // This AND group will be empty, skip to next OR group
                        // Fast-forward past all AND conditions
                        while (i < conditions.length - 1 && conditions[i].nextCombinator() == LogicalQuery.Combinator.AND) {
                            i++;
                        }
                        currentGroup = null;
                        continue;
                    }
                    // This is a standalone OR condition with empty result - continue
                    currentGroup = null;
                    continue;
                }
                // Non-empty result, start tracking this group
                currentGroup = new IntAccumulator(next.toIntArray());
            } else {
                // Intersect with current AND group
                currentGroup.intersect(next.toIntArray());
                if (currentGroup.isEmpty()) {
                    // AND group became empty - if followed by more ANDs, skip them
                    if (condition.nextCombinator() == LogicalQuery.Combinator.AND) {
                        while (i < conditions.length - 1 && conditions[i].nextCombinator() == LogicalQuery.Combinator.AND) {
                            i++;
                        }
                    }
                    currentGroup = null;
                    continue;
                }
            }

            LogicalQuery.Combinator combinator = condition.nextCombinator();
            if (combinator == LogicalQuery.Combinator.OR) {
                // End of AND group - if we have matches, return true
                if (currentGroup != null && !currentGroup.isEmpty()) {
                    return true;
                }
                currentGroup = null;
            }
        }

        // Check final group
        return currentGroup != null && !currentGroup.isEmpty();
    }

    /**
     * Fast COUNT that counts matches without building full selection arrays.
     * Uses a counter-based approach while still handling AND/OR logic correctly.
     */
    private long executeCountFast(CompiledQuery query, Object[] args) {
        if (query.distinct()) {
            return executeCount(query, args);
        }
        CompiledQuery.CompiledCondition[] conditions = query.conditions();

        if (conditions.length == 0) {
            // No conditions - count all rows
            return plan.table().liveCount();
        }

        // For single condition, just return its count
        if (conditions.length == 1) {
            Selection selection = selectWithIndex(conditions[0], args);
            return selection.size();
        }

        // Need to build proper intersection/union for accurate count
        // But we use IntAccumulator which is more memory-efficient than Selection
        IntAccumulator combined = null;
        IntAccumulator currentGroup = null;

        for (int i = 0; i < conditions.length; i++) {
            CompiledQuery.CompiledCondition condition = conditions[i];
            Selection next = selectWithIndex(condition, args);
            int[] nextRows = next.toIntArray();

            if (currentGroup == null) {
                currentGroup = new IntAccumulator(nextRows);
            } else {
                currentGroup.intersect(nextRows);
            }

            LogicalQuery.Combinator combinator = condition.nextCombinator();
            if (combinator == LogicalQuery.Combinator.OR) {
                // Union this group with combined result
                if (combined == null) {
                    combined = currentGroup;
                } else {
                    combined.union(currentGroup);
                }
                currentGroup = null;
            }
        }

        // Handle final group
        if (currentGroup != null) {
            if (combined == null) {
                combined = currentGroup;
            } else {
                combined.union(currentGroup);
            }
        }

        return combined != null ? combined.size() : 0;
    }

    /**
     * Memory-efficient accumulator for row indices.
     * Used by fast EXISTS and COUNT to avoid creating full Selection objects.
     */
    private static final class IntAccumulator {
        private int[] rows;
        private int size;
        private java.util.BitSet bitSet; // Used for efficient intersection/union

        IntAccumulator(int[] initialRows) {
            if (initialRows.length <= 64) {
                // Small sets: use sorted array
                this.rows = initialRows.clone();
                java.util.Arrays.sort(this.rows);
                this.size = this.rows.length;
            } else {
                // Large sets: use BitSet for efficient operations
                this.bitSet = new java.util.BitSet();
                for (int row : initialRows) {
                    this.bitSet.set(row);
                }
                this.size = initialRows.length;
            }
        }

        boolean isEmpty() {
            return size == 0;
        }

        int size() {
            return size;
        }

        void intersect(int[] other) {
            if (size == 0) {
                return;
            }

            if (bitSet != null) {
                // BitSet intersection
                java.util.BitSet otherBits = new java.util.BitSet();
                for (int row : other) {
                    otherBits.set(row);
                }
                bitSet.and(otherBits);
                size = bitSet.cardinality();
            } else if (other.length <= 64) {
                // Array intersection (both small)
                java.util.Arrays.sort(other);
                int newSize = 0;
                int i = 0, j = 0;
                while (i < size && j < other.length) {
                    if (rows[i] == other[j]) {
                        rows[newSize++] = rows[i];
                        i++;
                        j++;
                    } else if (rows[i] < other[j]) {
                        i++;
                    } else {
                        j++;
                    }
                }
                size = newSize;
            } else {
                // Other is large, convert to BitSet
                java.util.BitSet otherBits = new java.util.BitSet();
                for (int row : other) {
                    otherBits.set(row);
                }
                bitSet = new java.util.BitSet();
                for (int i = 0; i < size; i++) {
                    if (otherBits.get(rows[i])) {
                        bitSet.set(rows[i]);
                    }
                }
                rows = null;
                size = bitSet.cardinality();
            }
        }

        void union(IntAccumulator other) {
            if (other.size == 0) {
                return;
            }
            if (size == 0) {
                if (other.bitSet != null) {
                    bitSet = (java.util.BitSet) other.bitSet.clone();
                } else {
                    rows = other.rows.clone();
                    size = other.size;
                }
                return;
            }

            // Convert both to BitSet for union
            if (bitSet == null) {
                bitSet = new java.util.BitSet();
                for (int i = 0; i < size; i++) {
                    bitSet.set(rows[i]);
                }
                rows = null;
            }

            if (other.bitSet != null) {
                bitSet.or(other.bitSet);
            } else {
                for (int i = 0; i < other.size; i++) {
                    bitSet.set(other.rows[i]);
                }
            }
            size = bitSet.cardinality();
        }

        int[] toIntArray() {
            if (bitSet != null) {
                int[] result = new int[size];
                int idx = 0;
                for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
                    result[idx++] = i;
                }
                return result;
            }
            int[] result = new int[size];
            System.arraycopy(rows, 0, result, 0, size);
            return result;
        }
    }

    private boolean executeExistsById(Object[] args) {
        Object id = args[0];
        GeneratedTable table = plan.table();

        long packedRef;
        if (id instanceof Long longId) {
            packedRef = table.lookupById(longId);
        } else if (id instanceof String stringId) {
            packedRef = table.lookupByIdString(stringId);
        } else {
            throw new IllegalArgumentException("Unsupported ID type: " + id.getClass());
        }

        return packedRef >= 0;
    }

    private void executeDeleteOne(Object[] args) {
        T entity = (T) args[0];
        if (entity == null) {
            throw new IllegalArgumentException("Cannot delete null entity");
        }

        Object id;
        try {
            MethodHandle idGetter = metadata.fieldGetters().get(metadata.idColumnName());
            if (idGetter == null) {
                throw new IllegalStateException("No ID getter found for field: " + metadata.idColumnName());
            }
            id = idGetter.invoke(entity);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to extract ID from entity", e);
        }

        if (id == null) {
            throw new IllegalArgumentException("Cannot delete entity with null ID");
        }

        GeneratedTable table = plan.table();
        long packedRef;
        if (id instanceof Long longId) {
            packedRef = table.lookupById(longId);
        } else if (id instanceof String stringId) {
            packedRef = table.lookupByIdString(stringId);
        } else if (id instanceof Integer intId) {
            packedRef = table.lookupById(intId.longValue());
        } else {
            throw new IllegalArgumentException("Unsupported ID type: " + id.getClass());
        }

        if (packedRef >= 0) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);
        }
    }

    private void executeDeleteAll() {
        GeneratedTable table = plan.table();
        if (metadata != null && factory != null) {
            factory.clearIndexes(metadata.entityClass());
        }
        int[] rowIndices = table.scanAll();

        for (int rowIndex : rowIndices) {
            long packedRef = io.memris.storage.Selection.pack(rowIndex, table.currentGeneration());
            table.tombstone(packedRef);
        }
    }

    private void executeDeleteById(Object[] args) {
        Object id = args[0];
        GeneratedTable table = plan.table();

        long packedRef;
        if (id instanceof Long longId) {
            packedRef = table.lookupById(longId);
        } else if (id instanceof String stringId) {
            packedRef = table.lookupByIdString(stringId);
        } else {
            throw new IllegalArgumentException("Unsupported ID type: " + id.getClass());
        }

        if (packedRef >= 0) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);
        }
    }

    private long executeDeleteAllById(Object[] args) {
        @SuppressWarnings("unchecked")
        Iterable<Object> ids = (Iterable<Object>) args[0];
        GeneratedTable table = plan.table();

        long count = 0;
        for (Object id : ids) {
            long packedRef;
            if (id instanceof Long longId) {
                packedRef = table.lookupById(longId);
            } else if (id instanceof String stringId) {
                packedRef = table.lookupByIdString(stringId);
            } else if (id instanceof Integer intId) {
                packedRef = table.lookupById(intId.longValue());
            } else {
                throw new IllegalArgumentException("Unsupported ID type: " + id.getClass());
            }

            if (packedRef >= 0) {
                int rowIndex = io.memris.storage.Selection.index(packedRef);
                updateIndexesOnDelete(rowIndex);
                table.tombstone(packedRef);
                count++;
            }
        }
        return count;
    }

    private long executeDeleteQuery(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        GeneratedTable table = plan.table();

        long count = 0;
        long[] refs = selection.toRefArray();
        for (long packedRef : refs) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);
            count++;
        }
        return count;
    }

    private long executeUpdateQuery(CompiledQuery query, Object[] args) {
        CompiledQuery.CompiledUpdateAssignment[] updates = query.updateAssignments();
        if (updates == null || updates.length == 0) {
            throw new IllegalArgumentException("@Query update requires SET assignments");
        }

        Selection selection = executeConditions(query, args);
        GeneratedTable table = plan.table();

        long count = 0;
        long[] refs = selection.toRefArray();
        for (long packedRef : refs) {
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            Object[] values = buildRowValues(table, rowIndex);
            applyUpdateAssignments(values, updates, args);

            Object idValue = values[metadata.resolveColumnPosition(metadata.idColumnName())];
            updateIndexesOnDelete(rowIndex);
            table.tombstone(packedRef);

            table.insertFrom(values);
            int newRowIndex = resolveRowIndexById(table, idValue);
            if (newRowIndex >= 0) {
                updateIndexesOnInsert(readIndexValues(newRowIndex), newRowIndex);
            }
            count++;
        }

        return count;
    }

    private Object formatModifyingResult(long count, LogicalQuery.ReturnKind returnKind) {
        return switch (returnKind) {
            case MODIFYING_VOID -> null;
            case MODIFYING_INT -> (int) count;
            case MODIFYING_LONG, COUNT_LONG -> count;
            default -> count;
        };
    }

    private Object[] buildRowValues(GeneratedTable table, int rowIndex) {
        int columnCount = table.columnCount();
        Object[] values = new Object[columnCount];
        for (int i = 0; i < columnCount; i++) {
            if (!table.isPresent(i, rowIndex)) {
                values[i] = null;
                continue;
            }
            byte typeCode = plan.typeCodes()[i];
            values[i] = readStorageValue(table, i, typeCode, rowIndex);
        }
        return values;
    }

    private void applyUpdateAssignments(Object[] values, CompiledQuery.CompiledUpdateAssignment[] updates, Object[] args) {
        for (CompiledQuery.CompiledUpdateAssignment update : updates) {
            int columnIndex = update.columnIndex();
            io.memris.core.EntityMetadata.FieldMapping field = findFieldByColumnIndex(columnIndex);
            Object value = update.argumentIndex() < args.length ? args[update.argumentIndex()] : null;
            if (field != null) {
                TypeConverter<?, ?> converter = metadata.converters().get(field.name());
                if (converter != null) {
                    @SuppressWarnings("unchecked")
                    TypeConverter<Object, Object> typed = (TypeConverter<Object, Object>) converter;
                    value = typed.toStorage(value);
                }
            }
            values[columnIndex] = value;
        }
    }

    private Object readStorageValue(GeneratedTable table, int columnIndex, byte typeCode, int rowIndex) {
        return switch (typeCode) {
            case TypeCodes.TYPE_STRING,
                 TypeCodes.TYPE_BIG_DECIMAL,
                 TypeCodes.TYPE_BIG_INTEGER -> table.readString(columnIndex, rowIndex);
            case TypeCodes.TYPE_LONG,
                 TypeCodes.TYPE_INSTANT,
                 TypeCodes.TYPE_LOCAL_DATE,
                 TypeCodes.TYPE_LOCAL_DATE_TIME,
                 TypeCodes.TYPE_DATE,
                 TypeCodes.TYPE_DOUBLE -> table.readLong(columnIndex, rowIndex);
            default -> table.readInt(columnIndex, rowIndex);
        };
    }

    private int resolveRowIndexById(GeneratedTable table, Object idValue) {
        if (idValue == null) {
            return -1;
        }
        long packedRef;
        if (idValue instanceof String stringId) {
            packedRef = table.lookupByIdString(stringId);
        } else if (idValue instanceof Number number) {
            packedRef = table.lookupById(number.longValue());
        } else {
            return -1;
        }
        return packedRef < 0 ? -1 : io.memris.storage.Selection.index(packedRef);
    }

    private Object[] readIndexValues(int rowIndex) {
        int maxColumnPos = metadata.fields().stream()
            .filter(f -> f.columnPosition() >= 0)
            .mapToInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition)
            .max()
            .orElse(0);
        Object[] indexValues = new Object[maxColumnPos + 1];
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() < 0) {
                continue;
            }
            indexValues[field.columnPosition()] = readIndexValue(field, rowIndex);
        }
        return indexValues;
    }

    private io.memris.core.EntityMetadata.FieldMapping findFieldByColumnIndex(int columnIndex) {
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() == columnIndex) {
                return field;
            }
        }
        return null;
    }

    private void updateIndexesOnInsert(Object[] indexValues, int rowIndex) {
        if (metadata == null || factory == null) {
            return;
        }
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() < 0) {
                continue;
            }
            Object value = indexValues[field.columnPosition()];
            factory.addIndexEntry(metadata.entityClass(), field.name(), value, rowIndex);
        }
    }

    private void updateIndexesOnDelete(int rowIndex) {
        if (metadata == null || factory == null) {
            return;
        }
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() < 0) {
                continue;
            }
            Object value = readIndexValue(field, rowIndex);
            factory.removeIndexEntry(metadata.entityClass(), field.name(), value, rowIndex);
        }
    }

    private Object readIndexValue(io.memris.core.EntityMetadata.FieldMapping field, int rowIndex) {
        int columnIndex = field.columnPosition();
        GeneratedTable table = plan.table();
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }

        byte typeCode = field.typeCode();
        Object storage = switch (typeCode) {
            case TypeCodes.TYPE_STRING,
                TypeCodes.TYPE_BIG_DECIMAL,
                TypeCodes.TYPE_BIG_INTEGER -> table.readString(columnIndex, rowIndex);
            case TypeCodes.TYPE_LONG,
                TypeCodes.TYPE_INSTANT,
                TypeCodes.TYPE_LOCAL_DATE,
                TypeCodes.TYPE_LOCAL_DATE_TIME,
                TypeCodes.TYPE_DATE,
                TypeCodes.TYPE_DOUBLE -> table.readLong(columnIndex, rowIndex);
            default -> table.readInt(columnIndex, rowIndex);
        };

        TypeConverter<?, ?> converter = metadata.converters().get(field.name());
        if (converter != null && storage != null) {
            @SuppressWarnings("unchecked")
            TypeConverter<Object, Object> typedConverter = (TypeConverter<Object, Object>) converter;
            return typedConverter.fromStorage(storage);
        }

        return switch (typeCode) {
            case TypeCodes.TYPE_LONG -> (Long) storage;
            case TypeCodes.TYPE_INT -> (Integer) storage;
            case TypeCodes.TYPE_BOOLEAN -> ((Integer) storage) != 0;
            case TypeCodes.TYPE_BYTE -> (byte) (int) storage;
            case TypeCodes.TYPE_SHORT -> (short) (int) storage;
            case TypeCodes.TYPE_FLOAT -> Float.intBitsToFloat((Integer) storage);
            case TypeCodes.TYPE_DOUBLE -> Double.longBitsToDouble((Long) storage);
            case TypeCodes.TYPE_CHAR -> (char) (int) storage;
            case TypeCodes.TYPE_STRING -> storage;
            default -> storage;
        };
    }

    private Selection executeConditions(CompiledQuery query, Object[] args) {
        CompiledQuery.CompiledCondition[] conditions = query.conditions();

        if (conditions.length == 0) {
            int[] allRows = plan.table().scanAll();
            long[] packed = new long[allRows.length];
            for (int i = 0; i < allRows.length; i++) {
                packed[i] = io.memris.storage.Selection.pack(allRows[i], plan.table().currentGeneration());
            }
            return applyJoins(query, args, new io.memris.storage.SelectionImpl(packed));
        }

        Selection combined = null;
        Selection currentGroup = null;

        for (int i = 0; i < conditions.length; i++) {
            Selection next = selectWithIndex(conditions[i], args);
            currentGroup = (currentGroup == null) ? next : currentGroup.intersect(next);

            LogicalQuery.Combinator combinator = conditions[i].nextCombinator();
            if (combinator == LogicalQuery.Combinator.OR) {
                combined = (combined == null) ? currentGroup : combined.union(currentGroup);
                currentGroup = null;
            }
        }

        if (currentGroup != null) {
            combined = (combined == null) ? currentGroup : combined.union(currentGroup);
        }

        return applyJoins(query, args, combined);
    }

    private Selection selectWithIndex(CompiledQuery.CompiledCondition condition, Object[] args) {
        if (metadata == null || factory == null || condition.ignoreCase()) {
            return plan.kernel().executeCondition(condition, args);
        }

        String fieldName = resolveFieldName(condition.columnIndex());
        if (fieldName == null) {
            return plan.kernel().executeCondition(condition, args);
        }

        LogicalQuery.Operator operator = condition.operator();
        Object value = null;
        if (operator != LogicalQuery.Operator.IS_NULL && operator != LogicalQuery.Operator.NOT_NULL) {
            value = args[condition.argumentIndex()];
        }

        if (operator == LogicalQuery.Operator.IN) {
            Selection selection = selectWithIndexForIn(fieldName, value);
            return selection != null ? selection : plan.kernel().executeCondition(condition, args);
        }

        if (operator == LogicalQuery.Operator.BETWEEN) {
            Object[] range = new Object[]{args[condition.argumentIndex()], args[condition.argumentIndex() + 1]};
            int[] rows = factory.queryIndex(metadata.entityClass(), fieldName, operator.toPredicateOperator(), range);
            return rows != null ? selectionFromRows(rows) : plan.kernel().executeCondition(condition, args);
        }

        switch (operator) {
            case EQ, GT, GTE, LT, LTE -> {
                int[] rows = factory.queryIndex(metadata.entityClass(), fieldName, operator.toPredicateOperator(), value);
                if (rows != null) {
                    return selectionFromRows(rows);
                }
            }
            default -> {
            }
        }

        return plan.kernel().executeCondition(condition, args);
    }

    private Selection selectWithIndexForIn(String fieldName, Object value) {
        Iterable<?> iterable = null;
        if (value instanceof Iterable<?> it) {
            iterable = it;
        } else if (value instanceof Object[] arr) {
            iterable = java.util.Arrays.asList(arr);
        }

        if (iterable == null) {
            if (value instanceof int[] ints) {
                Selection combined = null;
                for (int v : ints) {
                    int[] rows = factory.queryIndex(metadata.entityClass(), fieldName, Predicate.Operator.EQ, v);
                    if (rows == null) {
                        return null;
                    }
                    Selection next = selectionFromRows(rows);
                    combined = combined == null ? next : combined.union(next);
                }
                return combined;
            }
            if (value instanceof long[] longs) {
                Selection combined = null;
                for (long v : longs) {
                    int[] rows = factory.queryIndex(metadata.entityClass(), fieldName, Predicate.Operator.EQ, v);
                    if (rows == null) {
                        return null;
                    }
                    Selection next = selectionFromRows(rows);
                    combined = combined == null ? next : combined.union(next);
                }
                return combined;
            }
            return null;
        }

        Selection combined = null;
        for (Object item : iterable) {
            int[] rows = factory.queryIndex(metadata.entityClass(), fieldName, Predicate.Operator.EQ, item);
            if (rows == null) {
                return null;
            }
            Selection next = selectionFromRows(rows);
            combined = combined == null ? next : combined.union(next);
        }
        return combined;
    }

    private String resolveFieldName(int columnIndex) {
        // Use precomputed column names array for O(1) lookup
        String[] columnNames = plan.columnNames();
        if (columnIndex >= 0 && columnIndex < columnNames.length) {
            return columnNames[columnIndex];
        }
        return null;
    }

    private Selection selectionFromRows(int[] rows) {
        long[] packed = new long[rows.length];
        long gen = plan.table().currentGeneration();
        for (int i = 0; i < rows.length; i++) {
            packed[i] = io.memris.storage.Selection.pack(rows[i], gen);
        }
        return new SelectionImpl(packed);
    }

    private int[] applyOrderBy(CompiledQuery query, int[] rows) {
        CompiledQuery.CompiledOrderBy[] orderBy = query.orderBy();
        if (orderBy == null || orderBy.length == 0 || rows.length < 2) {
            return rows;
        }

        if (orderBy.length == 1) {
            return applySingleOrderBy(rows, orderBy[0]);
        }

        return sortByMultipleColumns(rows, orderBy);
    }

    private int[] applySingleOrderBy(int[] rows, CompiledQuery.CompiledOrderBy orderBy) {

        int columnIndex = orderBy.columnIndex();
        boolean ascending = orderBy.ascending();
        byte typeCode = plan.table().typeCodeAt(columnIndex);

        return switch (typeCode) {
            case TypeCodes.TYPE_INT,
                TypeCodes.TYPE_BOOLEAN,
                TypeCodes.TYPE_BYTE,
                TypeCodes.TYPE_SHORT,
                TypeCodes.TYPE_CHAR -> sortByIntColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_FLOAT -> sortByFloatColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_LONG,
                TypeCodes.TYPE_INSTANT,
                TypeCodes.TYPE_LOCAL_DATE,
                TypeCodes.TYPE_LOCAL_DATE_TIME,
                TypeCodes.TYPE_DATE -> sortByLongColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_DOUBLE -> sortByDoubleColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_STRING,
                TypeCodes.TYPE_BIG_DECIMAL,
                TypeCodes.TYPE_BIG_INTEGER -> sortByStringColumn(rows, columnIndex, ascending);
            default -> throw new UnsupportedOperationException("Unsupported sort type code: " + typeCode);
        };
    }

    private int[] applyTopKOrderBy(CompiledQuery query, int[] rows, int k) {
        CompiledQuery.CompiledOrderBy[] orderBy = query.orderBy();
        if (orderBy.length == 1) {
            return topKSingleColumn(rows, orderBy[0], k);
        }
        // For multi-column, fall back to full sort then limit (optimization: could use bounded heap)
        rows = sortByMultipleColumns(rows, orderBy);
        if (k < rows.length) {
            int[] limited = new int[k];
            System.arraycopy(rows, 0, limited, 0, k);
            return limited;
        }
        return rows;
    }

    private int[] topKSingleColumn(int[] rows, CompiledQuery.CompiledOrderBy orderBy, int k) {
        int columnIndex = orderBy.columnIndex();
        boolean ascending = orderBy.ascending();
        byte typeCode = plan.table().typeCodeAt(columnIndex);

        return switch (typeCode) {
            case TypeCodes.TYPE_INT,
                TypeCodes.TYPE_BOOLEAN,
                TypeCodes.TYPE_BYTE,
                TypeCodes.TYPE_SHORT,
                TypeCodes.TYPE_CHAR -> topKInt(rows, columnIndex, ascending, k);
            case TypeCodes.TYPE_LONG,
                TypeCodes.TYPE_INSTANT,
                TypeCodes.TYPE_LOCAL_DATE,
                TypeCodes.TYPE_LOCAL_DATE_TIME,
                TypeCodes.TYPE_DATE -> topKLong(rows, columnIndex, ascending, k);
            case TypeCodes.TYPE_STRING,
                TypeCodes.TYPE_BIG_DECIMAL,
                TypeCodes.TYPE_BIG_INTEGER -> topKString(rows, columnIndex, ascending, k);
            default -> {
                // Fall back to full sort for unsupported types
                int[] sorted = sortBySingleColumn(rows, orderBy);
                if (k < sorted.length) {
                    int[] limited = new int[k];
                    System.arraycopy(sorted, 0, limited, 0, k);
                    yield limited;
                }
                yield sorted;
            }
        };
    }

    private int[] topKInt(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        // Use quickselect-like approach: partial sort to find top k
        int[] result = rows.clone();
        int[] keys = new int[result.length];
        boolean[] present = new boolean[result.length];
        for (int i = 0; i < result.length; i++) {
            int row = result[i];
            present[i] = table.isPresent(columnIndex, row);
            keys[i] = table.readInt(columnIndex, row);
        }
        // Use heap-based selection for top-k
        topKHeapInt(result, keys, present, k, ascending);
        // Sort the top k results
        quickSortInt(result, keys, present, 0, k - 1, ascending);
        // Trim to k elements
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private void topKHeapInt(int[] rows, int[] keys, boolean[] present, int k, boolean ascending) {
        // Build a max-heap (if ascending) or min-heap (if descending) for the first k elements
        // Then for remaining elements, compare with heap root and replace if better
        int n = rows.length;
        // Build heap for first k elements
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyInt(rows, keys, present, i, k, ascending);
        }
        // Process remaining elements
        for (int i = k; i < n; i++) {
            int rootCompare = compareIntForTopK(keys[i], present[i], keys[0], present[0], ascending);
            if (rootCompare < 0) {
                // Current element is better than heap root, replace it
                rows[0] = rows[i];
                keys[0] = keys[i];
                present[0] = present[i];
                heapifyInt(rows, keys, present, 0, k, ascending);
            }
        }
    }

    private void heapifyInt(int[] rows, int[] keys, boolean[] present, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareIntForTopK(keys[left], present[left], keys[largest], present[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size && compareIntForTopK(keys[right], present[right], keys[largest], present[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            swap(present, i, largest);
            heapifyInt(rows, keys, present, largest, size, maxHeap);
        }
    }

    private int compareIntForTopK(int keyA, boolean presentA, int keyB, boolean presentB, boolean ascending) {
        int cmp = compareNullable(presentA, presentB);
        if (cmp == 0) {
            cmp = Integer.compare(keyA, keyB);
        }
        return ascending ? cmp : -cmp;
    }

    private int[] topKLong(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        long[] keys = new long[result.length];
        boolean[] present = new boolean[result.length];
        for (int i = 0; i < result.length; i++) {
            int row = result[i];
            present[i] = table.isPresent(columnIndex, row);
            keys[i] = table.readLong(columnIndex, row);
        }
        topKHeapLong(result, keys, present, k, ascending);
        quickSortLong(result, keys, present, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private void topKHeapLong(int[] rows, long[] keys, boolean[] present, int k, boolean ascending) {
        int n = rows.length;
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyLong(rows, keys, present, i, k, ascending);
        }
        for (int i = k; i < n; i++) {
            int rootCompare = compareLongForTopK(keys[i], present[i], keys[0], present[0], ascending);
            if (rootCompare < 0) {
                rows[0] = rows[i];
                keys[0] = keys[i];
                present[0] = present[i];
                heapifyLong(rows, keys, present, 0, k, ascending);
            }
        }
    }

    private void heapifyLong(int[] rows, long[] keys, boolean[] present, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareLongForTopK(keys[left], present[left], keys[largest], present[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size && compareLongForTopK(keys[right], present[right], keys[largest], present[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            swap(present, i, largest);
            heapifyLong(rows, keys, present, largest, size, maxHeap);
        }
    }

    private int compareLongForTopK(long keyA, boolean presentA, long keyB, boolean presentB, boolean ascending) {
        int cmp = compareNullable(presentA, presentB);
        if (cmp == 0) {
            cmp = Long.compare(keyA, keyB);
        }
        return ascending ? cmp : -cmp;
    }

    private int[] topKString(int[] rows, int columnIndex, boolean ascending, int k) {
        GeneratedTable table = plan.table();
        int[] result = rows.clone();
        String[] keys = new String[result.length];
        for (int i = 0; i < result.length; i++) {
            keys[i] = table.readString(columnIndex, result[i]);
        }
        topKHeapString(result, keys, k, ascending);
        quickSortString(result, keys, 0, k - 1, ascending);
        int[] trimmed = new int[k];
        System.arraycopy(result, 0, trimmed, 0, k);
        return trimmed;
    }

    private void topKHeapString(int[] rows, String[] keys, int k, boolean ascending) {
        int n = rows.length;
        for (int i = k / 2 - 1; i >= 0; i--) {
            heapifyString(rows, keys, i, k, ascending);
        }
        for (int i = k; i < n; i++) {
            int rootCompare = compareStringForTopK(keys[i], keys[0], ascending);
            if (rootCompare < 0) {
                rows[0] = rows[i];
                keys[0] = keys[i];
                heapifyString(rows, keys, 0, k, ascending);
            }
        }
    }

    private void heapifyString(int[] rows, String[] keys, int i, int size, boolean maxHeap) {
        int largest = i;
        int left = 2 * i + 1;
        int right = 2 * i + 2;

        if (left < size && compareStringForTopK(keys[left], keys[largest], maxHeap) > 0) {
            largest = left;
        }
        if (right < size && compareStringForTopK(keys[right], keys[largest], maxHeap) > 0) {
            largest = right;
        }

        if (largest != i) {
            swap(rows, i, largest);
            swap(keys, i, largest);
            heapifyString(rows, keys, largest, size, maxHeap);
        }
    }

    private int compareStringForTopK(String keyA, String keyB, boolean ascending) {
        int cmp = compareStringValue(keyA, keyB);
        return ascending ? cmp : -cmp;
    }

    private int compareStringValue(String a, String b) {
        if (a == null && b == null) return 0;
        if (a == null) return 1; // nulls last
        if (b == null) return -1;
        return a.compareTo(b);
    }

    private int[] sortByMultipleColumns(int[] rows, CompiledQuery.CompiledOrderBy[] orderBy) {
        int[] result = rows.clone();
        OrderKey[] keys = buildOrderKeys(result, orderBy);
        quickSortMulti(result, keys, 0, result.length - 1);
        return result;
    }

    private OrderKey[] buildOrderKeys(int[] rows, CompiledQuery.CompiledOrderBy[] orderBy) {
        GeneratedTable table = plan.table();
        OrderKey[] keys = new OrderKey[orderBy.length];
        for (int i = 0; i < orderBy.length; i++) {
            int columnIndex = orderBy[i].columnIndex();
            boolean ascending = orderBy[i].ascending();
            byte typeCode = table.typeCodeAt(columnIndex);
            boolean[] present = new boolean[rows.length];
            OrderKey key = new OrderKey(typeCode, ascending, present);
            switch (typeCode) {
                case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_BOOLEAN,
                    TypeCodes.TYPE_BYTE,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR -> {
                    int[] values = new int[rows.length];
                    for (int r = 0; r < rows.length; r++) {
                        int row = rows[r];
                        present[r] = table.isPresent(columnIndex, row);
                        values[r] = table.readInt(columnIndex, row);
                    }
                    key.intKeys = values;
                }
                case TypeCodes.TYPE_FLOAT -> {
                    float[] values = new float[rows.length];
                    for (int r = 0; r < rows.length; r++) {
                        int row = rows[r];
                        present[r] = table.isPresent(columnIndex, row);
                        values[r] = Float.intBitsToFloat(table.readInt(columnIndex, row));
                    }
                    key.floatKeys = values;
                }
                case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE -> {
                    long[] values = new long[rows.length];
                    for (int r = 0; r < rows.length; r++) {
                        int row = rows[r];
                        present[r] = table.isPresent(columnIndex, row);
                        values[r] = table.readLong(columnIndex, row);
                    }
                    key.longKeys = values;
                }
                case TypeCodes.TYPE_DOUBLE -> {
                    double[] values = new double[rows.length];
                    for (int r = 0; r < rows.length; r++) {
                        int row = rows[r];
                        present[r] = table.isPresent(columnIndex, row);
                        values[r] = Double.longBitsToDouble(table.readLong(columnIndex, row));
                    }
                    key.doubleKeys = values;
                }
                case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER -> {
                    String[] values = new String[rows.length];
                    for (int r = 0; r < rows.length; r++) {
                        int row = rows[r];
                        values[r] = table.readString(columnIndex, row);
                        present[r] = values[r] != null;
                    }
                    key.stringKeys = values;
                }
                default -> throw new UnsupportedOperationException("Unsupported sort type code: " + typeCode);
            }
            keys[i] = key;
        }
        return keys;
    }

    private static void quickSortMulti(int[] rows, OrderKey[] keys, int low, int high) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);

        while (i <= j) {
            while (compareMulti(i, pivotIndex, rows, keys) < 0) {
                i++;
            }
            while (compareMulti(j, pivotIndex, rows, keys) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                for (OrderKey key : keys) {
                    key.swap(i, j);
                }
                i++;
                j--;
            }
        }

        if (low < j) quickSortMulti(rows, keys, low, j);
        if (i < high) quickSortMulti(rows, keys, i, high);
    }

    private static int compareMulti(int left, int right, int[] rows, OrderKey[] keys) {
        if (left == right) {
            return 0;
        }
        for (OrderKey key : keys) {
            int cmp = key.compare(left, right);
            if (cmp != 0) {
                return cmp;
            }
        }
        return Integer.compare(rows[left], rows[right]);
    }

    private static final class OrderKey {
        private final byte typeCode;
        private final boolean ascending;
        private final boolean[] present;
        private int[] intKeys;
        private long[] longKeys;
        private float[] floatKeys;
        private double[] doubleKeys;
        private String[] stringKeys;

        OrderKey(byte typeCode, boolean ascending, boolean[] present) {
            this.typeCode = typeCode;
            this.ascending = ascending;
            this.present = present;
        }

        int compare(int left, int right) {
            int cmp = compareNullable(present[left], present[right]);
            if (cmp == 0) {
                cmp = switch (typeCode) {
                    case TypeCodes.TYPE_INT,
                        TypeCodes.TYPE_BOOLEAN,
                        TypeCodes.TYPE_BYTE,
                        TypeCodes.TYPE_SHORT,
                        TypeCodes.TYPE_CHAR -> Integer.compare(intKeys[left], intKeys[right]);
                    case TypeCodes.TYPE_FLOAT -> Float.compare(floatKeys[left], floatKeys[right]);
                    case TypeCodes.TYPE_LONG,
                        TypeCodes.TYPE_INSTANT,
                        TypeCodes.TYPE_LOCAL_DATE,
                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                        TypeCodes.TYPE_DATE -> Long.compare(longKeys[left], longKeys[right]);
                    case TypeCodes.TYPE_DOUBLE -> Double.compare(doubleKeys[left], doubleKeys[right]);
                    case TypeCodes.TYPE_STRING,
                        TypeCodes.TYPE_BIG_DECIMAL,
                        TypeCodes.TYPE_BIG_INTEGER -> compareStringValue(stringKeys[left], stringKeys[right]);
                    default -> 0;
                };
            }
            return ascending ? cmp : -cmp;
        }

        void swap(int i, int j) {
            RepositoryRuntime.swap(present, i, j);
            switch (typeCode) {
                case TypeCodes.TYPE_INT,
                    TypeCodes.TYPE_BOOLEAN,
                    TypeCodes.TYPE_BYTE,
                    TypeCodes.TYPE_SHORT,
                    TypeCodes.TYPE_CHAR -> RepositoryRuntime.swap(intKeys, i, j);
                case TypeCodes.TYPE_FLOAT -> RepositoryRuntime.swap(floatKeys, i, j);
                case TypeCodes.TYPE_LONG,
                    TypeCodes.TYPE_INSTANT,
                    TypeCodes.TYPE_LOCAL_DATE,
                    TypeCodes.TYPE_LOCAL_DATE_TIME,
                    TypeCodes.TYPE_DATE -> RepositoryRuntime.swap(longKeys, i, j);
                case TypeCodes.TYPE_DOUBLE -> RepositoryRuntime.swap(doubleKeys, i, j);
                case TypeCodes.TYPE_STRING,
                    TypeCodes.TYPE_BIG_DECIMAL,
                    TypeCodes.TYPE_BIG_INTEGER -> RepositoryRuntime.swap(stringKeys, i, j);
                default -> {
                }
            }
        }

        private static int compareStringValue(String left, String right) {
            if (left == null && right == null) {
                return 0;
            }
            if (left == null) {
                return 1;
            }
            if (right == null) {
                return -1;
            }
            return left.compareTo(right);
        }
    }

    private int[] sortBySingleColumn(int[] rows, CompiledQuery.CompiledOrderBy orderBy) {
        int columnIndex = orderBy.columnIndex();
        boolean ascending = orderBy.ascending();
        byte typeCode = plan.table().typeCodeAt(columnIndex);

        return switch (typeCode) {
            case TypeCodes.TYPE_INT,
                TypeCodes.TYPE_BOOLEAN,
                TypeCodes.TYPE_BYTE,
                TypeCodes.TYPE_SHORT,
                TypeCodes.TYPE_CHAR -> sortByIntColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_LONG,
                TypeCodes.TYPE_INSTANT,
                TypeCodes.TYPE_LOCAL_DATE,
                TypeCodes.TYPE_LOCAL_DATE_TIME,
                TypeCodes.TYPE_DATE -> sortByLongColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_FLOAT -> sortByFloatColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_DOUBLE -> sortByDoubleColumn(rows, columnIndex, ascending);
            case TypeCodes.TYPE_STRING,
                TypeCodes.TYPE_BIG_DECIMAL,
                TypeCodes.TYPE_BIG_INTEGER -> sortByStringColumn(rows, columnIndex, ascending);
            default -> throw new IllegalArgumentException("Unsupported type for sorting: " + typeCode);
        };
    }

    private int[] sortByIntColumn(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        int[] keys = new int[result.length];
        boolean[] present = new boolean[result.length];
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            int row = result[i];
            present[i] = table.isPresent(columnIndex, row);
            keys[i] = table.readInt(columnIndex, row);
        }
        quickSortInt(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByFloatColumn(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        float[] keys = new float[result.length];
        boolean[] present = new boolean[result.length];
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            int row = result[i];
            present[i] = table.isPresent(columnIndex, row);
            keys[i] = Float.intBitsToFloat(table.readInt(columnIndex, row));
        }
        quickSortFloat(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByLongColumn(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        long[] keys = new long[result.length];
        boolean[] present = new boolean[result.length];
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            int row = result[i];
            present[i] = table.isPresent(columnIndex, row);
            keys[i] = table.readLong(columnIndex, row);
        }
        quickSortLong(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByDoubleColumn(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        double[] keys = new double[result.length];
        boolean[] present = new boolean[result.length];
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            int row = result[i];
            present[i] = table.isPresent(columnIndex, row);
            keys[i] = Double.longBitsToDouble(table.readLong(columnIndex, row));
        }
        quickSortDouble(result, keys, present, 0, result.length - 1, ascending);
        return result;
    }

    private int[] sortByStringColumn(int[] rows, int columnIndex, boolean ascending) {
        int[] result = rows.clone();
        String[] keys = new String[result.length];
        GeneratedTable table = plan.table();
        for (int i = 0; i < result.length; i++) {
            keys[i] = table.readString(columnIndex, result[i]);
        }
        quickSortString(result, keys, 0, result.length - 1, ascending);
        return result;
    }

    private static int compareNullable(boolean presentA, boolean presentB) {
        if (presentA == presentB) {
            return 0;
        }
        return presentA ? -1 : 1; // nulls last
    }

    private static void quickSortInt(int[] rows, int[] keys, boolean[] present, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        int pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareInt(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareInt(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j) quickSortInt(rows, keys, present, low, j, ascending);
        if (i < high) quickSortInt(rows, keys, present, i, high, ascending);
    }

    private static void quickSortFloat(int[] rows, float[] keys, boolean[] present, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        float pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareFloat(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareFloat(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j) quickSortFloat(rows, keys, present, low, j, ascending);
        if (i < high) quickSortFloat(rows, keys, present, i, high, ascending);
    }

    private static void quickSortLong(int[] rows, long[] keys, boolean[] present, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        long pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareLong(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareLong(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j) quickSortLong(rows, keys, present, low, j, ascending);
        if (i < high) quickSortLong(rows, keys, present, i, high, ascending);
    }

    private static void quickSortDouble(int[] rows, double[] keys, boolean[] present, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        double pivot = keys[pivotIndex];
        boolean pivotPresent = present[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareDouble(keys[i], present[i], rows[i], pivot, pivotPresent, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareDouble(keys[j], present[j], rows[j], pivot, pivotPresent, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(rows, i, j);
                swap(keys, i, j);
                swap(present, i, j);
                i++;
                j--;
            }
        }

        if (low < j) quickSortDouble(rows, keys, present, low, j, ascending);
        if (i < high) quickSortDouble(rows, keys, present, i, high, ascending);
    }

    private static void quickSortString(int[] rows, String[] keys, int low, int high, boolean ascending) {
        int i = low;
        int j = high;
        int pivotIndex = low + ((high - low) >>> 1);
        String pivot = keys[pivotIndex];
        int pivotRow = rows[pivotIndex];

        while (i <= j) {
            while (compareString(keys[i], rows[i], pivot, pivotRow, ascending) < 0) {
                i++;
            }
            while (compareString(keys[j], rows[j], pivot, pivotRow, ascending) > 0) {
                j--;
            }
            if (i <= j) {
                swap(keys, i, j, rows);
                i++;
                j--;
            }
        }

        if (low < j) quickSortString(rows, keys, low, j, ascending);
        if (i < high) quickSortString(rows, keys, i, high, ascending);
    }

    private static int compareInt(int value, boolean present, int row,
                                  int pivot, boolean pivotPresent, int pivotRow,
                                  boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Integer.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareFloat(float value, boolean present, int row,
                                    float pivot, boolean pivotPresent, int pivotRow,
                                    boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Float.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareLong(long value, boolean present, int row,
                                   long pivot, boolean pivotPresent, int pivotRow,
                                   boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Long.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareDouble(double value, boolean present, int row,
                                     double pivot, boolean pivotPresent, int pivotRow,
                                     boolean ascending) {
        int cmp = compareNullable(present, pivotPresent);
        if (cmp == 0) {
            cmp = Double.compare(value, pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static int compareString(String value, int row, String pivot, int pivotRow, boolean ascending) {
        int cmp;
        if (value == null && pivot == null) {
            cmp = 0;
        } else if (value == null) {
            cmp = 1;
        } else if (pivot == null) {
            cmp = -1;
        } else {
            cmp = value.compareTo(pivot);
        }
        if (cmp == 0) {
            cmp = Integer.compare(row, pivotRow);
        }
        return ascending ? cmp : -cmp;
    }

    private static void swap(int[] arr, int i, int j) {
        int tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(long[] arr, int i, int j) {
        long tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(double[] arr, int i, int j) {
        double tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(float[] arr, int i, int j) {
        float tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(boolean[] arr, int i, int j) {
        boolean tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static void swap(int[] arr, int i, int j, int[] keys) {
        swap(arr, i, j);
        swap(keys, i, j);
    }

    private static void swap(String[] arr, int i, int j, int[] rows) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
        int row = rows[i];
        rows[i] = rows[j];
        rows[j] = row;
    }

    private static void swap(String[] arr, int i, int j) {
        String tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private Selection applyJoins(CompiledQuery query, Object[] args, Selection baseSelection) {
        CompiledQuery.CompiledJoin[] joins = query.joins();
        if (joins == null || joins.length == 0) {
            return baseSelection;
        }

        Selection current = baseSelection;
        for (CompiledQuery.CompiledJoin join : joins) {
            Selection targetSelection = evaluateJoinPredicates(join, args);
            current = join.executor().filterJoin(plan.table(), join.targetTable(), current, targetSelection);
        }
        return current;
    }

    private Selection evaluateJoinPredicates(CompiledQuery.CompiledJoin join, Object[] args) {
        CompiledQuery.CompiledJoinPredicate[] predicates = join.predicates();
        if (predicates == null || predicates.length == 0) {
            return null;
        }

        Selection selection = null;
        for (CompiledQuery.CompiledJoinPredicate predicate : predicates) {
            CompiledQuery.CompiledCondition compiled = CompiledQuery.CompiledCondition.of(
                predicate.columnIndex(),
                predicate.operator(),
                predicate.argumentIndex(),
                predicate.ignoreCase()
            );
            Selection next = join.targetKernel().executeCondition(compiled, args);
            selection = (selection == null) ? next : selection.intersect(next);
        }

        return selection;
    }

    private void hydrateJoins(T entity, int rowIndex, CompiledQuery query) {
        CompiledQuery.CompiledJoin[] joins = query.joins();
        if (joins == null || joins.length == 0) {
            return;
        }

        for (CompiledQuery.CompiledJoin join : joins) {
            join.materializer().hydrate(entity, rowIndex, plan.table(), join.targetTable(), join.targetKernel(), join.targetMaterializer());
        }
    }

    private void hydrateCollections(T entity, int rowIndex) {
        if (metadata == null) {
            return;
        }
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (!field.isRelationship() || !field.isCollection()) {
                continue;
            }
            if (field.relationshipType() == io.memris.core.EntityMetadata.FieldMapping.RelationshipType.ONE_TO_MANY) {
                hydrateOneToMany(entity, rowIndex, field);
            } else if (field.relationshipType() == io.memris.core.EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY) {
                hydrateManyToMany(entity, rowIndex, field);
            }
        }
    }

    private void hydrateOneToMany(T entity, int rowIndex, io.memris.core.EntityMetadata.FieldMapping field) {
        if (field.targetEntity() == null) {
            return;
        }
        io.memris.storage.GeneratedTable targetTable = plan.tablesByEntity().get(field.targetEntity());
        if (targetTable == null) {
            return;
        }
        io.memris.runtime.HeapRuntimeKernel targetKernel = plan.kernelsByEntity().get(field.targetEntity());
        io.memris.runtime.EntityMaterializer<?> targetMaterializer = plan.materializersByEntity().get(field.targetEntity());
        if (targetKernel == null || targetMaterializer == null) {
            return;
        }

        io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity());
        int fkColumnIndex = targetMetadata.resolveColumnPosition(field.columnName());

        int idColumnIndex = metadata.resolveColumnPosition(metadata.idColumnName());
        long sourceId = readSourceId(plan.table(), idColumnIndex, field.typeCode(), rowIndex);

        int[] matches = switch (field.typeCode()) {
            case io.memris.core.TypeCodes.TYPE_LONG -> targetTable.scanEqualsLong(fkColumnIndex, sourceId);
            case io.memris.core.TypeCodes.TYPE_INT,
                 io.memris.core.TypeCodes.TYPE_SHORT,
                 io.memris.core.TypeCodes.TYPE_BYTE -> targetTable.scanEqualsInt(fkColumnIndex, (int) sourceId);
            default -> targetTable.scanEqualsLong(fkColumnIndex, sourceId);
        };

        java.util.Collection<Object> collection = createCollection(field.javaType(), matches.length);
        for (int targetRow : matches) {
            Object related = targetMaterializer.materialize(targetKernel, targetRow);
            invokeLifecycle(targetMetadata.postLoadHandle(), related);
            collection.add(related);
        }

        MethodHandle setter = metadata.fieldSetters().get(field.name());
        if (setter == null) {
            return;
        }
        try {
            setter.invoke(entity, collection);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set collection field: " + field.name(), e);
        }
    }

    private long readSourceId(io.memris.storage.GeneratedTable table, int idColumnIndex, byte typeCode, int rowIndex) {
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_LONG -> table.readLong(idColumnIndex, rowIndex);
            case io.memris.core.TypeCodes.TYPE_INT,
                 io.memris.core.TypeCodes.TYPE_SHORT,
                 io.memris.core.TypeCodes.TYPE_BYTE -> table.readInt(idColumnIndex, rowIndex);
            default -> table.readLong(idColumnIndex, rowIndex);
        };
    }

    private java.util.Collection<Object> createCollection(Class<?> fieldType, int expectedSize) {
        if (java.util.Set.class.isAssignableFrom(fieldType)) {
            return new java.util.LinkedHashSet<>(Math.max(expectedSize, 16));
        }
        return new java.util.ArrayList<>(Math.max(expectedSize, 10));
    }

    private void hydrateManyToMany(T entity, int rowIndex, io.memris.core.EntityMetadata.FieldMapping field) {
        JoinTableInfo joinInfo = resolveJoinTable(field);
        if (joinInfo == null || joinInfo.table == null) {
            return;
        }

        io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity());
        io.memris.runtime.HeapRuntimeKernel targetKernel = plan.kernelsByEntity().get(field.targetEntity());
        io.memris.runtime.EntityMaterializer<?> targetMaterializer = plan.materializersByEntity().get(field.targetEntity());
        if (targetKernel == null || targetMaterializer == null) {
            return;
        }

        io.memris.core.EntityMetadata.FieldMapping targetId = findIdField(targetMetadata);
        if (targetId == null) {
            return;
        }

        int sourceIdColumnIndex = metadata.resolveColumnPosition(metadata.idColumnName());
        Object sourceIdValue = readSourceIdValue(plan.table(), sourceIdColumnIndex, field.typeCode(), rowIndex);
        if (sourceIdValue == null) {
            return;
        }

        io.memris.storage.SimpleTable joinTable = joinInfo.table;
        io.memris.kernel.Column<?> joinColumn = joinTable.column(joinInfo.joinColumn);
        io.memris.kernel.Column<?> inverseColumn = joinTable.column(joinInfo.inverseJoinColumn);

        long rowCount = joinTable.rowCount();
        java.util.Collection<Object> collection = createCollection(field.javaType(), (int) Math.min(rowCount, Integer.MAX_VALUE));

        for (int i = 0; i < rowCount; i++) {
            io.memris.kernel.RowId rowId = new io.memris.kernel.RowId(i >>> 16, i & 0xFFFF);
            Object joinValue = joinColumn.get(rowId);
            if (joinValue == null || !joinValue.equals(sourceIdValue)) {
                continue;
            }
            Object targetIdValue = inverseColumn.get(rowId);
            if (targetIdValue == null) {
                continue;
            }

            int[] matches = scanTargetById(joinInfo.targetTable, targetId, targetIdValue);
            for (int targetRow : matches) {
                Object related = targetMaterializer.materialize(targetKernel, targetRow);
                invokeLifecycle(targetMetadata.postLoadHandle(), related);
                collection.add(related);
            }
        }

        MethodHandle setter = metadata.fieldSetters().get(field.name());
        if (setter == null) {
            return;
        }
        try {
            setter.invoke(entity, collection);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to set many-to-many collection: " + field.name(), e);
        }
    }

    private int[] scanTargetById(io.memris.storage.GeneratedTable targetTable,
                                 io.memris.core.EntityMetadata.FieldMapping targetId,
                                 Object targetIdValue) {
        return switch (targetId.typeCode()) {
            case io.memris.core.TypeCodes.TYPE_LONG -> targetTable.scanEqualsLong(
                targetId.columnPosition(), ((Number) targetIdValue).longValue());
            case io.memris.core.TypeCodes.TYPE_INT,
                 io.memris.core.TypeCodes.TYPE_SHORT,
                 io.memris.core.TypeCodes.TYPE_BYTE -> targetTable.scanEqualsInt(
                targetId.columnPosition(), ((Number) targetIdValue).intValue());
            case io.memris.core.TypeCodes.TYPE_STRING -> targetTable.scanEqualsString(
                targetId.columnPosition(), targetIdValue.toString());
            default -> targetTable.scanEqualsLong(targetId.columnPosition(), ((Number) targetIdValue).longValue());
        };
    }

    private Object readSourceIdValue(io.memris.storage.GeneratedTable table, int idColumnIndex, byte typeCode, int rowIndex) {
        return switch (typeCode) {
            case io.memris.core.TypeCodes.TYPE_LONG -> Long.valueOf(table.readLong(idColumnIndex, rowIndex));
            case io.memris.core.TypeCodes.TYPE_INT -> Integer.valueOf(table.readInt(idColumnIndex, rowIndex));
            case io.memris.core.TypeCodes.TYPE_SHORT -> Short.valueOf((short) table.readInt(idColumnIndex, rowIndex));
            case io.memris.core.TypeCodes.TYPE_BYTE -> Byte.valueOf((byte) table.readInt(idColumnIndex, rowIndex));
            default -> Long.valueOf(table.readLong(idColumnIndex, rowIndex));
        };
    }

    private void persistManyToMany(T entity) {
        if (metadata == null) {
            return;
        }
        for (io.memris.core.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (!field.isRelationship() || field.relationshipType() != io.memris.core.EntityMetadata.FieldMapping.RelationshipType.MANY_TO_MANY) {
                continue;
            }
            if (field.joinTable() == null || field.joinTable().isBlank()) {
                continue;
            }
            JoinTableInfo joinInfo = resolveJoinTable(field);
            if (joinInfo == null || joinInfo.table == null) {
                continue;
            }
            Object collectionValue = readFieldValue(entity, metadata, field.name());
            if (!(collectionValue instanceof Iterable<?> iterable)) {
                continue;
            }

            io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity());
            io.memris.core.EntityMetadata.FieldMapping targetId = findIdField(targetMetadata);
            if (targetId == null) {
                continue;
            }

            Object sourceIdValue = readFieldValue(entity, metadata, metadata.idColumnName());
            if (sourceIdValue == null) {
                continue;
            }

            java.util.Set<JoinKey> existing = loadJoinKeys(joinInfo);

            for (Object related : iterable) {
                if (related == null) {
                    continue;
                }
                Object targetIdValue = readFieldValue(related, targetMetadata, targetMetadata.idColumnName());
                if (targetIdValue == null) {
                    continue;
                }
                JoinKey key = new JoinKey(sourceIdValue, targetIdValue);
                if (existing.add(key)) {
                    joinInfo.table.insert(sourceIdValue, targetIdValue);
                }
            }
        }
    }

    private java.util.Set<JoinKey> loadJoinKeys(JoinTableInfo joinInfo) {
        java.util.Set<JoinKey> keys = new java.util.HashSet<>();
        io.memris.storage.SimpleTable table = joinInfo.table;
        io.memris.kernel.Column<?> joinColumn = table.column(joinInfo.joinColumn);
        io.memris.kernel.Column<?> inverseColumn = table.column(joinInfo.inverseJoinColumn);
        if (joinColumn == null || inverseColumn == null) {
            return keys;
        }
        long rowCount = table.rowCount();
        for (int i = 0; i < rowCount; i++) {
            io.memris.kernel.RowId rowId = new io.memris.kernel.RowId(i >>> 16, i & 0xFFFF);
            Object joinValue = joinColumn.get(rowId);
            Object inverseValue = inverseColumn.get(rowId);
            if (joinValue == null || inverseValue == null) {
                continue;
            }
            keys.add(new JoinKey(joinValue, inverseValue));
        }
        return keys;
    }

    private Object readFieldValue(Object entity, io.memris.core.EntityMetadata<?> entityMetadata, String fieldName) {
        if (entity == null || entityMetadata == null) {
            return null;
        }
        MethodHandle getter = entityMetadata.fieldGetters().get(fieldName);
        if (getter != null) {
            try {
                return getter.invoke(entity);
            } catch (Throwable ignored) {
                return null;
            }
        }
        try {
            java.lang.reflect.Field field = entity.getClass().getDeclaredField(fieldName);
            if (!field.canAccess(entity)) {
                field.setAccessible(true);
            }
            return field.get(entity);
        } catch (ReflectiveOperationException ignored) {
            return null;
        }
    }

    private io.memris.core.EntityMetadata.FieldMapping findIdField(io.memris.core.EntityMetadata<?> targetMetadata) {
        for (io.memris.core.EntityMetadata.FieldMapping field : targetMetadata.fields()) {
            if (field.name().equals(targetMetadata.idColumnName())) {
                return field;
            }
        }
        return null;
    }

    private JoinTableInfo resolveJoinTable(io.memris.core.EntityMetadata.FieldMapping field) {
        if (field.joinTable() != null && !field.joinTable().isBlank()) {
            return buildJoinTableInfo(field, metadata, io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity()), false);
        }
        if (field.mappedBy() != null && !field.mappedBy().isBlank()) {
            io.memris.core.EntityMetadata<?> targetMetadata = io.memris.core.MetadataExtractor.extractEntityMetadata(field.targetEntity());
            io.memris.core.EntityMetadata.FieldMapping ownerField = findFieldByName(targetMetadata, field.mappedBy());
            if (ownerField == null || ownerField.joinTable() == null || ownerField.joinTable().isBlank()) {
                return null;
            }
            return buildJoinTableInfo(ownerField, targetMetadata, metadata, true);
        }
        return null;
    }

    private JoinTableInfo buildJoinTableInfo(io.memris.core.EntityMetadata.FieldMapping ownerField,
                                             io.memris.core.EntityMetadata<?> ownerMetadata,
                                             io.memris.core.EntityMetadata<?> inverseMetadata,
                                             boolean inverseSide) {
        String joinTableName = ownerField.joinTable();
        io.memris.storage.SimpleTable joinTable = plan.joinTables().get(joinTableName);
        if (joinTable == null) {
            return null;
        }

        String joinColumn;
        String inverseJoinColumn;
        if (!inverseSide) {
            joinColumn = ownerField.columnName();
            if (joinColumn == null || joinColumn.isBlank()) {
                joinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase() + "_" + ownerMetadata.idColumnName();
            }
            inverseJoinColumn = ownerField.referencedColumnName();
            if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
                inverseJoinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase() + "_" + inverseMetadata.idColumnName();
            }
            return new JoinTableInfo(joinTableName, joinColumn, inverseJoinColumn, joinTable, plan.tablesByEntity().get(ownerField.targetEntity()));
        }

        joinColumn = ownerField.referencedColumnName();
        if (joinColumn == null || joinColumn.isBlank()) {
            joinColumn = inverseMetadata.entityClass().getSimpleName().toLowerCase() + "_" + inverseMetadata.idColumnName();
        }
        inverseJoinColumn = ownerField.columnName();
        if (inverseJoinColumn == null || inverseJoinColumn.isBlank()) {
            inverseJoinColumn = ownerMetadata.entityClass().getSimpleName().toLowerCase() + "_" + ownerMetadata.idColumnName();
        }
        return new JoinTableInfo(joinTableName, joinColumn, inverseJoinColumn, joinTable, plan.tablesByEntity().get(ownerMetadata.entityClass()));
    }

    private io.memris.core.EntityMetadata.FieldMapping findFieldByName(io.memris.core.EntityMetadata<?> targetMetadata, String name) {
        for (io.memris.core.EntityMetadata.FieldMapping field : targetMetadata.fields()) {
            if (field.name().equals(name)) {
                return field;
            }
        }
        return null;
    }

    private record JoinTableInfo(
        String name,
        String joinColumn,
        String inverseJoinColumn,
        io.memris.storage.SimpleTable table,
        io.memris.storage.GeneratedTable targetTable
    ) {
    }

    private record JoinKey(Object left, Object right) {
    }

    public RepositoryPlan<T> plan() {
        return plan;
    }
}
