package io.memris.spring.runtime;

import io.memris.spring.EntityMetadata;
import io.memris.spring.MemrisRepositoryFactory;
import io.memris.spring.TypeCodes;
import io.memris.kernel.Predicate;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.LogicalQuery;
import io.memris.spring.plan.OpCode;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.SelectionImpl;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
    private static long idCounter = 1L;

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
    }

    private static java.util.Map<Class<?>, EntityMetadata<?>> buildRelatedMetadata(EntityMetadata<?> metadata) {
        if (metadata == null) {
            return java.util.Map.of();
        }
        java.util.Map<Class<?>, EntityMetadata<?>> related = new java.util.HashMap<>();
        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.isRelationship() && field.targetEntity() != null) {
                related.putIfAbsent(field.targetEntity(), io.memris.spring.MetadataExtractor.extractEntityMetadata(field.targetEntity()));
            }
        }
        return java.util.Map.copyOf(related);
    }

    /**
     * Execute a method call based on the compiled query plan.
     */
    public Object executeMethod(Method method, Object[] args) {
        int methodIndex = findMethodIndex(method);
        if (methodIndex < 0) {
            throw new UnsupportedOperationException("Method not found in plan: " + method.getName());
        }

        CompiledQuery query = plan.queries()[methodIndex];
        return executeCompiledQuery(query, args);
    }

    private int findMethodIndex(Method method) {
        java.lang.reflect.Method[] methods = io.memris.spring.scaffold.RepositoryMethodIntrospector.extractQueryMethods(
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
                return executeFind(query, args);
            case COUNT:
                return executeCount(query, args);
            case COUNT_ALL:
                return executeCountAll();
            case EXISTS:
                return executeExists(query, args);
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
                return executeDeleteQuery(query, args);
            case DELETE_ALL_BY_ID:
                return executeDeleteAllById(args);
            default:
                throw new UnsupportedOperationException("OpCode not implemented: " + query.opCode());
        }
    }

    private T executeSaveOne(Object[] args) {
        T entity = (T) args[0];
        GeneratedTable table = plan.table();

        try {
            String idFieldName = metadata.idColumnName();
            MethodHandle idGetter = metadata.fieldGetters().get(idFieldName);
            MethodHandle idSetter = metadata.fieldSetters().get(idFieldName);

            Object currentId = idGetter != null ? idGetter.invoke(entity) : null;
            boolean isNew = (currentId == null) || isZeroId(currentId);

            Object id = currentId;
            if (isNew && idSetter != null) {
                id = generateIdForEntity(entity);
                idSetter.invoke(entity, id);
            }

            int maxColumnPos = metadata.fields().stream()
                .filter(f -> f.columnPosition() >= 0)
                .mapToInt(io.memris.spring.EntityMetadata.FieldMapping::columnPosition)
                .max()
                .orElse(0);
            Object[] values = new Object[maxColumnPos + 1];
            Object[] indexValues = new Object[maxColumnPos + 1];

            // First pass: populate all values
            for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
                if (field.columnPosition() < 0) {
                    continue; // Skip collection fields
                }

                MethodHandle getter = metadata.fieldGetters().get(field.name());
                if (getter == null) {
                    continue;
                }

                Object value = getter.invoke(entity);

                if (field.isRelationship()) {
                    value = resolveRelationshipId(field, value);
                    values[field.columnPosition()] = value;
                    indexValues[field.columnPosition()] = value;
                    continue;
                }

                Object rawValue = value;

                // Apply converter if present
                TypeConverter<?, ?> converter = metadata.converters().get(field.name());
                if (converter != null && value != null) {
                    @SuppressWarnings("unchecked")
                    TypeConverter<Object, Object> typedConverter = (TypeConverter<Object, Object>) converter;
                    value = typedConverter.toStorage(value);
                }

                values[field.columnPosition()] = value;
                indexValues[field.columnPosition()] = rawValue;
            }

            // Ensure ID column (position 0) is a Long
            Object idValue = values[0];
            if (idValue == null) {
                throw new IllegalStateException("ID value is null after generation");
            }
            if (idValue instanceof Number) {
                values[0] = ((Number) idValue).longValue();
            } else {
                throw new IllegalStateException("ID value is not a Number: " + idValue.getClass());
            }

            long packedRef = table.insertFrom(values);
            int rowIndex = io.memris.storage.Selection.index(packedRef);
            updateIndexesOnInsert(indexValues, rowIndex);
            return entity;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to save entity", e);
        }
    }

    private Object resolveRelationshipId(io.memris.spring.EntityMetadata.FieldMapping field, Object relatedEntity) throws Throwable {
        if (relatedEntity == null) {
            return null;
        }
        EntityMetadata<?> targetMetadata = relatedMetadata.get(field.targetEntity());
        if (targetMetadata == null) {
            throw new IllegalStateException("No metadata for related entity: " + field.targetEntity());
        }

        String idFieldName = targetMetadata.idColumnName();
        MethodHandle idGetter = targetMetadata.fieldGetters().get(idFieldName);
        if (idGetter == null) {
            throw new IllegalStateException("No ID getter for related entity: " + field.targetEntity().getName());
        }

        Object idValue = idGetter.invoke(relatedEntity);
        if (idValue == null) {
            return null;
        }

        Class<?> storageType = field.storageType();
        if (storageType == Long.class || storageType == long.class) {
            return ((Number) idValue).longValue();
        }
        if (storageType == Integer.class || storageType == int.class) {
            return ((Number) idValue).intValue();
        }
        if (storageType == Short.class || storageType == short.class) {
            return ((Number) idValue).shortValue();
        }
        if (storageType == Byte.class || storageType == byte.class) {
            return ((Number) idValue).byteValue();
        }
        return idValue;
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

    private Object generateIdForEntity(T entity) {
        long nextId = idCounter++;

        String idFieldName = metadata.idColumnName();
        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.name().equals(idFieldName)) {
                Class<?> idType = field.javaType();

                return switch (idType) {
                    case Class<?> c when c == Long.class || c == long.class -> Long.valueOf(nextId);
                    case Class<?> c when c == Integer.class || c == int.class -> (int) (int) nextId;
                    case Class<?> c when c == Short.class || c == short.class -> (short) (short) nextId;
                    case Class<?> c when c == Byte.class || c == byte.class -> (byte) (byte) nextId;
                    case Class<?> c when c == java.util.UUID.class -> java.util.UUID.randomUUID();
                    default -> throw new IllegalArgumentException("Unsupported ID type: " + idType);
                };
            }
        }
        throw new IllegalStateException("ID field not found in metadata");
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
        return Optional.of(entity);
    }

    private List<T> executeFindAll() {
        GeneratedTable table = plan.table();
        int[] rowIndices = table.scanAll();

        List<T> results = new ArrayList<>(rowIndices.length);
        for (int rowIndex : rowIndices) {
            results.add(materializer.materialize(plan.kernel(), rowIndex));
        }
        return results;
    }

    private Object executeFind(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);

        int[] rows = selection.toIntArray();
        rows = applyOrderBy(query, rows);

        int limit = query.limit();
        int max = (limit > 0 && limit < rows.length) ? limit : rows.length;

        List<T> results = new ArrayList<>(max);
        for (int i = 0; i < max; i++) {
            int rowIndex = rows[i];
            T entity = materializer.materialize(plan.kernel(), rowIndex);
            hydrateJoins(entity, rowIndex, query);
            results.add(entity);
        }

        return switch (query.returnKind()) {
            case ONE_OPTIONAL -> results.isEmpty() ? Optional.empty() : Optional.of(results.get(0));
            case MANY_LIST -> results;
            default -> throw new IllegalStateException("Unexpected return kind for FIND: " + query.returnKind());
        };
    }

    private long executeCount(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        return selection.size();
    }

    private long executeCountAll() {
        return plan.table().liveCount();
    }

    private boolean executeExists(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        return selection.size() > 0;
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

    private void updateIndexesOnInsert(Object[] indexValues, int rowIndex) {
        if (metadata == null || factory == null) {
            return;
        }
        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
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
        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() < 0) {
                continue;
            }
            Object value = readIndexValue(field, rowIndex);
            factory.removeIndexEntry(metadata.entityClass(), field.name(), value, rowIndex);
        }
    }

    private Object readIndexValue(io.memris.spring.EntityMetadata.FieldMapping field, int rowIndex) {
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
        Object value = args[condition.argumentIndex()];

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
        if (metadata == null) {
            return null;
        }
        for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
            if (field.columnPosition() == columnIndex) {
                return field.name();
            }
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
        CompiledQuery.CompiledOrderBy orderBy = query.orderBy();
        if (orderBy == null || rows.length < 2) {
            return rows;
        }

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

    public RepositoryPlan<T> plan() {
        return plan;
    }
}
