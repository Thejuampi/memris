package io.memris.spring.runtime;

import io.memris.spring.EntityMetadata;
import io.memris.spring.MemrisRepositoryFactory;
import io.memris.spring.TypeCodes;
import io.memris.spring.converter.TypeConverter;
import io.memris.spring.plan.CompiledQuery;
import io.memris.spring.plan.LogicalQuery;
import io.memris.spring.plan.OpCode;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;

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
        this.materializer = metadata != null ? new EntityMaterializerImpl<>(metadata) : null;
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

            for (io.memris.spring.EntityMetadata.FieldMapping field : metadata.fields()) {
                if (field.columnPosition() < 0) {
                    continue;
                }

                MethodHandle getter = metadata.fieldGetters().get(field.name());
                if (getter == null) {
                    continue;
                }

                Object value;
                if (field.name().equals(idFieldName) && isNew) {
                    value = id;
                } else {
                    value = getter.invoke(entity);
                }

                TypeConverter<?, ?> converter = metadata.converters().get(field.name());
                if (converter != null && value != null) {
                    @SuppressWarnings("unchecked")
                    TypeConverter<Object, Object> typedConverter = (TypeConverter<Object, Object>) converter;
                    value = typedConverter.toStorage(value);
                }

                values[field.columnPosition()] = value;
            }

            Object idValue = values[0];
            if (idValue == null && id instanceof Number) {
                values[0] = ((Number) id).longValue();
            }

            table.insertFrom(values);
            return entity;
        } catch (Throwable e) {
            throw new RuntimeException("Failed to save entity", e);
        }
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

    private List<T> executeFind(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);

        List<T> results = new ArrayList<>();
        long[] refs = selection.toRefArray();
        for (long ref : refs) {
            int rowIndex = io.memris.storage.Selection.index(ref);
            results.add(materializer.materialize(plan.kernel(), rowIndex));
        }

        return switch (query.returnKind()) {
            case ONE_OPTIONAL -> results.isEmpty() ? List.of() : List.of(results.get(0));
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
        throw new UnsupportedOperationException("DELETE_ONE not yet implemented");
    }

    private void executeDeleteAll() {
        GeneratedTable table = plan.table();
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
            table.tombstone(packedRef);
        }
    }

    private long executeDeleteQuery(CompiledQuery query, Object[] args) {
        Selection selection = executeConditions(query, args);
        GeneratedTable table = plan.table();

        long count = 0;
        long[] refs = selection.toRefArray();
        for (long packedRef : refs) {
            table.tombstone(packedRef);
            count++;
        }
        return count;
    }

    private Selection executeConditions(CompiledQuery query, Object[] args) {
        CompiledQuery.CompiledCondition[] conditions = query.conditions();

        if (conditions.length == 0) {
            int[] allRows = plan.table().scanAll();
            long[] packed = new long[allRows.length];
            for (int i = 0; i < allRows.length; i++) {
                packed[i] = io.memris.storage.Selection.pack(allRows[i], plan.table().currentGeneration());
            }
            return new io.memris.storage.SelectionImpl(packed);
        }

        Selection result = plan.kernel().executeCondition(conditions[0], args);

        for (int i = 1; i < conditions.length; i++) {
            Selection next = plan.kernel().executeCondition(conditions[i], args);
            result = result.intersect(next);
        }

        return result;
    }

    public RepositoryPlan<T> plan() {
        return plan;
    }
}
