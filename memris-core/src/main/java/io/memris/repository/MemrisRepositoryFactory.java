package io.memris.repository;

import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.core.TypeCodes;
import io.memris.index.HashIndex;
import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.index.RangeIndex;
import io.memris.index.StringPrefixIndex;
import io.memris.index.StringSuffixIndex;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.kernel.RowIdSet;
import io.memris.runtime.codegen.RuntimeExecutorGenerator;
import io.memris.storage.GeneratedTable;
import io.memris.storage.Selection;
import io.memris.storage.heap.AbstractTable;
import io.memris.storage.heap.FieldMetadata;
import io.memris.storage.heap.TableGenerator;
import io.memris.storage.heap.TableMetadata;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;



public final class MemrisRepositoryFactory implements AutoCloseable {

    private final Map<Class<?>, GeneratedTable> tables = new HashMap<>();
    private final Map<Class<?>, Map<String, Object>> indexes = new HashMap<>();

    // Configuration settings
    private final MemrisConfiguration configuration;

    // Default arena for direct factory calls (always uses arena pattern) - lazily initialized using CAS
    private final AtomicReference<MemrisArena> defaultArenaRef = new AtomicReference<>();

    /**
     * Creates a factory with default configuration.
     */
    public MemrisRepositoryFactory() {
        this(MemrisConfiguration.builder().build());
    }

    /**
     * Creates a factory with the specified configuration.
     *
     * @param configuration the configuration to use
     */
    public MemrisRepositoryFactory(MemrisConfiguration configuration) {
        this.configuration = configuration;
        RuntimeExecutorGenerator.setConfiguration(configuration);
    }

    /**
     * Get the configuration for this factory.
     *
     * @return the configuration
     */
    public MemrisConfiguration getConfiguration() {
        return configuration;
    }

    /**
     * Extract entity class T from repository interface extending
     * MemrisRepository<T>
     */
    @SuppressWarnings("unchecked")
    private <T> Class<T> extractEntityClass(Class<? extends MemrisRepository<T>> repositoryInterface) {
        // Get the first generic interface that extends MemrisRepository
        for (var iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof ParameterizedType pt) {
                var rawType = pt.getRawType();
                if (rawType instanceof Class<?> clazz && MemrisRepository.class.isAssignableFrom(clazz)) {
                    Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                        return (Class<T>) typeArgs[0];
                    }
                }
            }
        }
        throw new IllegalArgumentException("Cannot extract entity class from " + repositoryInterface.getName() +
                ". Repository must extend MemrisRepository<EntityType>.");
    }

    GeneratedTable getTable(Class<?> entityClass) {
        return tables.get(entityClass);
    }

    /**
     * Get index for a specific field, or null if no index exists.
     */
    Object getIndex(Class<?> entityClass, String fieldName) {
        var entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null) {
            return null;
        }
        return entityIndexes.get(fieldName);
    }

    /**
     * Check if an index exists for a field.
     */
    boolean hasIndex(Class<?> entityClass, String fieldName) {
        var entityIndexes = indexes.get(entityClass);
        return entityIndexes != null && entityIndexes.containsKey(fieldName);
    }

    /**
     * Query using index - returns row IDs for matching entities.
     * 
     * @param entityClass The entity class
     * @param fieldName   The indexed field name
     * @param operator    The comparison operator
     * @param value       The value to match
     * @return Array of matching row indices, or null if no index available
     */
    public int[] queryIndex(Class<?> entityClass, String fieldName, Predicate.Operator operator, Object value) {
        var entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null) {
            return null;
        }

        Object index = entityIndexes.get(fieldName);
        if (index == null) {
            return null;
        }

        GeneratedTable table = tables.get(entityClass);
        if (table == null) {
            return null;
        }
        java.util.function.Predicate<RowId> validator = rowId -> {
            int rowIndex = (int) rowId.value();
            long generation = table.rowGeneration(rowIndex);
            long ref = Selection.pack(rowIndex, generation);
            return table.isLive(ref);
        };

        // Java 21+ pattern matching with switch on sealed types
        return switch (index) {
            case HashIndex hashIndex when operator == Predicate.Operator.EQ && value != null ->
                rowIdSetToIntArray(hashIndex.lookup(value, validator));
            case RangeIndex rangeIndex when value instanceof Comparable comp -> switch (operator) {
                case EQ -> rowIdSetToIntArray(rangeIndex.lookup(comp, validator));
                case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(comp, validator));
                case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(comp, validator));
                case LT -> rowIdSetToIntArray(rangeIndex.lessThan(comp, validator));
                case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(comp, validator));
                default -> null;
            };
            case RangeIndex rangeIndex when operator == Predicate.Operator.BETWEEN
                    && value instanceof Object[] range -> {
                if (range.length < 2) {
                    yield null;
                }
                Object lower = range[0];
                Object upper = range[1];
                if (lower instanceof Comparable lowerComp && upper instanceof Comparable upperComp) {
                    yield rowIdSetToIntArray(rangeIndex.between(lowerComp, upperComp, validator));
                }
                yield null;
            }
            case StringPrefixIndex prefixIndex when value instanceof String s -> {
                if (operator == Predicate.Operator.STARTING_WITH) {
                    yield rowIdSetToIntArray(prefixIndex.startsWith(s));
                } else if (operator == Predicate.Operator.EQ) {
                    // Prefix index can also handle exact matches
                    yield rowIdSetToIntArray(prefixIndex.startsWith(s));
                }
                yield null;
            }
            case StringSuffixIndex suffixIndex when value instanceof String s -> {
                if (operator == Predicate.Operator.ENDING_WITH) {
                    yield rowIdSetToIntArray(suffixIndex.endsWith(s));
                }
                yield null;
            }
            case CompositeHashIndex hashIndex when operator == Predicate.Operator.EQ && value instanceof CompositeKey key ->
                rowIdSetToIntArray(hashIndex.lookup(key, validator));
            case CompositeRangeIndex rangeIndex when value instanceof CompositeKey key -> switch (operator) {
                case EQ -> rowIdSetToIntArray(rangeIndex.lookup(key, validator));
                case GT -> rowIdSetToIntArray(rangeIndex.greaterThan(key, validator));
                case GTE -> rowIdSetToIntArray(rangeIndex.greaterThanOrEqual(key, validator));
                case LT -> rowIdSetToIntArray(rangeIndex.lessThan(key, validator));
                case LTE -> rowIdSetToIntArray(rangeIndex.lessThanOrEqual(key, validator));
                default -> null;
            };
            case CompositeRangeIndex rangeIndex when operator == Predicate.Operator.BETWEEN
                    && value instanceof Object[] range
                    && range.length >= 2
                    && range[0] instanceof CompositeKey lower
                    && range[1] instanceof CompositeKey upper ->
                rowIdSetToIntArray(rangeIndex.between(lower, upper, validator));
            default -> null;
        };
    }

    public void addIndexEntry(Class<?> entityClass, String fieldName, Object value, int rowIndex) {
        Object index = getIndex(entityClass, fieldName);
        if (index == null || value == null) {
            return;
        }
        RowId rowId = RowId.fromLong(rowIndex);
        switch (index) {
            case HashIndex hashIndex -> hashIndex.add(value, rowId);
            case RangeIndex rangeIndex -> {
                if (value instanceof Comparable comp) {
                    rangeIndex.add(comp, rowId);
                }
            }
            case StringPrefixIndex prefixIndex -> {
                if (value instanceof String s) {
                    prefixIndex.add(s, rowId);
                }
            }
            case StringSuffixIndex suffixIndex -> {
                if (value instanceof String s) {
                    suffixIndex.add(s, rowId);
                }
            }
            case CompositeHashIndex hashIndex -> {
                if (value instanceof CompositeKey key) {
                    hashIndex.add(key, rowId);
                }
            }
            case CompositeRangeIndex rangeIndex -> {
                if (value instanceof CompositeKey key) {
                    rangeIndex.add(key, rowId);
                }
            }
            default -> {
            }
        }
    }

    public void removeIndexEntry(Class<?> entityClass, String fieldName, Object value, int rowIndex) {
        var index = getIndex(entityClass, fieldName);
        if (index == null || value == null) {
            return;
        }
        var rowId = RowId.fromLong(rowIndex);
        switch (index) {
            case HashIndex hashIndex -> hashIndex.remove(value, rowId);
            case RangeIndex rangeIndex -> {
                if (value instanceof Comparable comp) {
                    rangeIndex.remove(comp, rowId);
                }
            }
            case StringPrefixIndex prefixIndex -> {
                if (value instanceof String s) {
                    prefixIndex.remove(s, rowId);
                }
            }
            case StringSuffixIndex suffixIndex -> {
                if (value instanceof String s) {
                    suffixIndex.remove(s, rowId);
                }
            }
            case CompositeHashIndex hashIndex -> {
                if (value instanceof CompositeKey key) {
                    hashIndex.remove(key, rowId);
                }
            }
            case CompositeRangeIndex rangeIndex -> {
                if (value instanceof CompositeKey key) {
                    rangeIndex.remove(key, rowId);
                }
            }
            default -> {
            }
        }
    }

    public void clearIndexes(Class<?> entityClass) {
        var entityIndexes = indexes.get(entityClass);
        if (entityIndexes == null) {
            return;
        }
        for (Object index : entityIndexes.values()) {
            switch (index) {
                case HashIndex hashIndex -> hashIndex.clear();
                case RangeIndex rangeIndex -> rangeIndex.clear();
                case StringPrefixIndex prefixIndex -> prefixIndex.clear();
                case StringSuffixIndex suffixIndex -> suffixIndex.clear();
                case CompositeHashIndex hashIndex -> hashIndex.clear();
                case CompositeRangeIndex rangeIndex -> rangeIndex.clear();
                default -> {
                }
            }
        }
    }

    private static int[] rowIdSetToIntArray(RowIdSet rowIdSet) {
        long[] longArray = rowIdSet.toLongArray();
        int[] intArray = new int[longArray.length];
        for (int i = 0; i < longArray.length; i++) {
            intArray[i] = (int) longArray[i]; // RowId value IS the row index
        }
        return intArray;
    }

    @Override
    public void close() {
        // TODO: Clean up resources
    }

    /**
     * Create a JPA repository from a repository interface.
     * <p>
     * This is the main entry point for creating type-safe repositories.
     * Always uses an arena - creates a default arena if none exists.
     *
     * @param repositoryInterface the repository interface (must extend
     *                            MemrisRepository<T>)
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return an instantiated repository implementation
     */
    public <T, R extends MemrisRepository<T>> R createJPARepository(Class<R> repositoryInterface) {
        // Always use arena - create default if needed (lock-free)
        var arena = defaultArenaRef.updateAndGet(existing -> existing != null ? existing : createArena());
        return arena.createRepository(repositoryInterface);
    }

    /**
     * Build a table for an entity class.
     * If arena is provided, the table is cached in the arena.
     */
    public <T> GeneratedTable buildTableForEntity(Class<T> entityClass, MemrisArena arena) {
        // Extract metadata to get field information
        EntityMetadata<T> metadata = configuration.entityMetadataProvider().getMetadata(entityClass);

        // Build TableMetadata for TableGenerator
        List<FieldMetadata> fields = new ArrayList<>();

        for (FieldMapping fm : metadata.fields()) {
            if (fm.columnPosition() < 0) {
                continue; // Skip collection fields (no column)
            }

            Byte tc = fm.typeCode();
            byte typeCode = (tc != null) ? tc.byteValue() : TypeCodes.TYPE_LONG;

            // Check if this is the ID field
            boolean isId = fm.name().equals(metadata.idColumnName());

            fields.add(new FieldMetadata(
                    fm.columnName(),
                    typeCode,
                    isId,
                    isId,
                    fm.javaType().isPrimitive()));

        }

        String entityName = entityClass.getSimpleName();
        TableMetadata tableMetadata = new TableMetadata(
                entityName,
                entityClass.getCanonicalName(),
                fields);

        // Generate the table class using ByteBuddy
        Class<? extends AbstractTable> tableClass;
        try {
            tableClass = TableGenerator.generate(tableMetadata, configuration);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate table for entity: " + entityClass.getName(), e);
        }

        // Instantiate the generated table
        try {
            int pageSize = configuration.pageSize();
            int maxPages = configuration.maxPages();
            int initialPages = configuration.initialPages();
            AbstractTable table = tableClass
                    .getDeclaredConstructor(int.class, int.class, int.class)
                    .newInstance(pageSize, maxPages, initialPages);
            return (GeneratedTable) table;
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate table for entity: " + entityClass.getName(), e);
        }
    }

    // ========== Arena Support Methods ==========

    private final AtomicLong arenaCounter = new AtomicLong(0);
    private final Map<Long, MemrisArena> arenas = new HashMap<>();

    /**
     * Create a new isolated arena.
     * <p>
     * Each arena has its own tables, repositories, and indexes.
     * Data is completely isolated between arenas.
     *
     * @return the new arena
     */
    public MemrisArena createArena() {
        long arenaId = arenaCounter.incrementAndGet();
        MemrisArena arena = new MemrisArena(arenaId, this);
        arenas.put(arenaId, arena);
        return arena;
    }

    /**
     * Create a repository in the default (non-arena) scope.
     * <p>
     * This is a convenience method for backward compatibility.
     *
     * @param repositoryInterface the repository interface
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return the repository instance
     */
    public <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface) {
        return createJPARepository(repositoryInterface);
    }
}
