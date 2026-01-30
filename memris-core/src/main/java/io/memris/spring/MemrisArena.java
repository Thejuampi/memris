package io.memris.spring;

import io.memris.spring.scaffold.RepositoryEmitter;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An isolated data space containing its own tables, repositories, and indexes.
 * <p>
 * Multiple arenas can coexist in the same factory, enabling:
 * <ul>
 *   <li>Multi-tenant applications (one arena per tenant)</li>
 *   <li>Test isolation (fresh arena per test)</li>
 *   <li>Parallel processing (different arenas in different threads)</li>
 * </ul>
 * <p>
 * Each arena is completely isolated - data saved in one arena is not visible
 * in another arena, even for the same entity type.
 *
 * @see MemrisRepositoryFactory
 */
public final class MemrisArena implements AutoCloseable {

    private final long arenaId;
    private final MemrisRepositoryFactory factory;
    
    // Arena-scoped state
    private final Map<Class<?>, io.memris.storage.GeneratedTable> tables = new HashMap<>();
    private final Map<Class<?>, Map<String, Object>> indexes = new HashMap<>();
    private final Map<Class<?>, AtomicLong> numericIdCounters = new HashMap<>();
    private final Map<Class<?>, Object> repositories = new HashMap<>();
    
    // Mapping from entity class to repository interface class
    private final Map<Class<?>, Class<?>> entityToRepositoryMap = new HashMap<>();

    MemrisArena(long arenaId, MemrisRepositoryFactory factory) {
        this.arenaId = arenaId;
        this.factory = factory;
    }

    /**
     * Get the unique ID of this arena.
     */
    public long getArenaId() {
        return arenaId;
    }

    /**
     * Create or get a repository for the given interface in this arena.
     * <p>
     * If the repository already exists in this arena, returns the cached instance.
     *
     * @param repositoryInterface the repository interface
     * @param <T> the entity type
     * @param <R> the repository interface type
     * @return the repository instance
     */
    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface) {
        // Check cache first
        if (repositories.containsKey(repositoryInterface)) {
            return (R) repositories.get(repositoryInterface);
        }

        // Create repository through factory's emitter, but scoped to this arena
        R repository = (R) RepositoryEmitter.createRepository(repositoryInterface, this);
        repositories.put(repositoryInterface, repository);

        // Also map entity class to repository interface for lookup by entity class
        Class<T> entityClass = extractEntityClass(repositoryInterface);
        entityToRepositoryMap.put(entityClass, repositoryInterface);

        return repository;
    }

    /**
     * Extract entity class from repository interface.
     */
    @SuppressWarnings("unchecked")
    private <T> Class<T> extractEntityClass(Class<? extends MemrisRepository<T>> repositoryInterface) {
        for (java.lang.reflect.Type iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof java.lang.reflect.ParameterizedType pt) {
                java.lang.reflect.Type rawType = pt.getRawType();
                if (rawType instanceof Class<?> clazz && MemrisRepository.class.isAssignableFrom(clazz)) {
                    java.lang.reflect.Type[] typeArgs = pt.getActualTypeArguments();
                    if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                        return (Class<T>) typeArgs[0];
                    }
                }
            }
        }
        throw new IllegalArgumentException("Cannot extract entity class from " + repositoryInterface.getName());
    }

    /**
     * Get an existing repository for the given entity class in this arena.
     * <p>
     * Returns null if no repository has been created for this entity type.
     *
     * @param entityClass the entity class
     * @param <T> the entity type
     * @return the repository instance, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> MemrisRepository<T> getRepository(Class<T> entityClass) {
        // Look up the repository interface for this entity class
        Class<?> repositoryInterface = entityToRepositoryMap.get(entityClass);
        if (repositoryInterface == null) {
            return null;
        }
        // Get the repository instance using the interface class
        return (MemrisRepository<T>) repositories.get(repositoryInterface);
    }

    /**
     * Get or create a table for the given entity class in this arena.
     * <p>
     * Uses the factory's table builder but caches the result per-arena.
     *
     * @param entityClass the entity class
     * @param <T> the entity type
     * @return the generated table
     */
    @SuppressWarnings("unchecked")
    public <T> io.memris.storage.GeneratedTable getOrCreateTable(Class<T> entityClass) {
        return tables.computeIfAbsent(entityClass, ec -> 
            factory.buildTableForEntity(ec, this)
        );
    }

    /**
     * Get the table for the given entity class if it exists.
     *
     * @param entityClass the entity class
     * @return the table, or null if not created
     */
    public io.memris.storage.GeneratedTable getTable(Class<?> entityClass) {
        return tables.get(entityClass);
    }

    /**
     * Get or create a numeric ID counter for the given entity class.
     *
     * @param entityClass the entity class
     * @return the atomic long counter
     */
    public AtomicLong getOrCreateNumericIdCounter(Class<?> entityClass) {
        return numericIdCounters.computeIfAbsent(entityClass, ec -> new AtomicLong(0));
    }

    /**
     * Get the numeric ID counter for the given entity class.
     *
     * @param entityClass the entity class
     * @return the counter, or null if not created
     */
    public AtomicLong getNumericIdCounter(Class<?> entityClass) {
        return numericIdCounters.get(entityClass);
    }

    /**
     * Get or create indexes for the given entity class.
     *
     * @param entityClass the entity class
     * @return the index map
     */
    public Map<String, Object> getOrCreateIndexes(Class<?> entityClass) {
        return indexes.computeIfAbsent(entityClass, ec -> new HashMap<>());
    }

    /**
     * Get indexes for the given entity class.
     *
     * @param entityClass the entity class
     * @return the index map, or null if not created
     */
    public Map<String, Object> getIndexes(Class<?> entityClass) {
        return indexes.get(entityClass);
    }

    @Override
    public void close() {
        // Clear all arena-scoped state
        repositories.clear();
        tables.clear();
        indexes.clear();
        numericIdCounters.clear();
    }
}
