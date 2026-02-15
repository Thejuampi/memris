package io.memris.core;

import io.memris.repository.RepositoryEmitter;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An isolated data space containing its own tables, repositories, and indexes.
 * <p>
 * Multiple arenas can coexist in the same factory, enabling:
 * <ul>
 * <li>Multi-tenant applications (one arena per tenant)</li>
 * <li>Test isolation (fresh arena per test)</li>
 * <li>Parallel processing (different arenas in different threads)</li>
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
    private final RepositoryEmitter repositoryEmitter;

    // Arena-scoped state
    private final ConcurrentMap<Class<?>, io.memris.storage.GeneratedTable> tables = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, Map<String, Object>> indexes = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, AtomicLong> numericIdCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<Class<?>, Object> repositories = new ConcurrentHashMap<>();

    // Mapping from entity class to repository interface class
    private final ConcurrentMap<Class<?>, Class<?>> entityToRepositoryMap = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public MemrisArena(long arenaId, MemrisRepositoryFactory factory) {
        this.arenaId = arenaId;
        this.factory = factory;
        this.repositoryEmitter = new RepositoryEmitter(factory.getConfiguration());
    }

    /**
     * Get the unique ID of this arena.
     */
    public long getArenaId() {
        return arenaId;
    }

    /**
     * Get the factory that created this arena.
     */
    public MemrisRepositoryFactory getFactory() {
        return factory;
    }

    /**
     * Create or get a repository for the given interface in this arena.
     * <p>
     * If the repository already exists in this arena, returns the cached instance.
     *
     * @param repositoryInterface the repository interface
     * @param <T>                 the entity type
     * @param <R>                 the repository interface type
     * @return the repository instance
     */
    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            Object repository = repositories.computeIfAbsent(repositoryInterface, ignored -> {
                R created = repositoryEmitter.createRepository(repositoryInterface, this);
                Class<T> entityClass = extractEntityClass(repositoryInterface);
                entityToRepositoryMap.put(entityClass, repositoryInterface);
                return created;
            });
            return (R) repository;
        } finally {
            readLock.unlock();
        }
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
     * @param <T>         the entity type
     * @return the repository instance, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> MemrisRepository<T> getRepository(Class<T> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            Class<?> repositoryInterface = entityToRepositoryMap.get(entityClass);
            if (repositoryInterface == null) {
                return null;
            }
            return (MemrisRepository<T>) repositories.get(repositoryInterface);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get or create a table for the given entity class in this arena.
     * <p>
     * Uses the factory's table builder but caches the result per-arena.
     *
     * @param entityClass the entity class
     * @param <T>         the entity type
     * @return the generated table
     */
    public <T> io.memris.storage.GeneratedTable getOrCreateTable(Class<T> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            return tables.computeIfAbsent(entityClass, ec -> factory.buildTableForEntity(ec, this));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the table for the given entity class if it exists.
     *
     * @param entityClass the entity class
     * @return the table, or null if not created
     */
    public io.memris.storage.GeneratedTable getTable(Class<?> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            return tables.get(entityClass);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get or create a numeric ID counter for the given entity class.
     *
     * @param entityClass the entity class
     * @return the atomic long counter
     */
    public AtomicLong getOrCreateNumericIdCounter(Class<?> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            return numericIdCounters.computeIfAbsent(entityClass, ec -> new AtomicLong(0));
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get the numeric ID counter for the given entity class.
     *
     * @param entityClass the entity class
     * @return the counter, or null if not created
     */
    public AtomicLong getNumericIdCounter(Class<?> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            return numericIdCounters.get(entityClass);
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get or create indexes for the given entity class.
     *
     * @param entityClass the entity class
     * @return the index map
     */
    public Map<String, Object> getOrCreateIndexes(Class<?> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            return indexes.computeIfAbsent(entityClass, ec -> new ConcurrentHashMap<>());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Get indexes for the given entity class.
     *
     * @param entityClass the entity class
     * @return the index map, or null if not created
     */
    public Map<String, Object> getIndexes(Class<?> entityClass) {
        var readLock = lifecycleLock.readLock();
        readLock.lock();
        try {
            assertOpen();
            return indexes.get(entityClass);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void close() {
        var writeLock = lifecycleLock.writeLock();
        writeLock.lock();
        try {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            repositoryEmitter.clearCaches();
            repositories.clear();
            entityToRepositoryMap.clear();
            tables.clear();
            indexes.clear();
            numericIdCounters.clear();
        } finally {
            writeLock.unlock();
        }
    }

    private void assertOpen() {
        if (closed.get()) {
            throw new IllegalStateException("Arena is closed");
        }
    }
}
