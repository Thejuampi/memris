package io.memris.spring.data.repository.support;

import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;
import org.springframework.data.repository.core.EntityInformation;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

public final class MemrisSpringRepositoryFactory extends RepositoryFactorySupport {
    private final MemrisArena arena;

    public MemrisSpringRepositoryFactory(MemrisArena arena) {
        this.arena = arena;
    }

    @Override
    protected Object getTargetRepository(RepositoryInformation information) {
        @SuppressWarnings({"rawtypes", "unchecked"})
        Class<? extends MemrisRepository> repositoryInterface = (Class) information.getRepositoryInterface();
        return arena.createRepository(repositoryInterface);
    }

    @Override
    protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
        return metadata.getRepositoryInterface();
    }

    @Override
    public <T, ID> EntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
        return new MemrisEntityInformation<>(domainClass);
    }
}
