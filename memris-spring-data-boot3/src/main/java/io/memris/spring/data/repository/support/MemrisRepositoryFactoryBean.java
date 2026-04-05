package io.memris.spring.data.repository.support;

import io.memris.repository.MemrisArena;
import io.memris.spring.boot.autoconfigure.MemrisArenaProvider;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

/**
 * Spring Data factory bean that creates Memris-backed repository instances.
 *
 * @param <T> repository type
 * @param <S> aggregate type
 * @param <ID> identifier type
 */
public class MemrisRepositoryFactoryBean<T extends Repository<S, ID>, S, ID>
        extends RepositoryFactoryBeanSupport<T, S, ID> implements BeanFactoryAware {

    private BeanFactory beanFactory;

    /**
     * Creates a factory bean for the given repository interface.
     *
     * @param repositoryInterface repository interface class
     */
    public MemrisRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {
        MemrisArenaProvider arenaProvider;
        try {
            arenaProvider = beanFactory.getBean(MemrisArenaProvider.class);
        } catch (NoSuchBeanDefinitionException e) {
            throw new IllegalStateException(
                    "MemrisArenaProvider bean not found. Ensure @EnableMemrisRepositories is configured on a @Configuration class.",
                    e);
        }
        MemrisArena arena = arenaProvider.getDefaultArena();
        return new MemrisSpringRepositoryFactory(arena);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }
}
