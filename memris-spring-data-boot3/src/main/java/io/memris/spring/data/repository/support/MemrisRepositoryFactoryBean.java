package io.memris.spring.data.repository.support;

import io.memris.core.MemrisArena;
import io.memris.spring.boot.autoconfigure.MemrisArenaProvider;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;

public class MemrisRepositoryFactoryBean<T extends Repository<S, ID>, S, ID>
        extends RepositoryFactoryBeanSupport<T, S, ID> implements BeanFactoryAware {

    private BeanFactory beanFactory;

    public MemrisRepositoryFactoryBean(Class<? extends T> repositoryInterface) {
        super(repositoryInterface);
    }

    @Override
    protected RepositoryFactorySupport createRepositoryFactory() {
        var arenaProvider = beanFactory.getBean(MemrisArenaProvider.class);
        MemrisArena arena = arenaProvider.getDefaultArena();
        return new MemrisSpringRepositoryFactory(arena);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }
}
