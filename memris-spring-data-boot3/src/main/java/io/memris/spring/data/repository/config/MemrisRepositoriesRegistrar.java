package io.memris.spring.data.repository.config;

import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

import java.lang.annotation.Annotation;

/**
 * Registers Memris repository definitions when {@link EnableMemrisRepositories} is used.
 */
public final class MemrisRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {

    /**
     * Creates a registrar instance.
     */
    public MemrisRepositoriesRegistrar() {
    }

    @Override
    protected Class<? extends Annotation> getAnnotation() {
        return EnableMemrisRepositories.class;
    }

    @Override
    protected RepositoryConfigurationExtension getExtension() {
        return new MemrisRepositoryConfigurationExtension();
    }
}
