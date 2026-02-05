package io.memris.spring.data.repository.config;

import org.springframework.data.repository.config.RepositoryBeanDefinitionRegistrarSupport;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

import java.lang.annotation.Annotation;

public final class MemrisRepositoriesRegistrar extends RepositoryBeanDefinitionRegistrarSupport {

    @Override
    protected Class<? extends Annotation> getAnnotation() {
        return EnableMemrisRepositories.class;
    }

    @Override
    protected RepositoryConfigurationExtension getExtension() {
        return new MemrisRepositoryConfigurationExtension();
    }
}
