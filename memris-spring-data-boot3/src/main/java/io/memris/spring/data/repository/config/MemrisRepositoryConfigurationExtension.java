package io.memris.spring.data.repository.config;

import io.memris.spring.data.repository.MemrisSpringRepository;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.core.RepositoryMetadata;

public final class MemrisRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

    @Override
    public String getModuleName() {
        return "Memris";
    }

    @Override
    public String getRepositoryFactoryBeanClassName() {
        return "io.memris.spring.data.repository.support.MemrisRepositoryFactoryBean";
    }

    @Override
    protected String getModulePrefix() {
        return "memris";
    }

    @Override
    protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
        return MemrisSpringRepository.class.isAssignableFrom(metadata.getRepositoryInterface());
    }
}
