package io.memris.spring.data.repository.config;

import io.memris.spring.data.repository.MemrisSpringRepository;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.core.RepositoryMetadata;

/**
 * Spring Data repository configuration extension for Memris repositories.
 */
public final class MemrisRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

    /**
     * Creates an extension instance.
     */
    public MemrisRepositoryConfigurationExtension() {
    }

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
