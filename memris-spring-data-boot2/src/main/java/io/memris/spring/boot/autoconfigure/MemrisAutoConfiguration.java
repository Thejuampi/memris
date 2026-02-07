package io.memris.spring.boot.autoconfigure;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for Memris core components and default arena beans.
 */
@Configuration
@ConditionalOnClass(MemrisRepositoryFactory.class)
@EnableConfigurationProperties(MemrisArenaProperties.class)
public class MemrisAutoConfiguration {

    /**
     * Creates the auto-configuration instance.
     */
    public MemrisAutoConfiguration() {
    }

    /**
     * Creates the default Memris configuration from bound properties.
     *
     * @param properties bound Memris properties
     * @return Memris configuration
     */
    @Bean
    @ConditionalOnMissingBean
    public MemrisConfiguration memrisConfiguration(MemrisArenaProperties properties) {
        var defaultProps = properties.getArenas().getOrDefault(properties.getDefaultArena(),
                new MemrisConfigurationProperties());
        return defaultProps.toConfiguration();
    }

    /**
     * Creates a repository factory for the default configuration.
     *
     * @param configuration Memris configuration
     * @return repository factory
     */
    @Bean
    @ConditionalOnMissingBean
    public MemrisRepositoryFactory memrisRepositoryFactory(MemrisConfiguration configuration) {
        return new MemrisRepositoryFactory(configuration);
    }

    /**
     * Creates an arena provider capable of resolving named arenas.
     *
     * @param factory default repository factory
     * @param properties arena properties
     * @return arena provider
     */
    @Bean
    @ConditionalOnMissingBean
    public MemrisArenaProvider memrisArenaProvider(MemrisRepositoryFactory factory,
            MemrisArenaProperties properties) {
        return new MemrisArenaProviderImpl(factory, properties);
    }

    /**
     * Registers JPA attribute converters as Memris type converters.
     *
     * @return converter registrar bean
     */
    @Bean
    @ConditionalOnMissingBean
    public static MemrisConverterRegistrar memrisConverterRegistrar() {
        return new MemrisConverterRegistrar();
    }

    /**
     * Exposes the default arena as a bean.
     *
     * @param provider arena provider
     * @return default arena
     */
    @Bean
    @ConditionalOnMissingBean
    public MemrisArena memrisArena(MemrisArenaProvider provider) {
        return provider.getDefaultArena();
    }
}
