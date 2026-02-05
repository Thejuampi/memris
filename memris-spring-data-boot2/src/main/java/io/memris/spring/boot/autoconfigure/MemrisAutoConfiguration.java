package io.memris.spring.boot.autoconfigure;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConditionalOnClass(MemrisRepositoryFactory.class)
@EnableConfigurationProperties(MemrisArenaProperties.class)
public class MemrisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public MemrisConfiguration memrisConfiguration(MemrisArenaProperties properties) {
        var defaultProps = properties.getArenas().getOrDefault(properties.getDefaultArena(),
                new MemrisConfigurationProperties());
        return defaultProps.toConfiguration();
    }

    @Bean
    @ConditionalOnMissingBean
    public MemrisRepositoryFactory memrisRepositoryFactory(MemrisConfiguration configuration) {
        return new MemrisRepositoryFactory(configuration);
    }

    @Bean
    @ConditionalOnMissingBean
    public MemrisArenaProvider memrisArenaProvider(MemrisRepositoryFactory factory,
            MemrisArenaProperties properties) {
        return new MemrisArenaProviderImpl(factory, properties);
    }

    @Bean
    @ConditionalOnMissingBean
    public MemrisConverterRegistrar memrisConverterRegistrar() {
        return new MemrisConverterRegistrar();
    }

    @Bean
    @ConditionalOnMissingBean
    public MemrisArena memrisArena(MemrisArenaProvider provider) {
        return provider.getDefaultArena();
    }
}
