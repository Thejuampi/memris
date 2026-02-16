package io.memris.spring.boot.autoconfigure;

import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class MemrisAutoConfigurationTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MemrisAutoConfiguration.class));

    @Test
    void autoConfigurationProvidesDefaultBeans() {
        contextRunner.run(context -> Assertions.assertThat(context)
                .hasSingleBean(MemrisRepositoryFactory.class)
                .hasSingleBean(MemrisArenaProvider.class)
                .hasSingleBean(MemrisArena.class));
    }
}
