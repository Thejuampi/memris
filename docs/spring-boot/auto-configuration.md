# Spring Boot Auto-Configuration

Memris provides comprehensive auto-configuration for Spring Boot applications, requiring minimal setup to get started.

## How Auto-Configuration Works

The `MemrisAutoConfiguration` class is the entry point for Spring Boot integration. It's triggered when:

1. The `MemrisRepositoryFactory` class is present on the classpath
2. No existing `MemrisConfiguration` bean is defined

### Boot 2 vs Boot 3 Differences

| Aspect | Boot 2 | Boot 3 |
|--------|--------|--------|
| Configuration annotation | `@Configuration` | `@AutoConfiguration` |
| Auto-config registration | `META-INF/spring.factories` | `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports` |
| JPA namespace (AttributeConverter) | `javax.persistence.*` | `jakarta.persistence.*` |
| Memris entity annotations | `io.memris.core.*` | `io.memris.core.*` (same for both) |
| Repository interfaces | `io.memris.spring.data.repository.*` | `io.memris.spring.data.repository.*` (same for both) |

**Boot 3 AutoConfiguration:**
```java
package io.memris.spring.boot.autoconfigure;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;

@AutoConfiguration
@ConditionalOnClass(MemrisRepositoryFactory.class)
@EnableConfigurationProperties(MemrisArenaProperties.class)
public class MemrisAutoConfiguration {
    // ...
}
```

**Boot 2 AutoConfiguration:**
```java
package io.memris.spring.boot.autoconfigure;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;

@Configuration
@ConditionalOnClass(MemrisRepositoryFactory.class)
@EnableConfigurationProperties(MemrisArenaProperties.class)
public class MemrisAutoConfiguration {
    // ...
}
```

The only difference is the annotation (`@AutoConfiguration` vs `@Configuration`) and the import for that annotation.

## Auto-Configured Beans

The following beans are automatically created:

### 1. memrisConfiguration

**Type:** `MemrisConfiguration`

Core configuration bean created from `application.yml` properties.

```java
@Bean
@ConditionalOnMissingBean
public MemrisConfiguration memrisConfiguration(MemrisArenaProperties properties) {
    var defaultProps = properties.getArenas()
            .getOrDefault(properties.getDefaultArena(), new MemrisConfigurationProperties());
    return defaultProps.toConfiguration();
}
```

### 2. memrisRepositoryFactory

**Type:** `MemrisRepositoryFactory`

Factory for creating MemrisArena instances from configuration.

```java
@Bean
@ConditionalOnMissingBean
public MemrisRepositoryFactory memrisRepositoryFactory(MemrisConfiguration configuration) {
    return new MemrisRepositoryFactory(configuration);
}
```

### 3. memrisArenaProvider

**Type:** `MemrisArenaProvider`

Resolves named arenas for multi-arena setups.

```java
@Bean
@ConditionalOnMissingBean
public MemrisArenaProvider memrisArenaProvider(MemrisRepositoryFactory factory,
        MemrisArenaProperties properties) {
    return new MemrisArenaProviderImpl(factory, properties);
}
```

### 4. memrisConverterRegistrar

**Type:** `MemrisConverterRegistrar` (Static Bean)

Automatically registers JPA `AttributeConverter` beans with Memris.

```java
@Bean
@ConditionalOnMissingBean
public static MemrisConverterRegistrar memrisConverterRegistrar() {
    return new MemrisConverterRegistrar();
}
```

This bean is static and uses a `BeanPostProcessor` to intercept `@Converter` annotated beans.

### 5. memrisArena

**Type:** `MemrisArena`

The default arena for repository operations.

```java
@Bean
@ConditionalOnMissingBean
public MemrisArena memrisArena(MemrisArenaProvider arenaProvider) {
    return arenaProvider.getDefaultArena();
}
```

## Conditional Behavior

All beans use `@ConditionalOnMissingBean`, meaning:

- If you define your own bean, Memris will use yours
- Memris only auto-configures when beans are missing
- You can selectively override any component

## Disabling Auto-Configuration

### Disable All Auto-Configuration

Exclude Memris auto-configuration entirely:

```java
@SpringBootApplication(exclude = {
    MemrisAutoConfiguration.class
})
public class Application {
    // ...
}
```

### Disable via Properties

Set in `application.yml`:

```yaml
spring:
  autoconfigure:
    exclude:
      - io.memris.spring.boot.autoconfigure.MemrisAutoConfiguration
```

## Custom Configuration

### Programmatic Configuration

Define your own configuration bean:

```java
@Configuration
public class MemrisCustomConfig {
    
    @Bean
    public MemrisConfiguration memrisConfiguration() {
        return MemrisConfiguration.builder()
            .pageSize(2048)
            .maxPages(4096)
            .enableParallelSorting(true)
            .build();
    }
}
```

### Custom Repository Factory

```java
@Bean
public MemrisRepositoryFactory memrisRepositoryFactory(MemrisConfiguration config) {
    // Add custom interceptors or behavior
    return new MemrisRepositoryFactory(config) {
        @Override
        public MemrisArena createArena() {
            // Custom arena creation logic
            return super.createArena();
        }
    };
}
```

### Custom Arena Provider

```java
@Bean
public MemrisArenaProvider memrisArenaProvider(MemrisRepositoryFactory factory,
        MemrisArenaProperties properties) {
    return new MemrisArenaProviderImpl(factory, properties) {
        @Override
        public MemrisArena getArena(String name) {
            // Custom arena resolution
            if ("special".equals(name)) {
                // Return specialized arena configuration
            }
            return super.getArena(name);
        }
    };
}
```

## JPA Converter Registration

The `MemrisConverterRegistrar` automatically detects and registers JPA `AttributeConverter` beans:

```java
@Component
public class MemrisConverterRegistrar implements BeanPostProcessor {
    
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof AttributeConverter<?, ?> converter) {
            // Register with Memris type converter registry
            registerConverter(converter);
        }
        return bean;
    }
}
```

### How It Works

1. Any bean implementing `AttributeConverter<J, S>` is detected
2. The converter is wrapped in a `MemrisJpaAttributeConverterAdapter`
3. Registered with Memris's internal `TypeConverter` registry
4. Automatically used for entity field conversion

### Example Converter

```java
@Converter(autoApply = true)
public class MoneyConverter implements AttributeConverter<Money, BigDecimal> {
    @Override
    public BigDecimal convertToDatabaseColumn(Money money) {
        return money != null ? money.getAmount() : null;
    }
    
    @Override
    public Money convertToEntityAttribute(BigDecimal amount) {
        return amount != null ? new Money(amount) : null;
    }
}
```

## Repository Scanning

Repositories are scanned based on `@EnableMemrisRepositories`:

```java
import io.memris.spring.data.repository.config.EnableMemrisRepositories;

@SpringBootApplication
@EnableMemrisRepositories(basePackages = "com.example.repositories")
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
```

The auto-configuration doesn't scan repositories itself - you must use `@EnableMemrisRepositories`.

## Startup Sequence

1. Spring Boot loads `application.yml`
2. `MemrisArenaProperties` binds to configuration
3. `MemrisAutoConfiguration` creates beans in order:
   - Configuration
   - RepositoryFactory
   - ArenaProvider
   - ConverterRegistrar
   - Default Arena
4. `@EnableMemrisRepositories` scans and creates repository beans
5. Repositories are injected with the default arena

## Troubleshooting

### Bean Not Created

If auto-configuration isn't working:

1. Check that the starter dependency is in your POM
2. Verify `@EnableMemrisRepositories` is present
3. Check for conflicting bean definitions
4. Review logs for auto-configuration report: `--debug`

### Converter Not Registered

If your JPA converter isn't working:

1. Ensure it's annotated with `@Converter`
2. Check that it's a Spring bean (component scanned or @Bean)
3. Verify the converter implements `AttributeConverter<J, S>`
4. Check Memris logs for converter registration

### Multiple Arenas Not Working

If named arenas aren't resolving:

1. Verify `memris.arenas.*` configuration in YAML
2. Check `default-arena` property is set correctly
3. Ensure `MemrisArenaProvider` is being used
4. Use `@Qualifier` to specify arena name
