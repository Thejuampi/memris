package io.memris.spring.boot.autoconfigure;

import io.memris.core.converter.TypeConverterRegistry;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * Registers JPA attribute converters with the Memris type converter registry.
 * <p>
 * <b>Design note:</b> {@link TypeConverterRegistry} is a JVM-level singleton by design.
 * All Spring application contexts in the same JVM share the same registry instance.
 * This is intentional: the Memris engine's type-conversion layer is global, similar to
 * how JDBC drivers are registered JVM-wide via {@code DriverManager}.
 * <p>
 * If multiple Spring contexts with conflicting converters are required, each context
 * should run in its own JVM, or converters must be designed to be context-agnostic.
 */
public final class MemrisConverterRegistrar implements BeanPostProcessor {
    /**
     * Creates a registrar instance.
     */
    public MemrisConverterRegistrar() {
    }

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (bean instanceof jakarta.persistence.AttributeConverter<?, ?>
                || bean.getClass().isAnnotationPresent(jakarta.persistence.Converter.class)) {
            var adapter = new MemrisJpaAttributeConverterAdapter<>(bean);
            TypeConverterRegistry.getInstance().register(adapter);
        }
        return bean;
    }
}
