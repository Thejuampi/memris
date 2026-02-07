package io.memris.spring.boot.autoconfigure;

import io.memris.core.converter.TypeConverterRegistry;
import org.springframework.beans.factory.config.BeanPostProcessor;

/**
 * Registers JPA attribute converters with the Memris type converter registry.
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
