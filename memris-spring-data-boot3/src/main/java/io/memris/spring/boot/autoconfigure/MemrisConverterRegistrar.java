package io.memris.spring.boot.autoconfigure;

import io.memris.core.converter.TypeConverterRegistry;
import org.springframework.beans.factory.config.BeanPostProcessor;

public final class MemrisConverterRegistrar implements BeanPostProcessor {
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
