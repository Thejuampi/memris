package io.memris.consumer;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;
import org.springframework.boot.SpringBootVersion;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.memris.consumer.InventoryApplication.Sku;

@Configuration(proxyBeanMethods = false)
public class ConverterConfiguration {
    @Bean
    public SkuConverter skuConverter() {
        return new SkuConverter();
    }

    public static void verifyPersistenceGeneration() throws Exception {
        if (!SpringBootVersion.getVersion().startsWith("2.")) {
            throw new IllegalStateException("Unexpected Spring Boot generation");
        }
        try {
            Class.forName("jakarta.persistence.AttributeConverter");
            throw new IllegalStateException("Mixed persistence API generations");
        } catch (ClassNotFoundException expected) {
            // Each consumer resolves only its own starter's persistence API.
        }
    }

    @Converter(autoApply = true)
    public static class SkuConverter implements AttributeConverter<Sku, String> {
        @Override
        public String convertToDatabaseColumn(Sku value) {
            return value == null ? null : value.value();
        }

        @Override
        public Sku convertToEntityAttribute(String value) {
            return value == null ? null : new Sku(value);
        }
    }
}
