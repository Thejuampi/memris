package io.memris.spring.boot.autoconfigure;

import io.memris.core.converter.TypeConverterRegistry;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import javax.persistence.AttributeConverter;
import javax.persistence.Converter;

class MemrisConverterRegistrarTest {
    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MemrisAutoConfiguration.class));

    @Test
    void registersAttributeConverter() {
        contextRunner.withBean(SkuConverter.class).run(context -> Assertions.assertThat(
                TypeConverterRegistry.getInstance().getConverter(Sku.class)).isNotNull());
    }

    static final class Sku {
        private final String value;

        private Sku(String value) {
            this.value = value;
        }
    }

    @Converter(autoApply = true)
    static final class SkuConverter implements AttributeConverter<Sku, String> {
        @Override
        public String convertToDatabaseColumn(Sku attribute) {
            return attribute == null ? null : attribute.value;
        }

        @Override
        public Sku convertToEntityAttribute(String dbData) {
            return dbData == null ? null : new Sku(dbData);
        }
    }
}
