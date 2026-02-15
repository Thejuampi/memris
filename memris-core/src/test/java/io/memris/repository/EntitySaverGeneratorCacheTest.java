package io.memris.repository;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EntitySaverGeneratorCacheTest {

    @BeforeEach
    void clearCache() {
        EntitySaverGenerator.clearCacheForTests();
    }

    @Test
    void shouldReuseSaverForSameEntityShape() {
        var metadata = MetadataExtractor.extractEntityMetadata(CachedEntity.class);

        var first = EntitySaverGenerator.generate(CachedEntity.class, metadata);
        var second = EntitySaverGenerator.generate(CachedEntity.class, metadata);

        assertThat(second).isSameAs(first);
    }

    @Test
    void shouldDifferentiateSaverByConverterIdentity() {
        var registry = TypeConverterRegistry.getInstance();
        registry.registerFieldConverter(ConverterEntity.class, "score", new OffsetConverter(1));
        var first = EntitySaverGenerator.generate(
                ConverterEntity.class,
                MetadataExtractor.extractEntityMetadata(ConverterEntity.class));

        registry.registerFieldConverter(ConverterEntity.class, "score", new OffsetConverter(7));
        var second = EntitySaverGenerator.generate(
                ConverterEntity.class,
                MetadataExtractor.extractEntityMetadata(ConverterEntity.class));

        assertThat(second).isNotSameAs(first);
    }

    @Entity
    static class CachedEntity {
        @Id
        public Long id;
        public String name;
        public Profile profile;
    }

    static class Profile {
        public String city;
    }

    @Entity
    static class ConverterEntity {
        @Id
        public Long id;
        public Integer score;
    }

    private record OffsetConverter(int offset) implements TypeConverter<Integer, Integer> {
        @Override
        public Class<Integer> javaType() {
            return Integer.class;
        }

        @Override
        public Class<Integer> storageType() {
            return Integer.class;
        }

        @Override
        public Integer toStorage(Integer javaValue) {
            return javaValue == null ? null : javaValue + offset;
        }

        @Override
        public Integer fromStorage(Integer storageValue) {
            return storageValue == null ? null : storageValue - offset;
        }
    }
}
