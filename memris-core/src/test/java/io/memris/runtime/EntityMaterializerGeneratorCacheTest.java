package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class EntityMaterializerGeneratorCacheTest {

    private final EntityMaterializerGenerator generator = new EntityMaterializerGenerator();

    @BeforeEach
    void clearCache() {
        EntityMaterializerGenerator.clearCacheForTests();
    }

    @Test
    void shouldReuseMaterializerForSameEntityShape() {
        var metadata = MetadataExtractor.extractEntityMetadata(CachedEntity.class);

        var first = generator.generate(metadata);
        var second = generator.generate(metadata);

        assertThat(second).isSameAs(first);
    }

    @Test
    void shouldDifferentiateMaterializerByConverterIdentity() {
        var registry = TypeConverterRegistry.getInstance();
        registry.registerFieldConverter(ConverterEntity.class, "score", new OffsetConverter(1));
        var first = generator.generate(MetadataExtractor.extractEntityMetadata(ConverterEntity.class));

        registry.registerFieldConverter(ConverterEntity.class, "score", new OffsetConverter(3));
        var second = generator.generate(MetadataExtractor.extractEntityMetadata(ConverterEntity.class));

        assertThat(second).isNotSameAs(first);
    }

    @Entity
    public static class CachedEntity {
        @Id
        public Long id;
        public String name;
        public Profile profile;

        public CachedEntity() {
        }
    }

    public static class Profile {
        public String city;

        public Profile() {
        }
    }

    @Entity
    public static class ConverterEntity {
        @Id
        public Long id;
        public Integer score;

        public ConverterEntity() {
        }
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
