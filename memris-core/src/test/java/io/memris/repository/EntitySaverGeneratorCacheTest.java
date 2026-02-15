package io.memris.repository;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.MetadataExtractor;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class EntitySaverGeneratorCacheTest {

    private EntitySaverGenerator generator;

    @BeforeEach
    void clearCache() {
        generator = new EntitySaverGenerator();
        generator.clearCache();
    }

    @Test
    void shouldReuseSaverForSameEntityShape() {
        var metadata = MetadataExtractor.extractEntityMetadata(CachedEntity.class);

        var first = generator.generate(CachedEntity.class, metadata);
        var second = generator.generate(CachedEntity.class, metadata);

        assertThat(second).isSameAs(first);
    }

    @Test
    void shouldDifferentiateSaverByConverterIdentity() {
        var registry = TypeConverterRegistry.getInstance();
        registry.registerFieldConverter(ConverterEntity.class, "score", new OffsetConverter(1));
        var first = generator.generate(
                ConverterEntity.class,
                MetadataExtractor.extractEntityMetadata(ConverterEntity.class));

        registry.registerFieldConverter(ConverterEntity.class, "score", new OffsetConverter(7));
        var second = generator.generate(
                ConverterEntity.class,
                MetadataExtractor.extractEntityMetadata(ConverterEntity.class));

        assertThat(second).isNotSameAs(first);
    }

    @Test
    void shouldReuseSameSaverUnderConcurrentGeneration() throws Exception {
        var metadata = MetadataExtractor.extractEntityMetadata(CachedEntity.class);
        var unique = ConcurrentHashMap.newKeySet();
        int threads = 8;
        var ready = new CountDownLatch(threads);
        var start = new CountDownLatch(1);
        var done = new CountDownLatch(threads);
        try (var executor = Executors.newFixedThreadPool(threads)) {
            for (int i = 0; i < threads; i++) {
                executor.submit(() -> {
                    ready.countDown();
                    try {
                        start.await();
                        unique.add(generator.generate(CachedEntity.class, metadata));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        done.countDown();
                    }
                });
            }
            ready.await();
            start.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        }
        assertThat((Set<?>) unique).hasSize(1);
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
