package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadataProvider;
import io.memris.core.GeneratedValue;
import io.memris.core.Id;
import io.memris.core.JoinColumn;
import io.memris.core.ManyToOne;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.core.MetadataExtractor;
import io.memris.core.Modifying;
import io.memris.core.OneToMany;
import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeHotPathRegressionTest {

    private MemrisRepositoryFactory factory;

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldResolveMetadataDuringSetupNotDuringRepeatedHydration() {
        var provider = new CountingMetadataProvider();
        var configuration = MemrisConfiguration.builder()
                .entityMetadataProvider(provider)
                .build();
        factory = new MemrisRepositoryFactory(configuration);
        var arena = factory.createArena();

        var parentRepository = arena.createRepository(SetupParentRepository.class);
        var childRepository = arena.createRepository(SetupChildRepository.class);

        var parent = parentRepository.save(new SetupParent("parent"));
        childRepository.save(new SetupChild(parent, "child-a"));
        childRepository.save(new SetupChild(parent, "child-b"));

        var before = provider.callCount();
        for (var i = 0; i < 20; i++) {
            parentRepository.findByName("parent");
        }
        var after = provider.callCount();

        assertThat(after).isEqualTo(before);
    }

    @Test
    void shouldApplyFieldConverterForUpdateQueryAssignments() {
        TypeConverterRegistry.getInstance().registerFieldConverter(
                ConvertedInventory.class,
                "stock",
                new OffsetStockConverter());

        factory = new MemrisRepositoryFactory();
        var arena = factory.createArena();
        var repository = arena.createRepository(ConvertedInventoryRepository.class);

        repository.save(new ConvertedInventory("SKU-1", 10));
        repository.updateStockBySku("SKU-1", 13);
        var updated = repository.findBySku("SKU-1").orElseThrow();

        assertThat(updated.stock).isEqualTo(13);
    }

    private static final class CountingMetadataProvider implements EntityMetadataProvider {
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public <T> EntityMetadata<T> getMetadata(Class<T> entityClass) {
            calls.incrementAndGet();
            return MetadataExtractor.extractEntityMetadata(entityClass);
        }

        int callCount() {
            return calls.get();
        }
    }

    private static final class OffsetStockConverter implements TypeConverter<Integer, Integer> {
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
            return javaValue == null ? null : javaValue + 5;
        }

        @Override
        public Integer fromStorage(Integer storageValue) {
            return storageValue == null ? null : storageValue - 5;
        }
    }

    @Entity
    public static class SetupParent {
        @Id
        @GeneratedValue
        public Long id;
        public String name;
        @OneToMany(mappedBy = "parent")
        public List<SetupChild> children;

        public SetupParent() {
        }

        public SetupParent(String name) {
            this.name = name;
        }
    }

    @Entity
    public static class SetupChild {
        @Id
        @GeneratedValue
        public Long id;
        @ManyToOne
        @JoinColumn(name = "parent_id")
        public SetupParent parent;
        public String label;

        public SetupChild() {
        }

        public SetupChild(SetupParent parent, String label) {
            this.parent = parent;
            this.label = label;
        }
    }

    @Entity
    public static class ConvertedInventory {
        @Id
        @GeneratedValue
        public Long id;
        public String sku;
        public int stock;

        public ConvertedInventory() {
        }

        public ConvertedInventory(String sku, int stock) {
            this.sku = sku;
            this.stock = stock;
        }
    }

    public interface SetupParentRepository extends MemrisRepository<SetupParent> {
        SetupParent save(SetupParent parent);

        List<SetupParent> findByName(String name);
    }

    public interface SetupChildRepository extends MemrisRepository<SetupChild> {
        SetupChild save(SetupChild child);
    }

    public interface ConvertedInventoryRepository extends MemrisRepository<ConvertedInventory> {
        ConvertedInventory save(ConvertedInventory entity);

        Optional<ConvertedInventory> findBySku(String sku);

        @Modifying
        @Query("update ConvertedInventory c set c.stock = :stock where c.sku = :sku")
        long updateStockBySku(@Param("sku") String sku, @Param("stock") int stock);
    }
}
