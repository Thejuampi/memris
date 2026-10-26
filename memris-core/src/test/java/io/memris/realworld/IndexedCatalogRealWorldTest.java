package io.memris.realworld;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class IndexedCatalogRealWorldTest {
    @ParameterizedTest(name = "catalog lifecycle with pattern indexes={0}")
    @ValueSource(booleans = { true, false })
    void shouldKeepCatalogSearchesConsistentAfterRepricingRenamingDeletionAndRestocking(boolean patternIndexes) {
        var config = RealWorldSupport.configuration()
                .enablePrefixIndex(patternIndexes).enableSuffixIndex(patternIndexes).build();
        try (var factory = new MemrisRepositoryFactory(config); var arena = factory.createArena()) {
            var catalog = arena.createRepository(CatalogRepository.class);
            var laptop = catalog.save(new CatalogProduct("LAPTOP", "Pro Laptop", "Acme Labs", "devices", 2000, 3));
            var mouse = catalog.save(new CatalogProduct("MOUSE", "Pro Mouse", "Acme Labs", "devices", 200, 0));
            var cable = catalog.save(new CatalogProduct("CABLE", "Basic Cable", "Tools Inc", "accessories", 100, 10));

            assertThat(catalog.findByNameStartingWith("Pro")).extracting(row -> row.sku)
                    .containsExactlyInAnyOrder("LAPTOP", "MOUSE");
            assertThat(catalog.findByBrandEndingWith("Labs")).extracting(row -> row.sku)
                    .containsExactlyInAnyOrder("LAPTOP", "MOUSE");
            assertThat(catalog.findByCategoryAndStockGreaterThanOrderByPriceAsc("devices", 0))
                    .extracting(row -> row.sku).containsExactly("LAPTOP");
            assertThat(catalog.countByCategoryAndStockGreaterThan("devices", 0)).isEqualTo(1);

            laptop.sku = "TRAVEL";
            laptop.name = "Travel Laptop";
            laptop.brand = "North Inc";
            laptop.category = "clearance";
            laptop.price = 3000;
            laptop.stock = 0;
            catalog.save(laptop);

            assertThat(catalog.findBySku("LAPTOP")).isEmpty();
            assertThat(catalog.findBySku("TRAVEL")).map(row -> row.id).contains(laptop.id);
            assertThat(catalog.findByNameStartingWith("Pro")).extracting(row -> row.id).containsExactly(mouse.id);
            assertThat(catalog.findByBrandEndingWith("Labs")).extracting(row -> row.id).containsExactly(mouse.id);
            assertThat(catalog.findByBrandEndingWith("Inc")).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(laptop.id, cable.id);
            assertThat(catalog.findByPriceBetween(500, 2500)).isEmpty();
            assertThat(catalog.existsByCategoryAndStockGreaterThan("devices", 0)).isFalse();
            assertThat(catalog.countByCategoryAndStockGreaterThan("devices", 0)).isZero();

            catalog.deleteById(mouse.id);
            var tablet = catalog.save(new CatalogProduct("TABLET", "Pro Tablet", "Acme Labs", "devices", 1500, 2));
            assertThat(tablet.id).isNotEqualTo(mouse.id);
            assertThat(catalog.findById(mouse.id)).isEmpty();
            assertThat(catalog.findBySku("MOUSE")).isEmpty();
            assertThat(catalog.findByNameStartingWith("Pro")).extracting(row -> row.id).containsExactly(tablet.id);
            assertThat(catalog.findByBrandEndingWith("Labs")).extracting(row -> row.id).containsExactly(tablet.id);
            assertThat(catalog.findByPriceBetween(500, 2500)).extracting(row -> row.id).containsExactly(tablet.id);
            assertThat(catalog.existsByCategoryAndStockGreaterThan("devices", 0)).isTrue();

            catalog.deleteAll();
            assertThat(catalog.count()).isZero();
            assertThat(catalog.findByNameStartingWith("Pro")).isEmpty();
            assertThat(catalog.findByBrandEndingWith("Labs")).isEmpty();
            assertThat(catalog.existsByCategoryAndStockGreaterThan("devices", 0)).isFalse();
            var restocked = catalog.save(new CatalogProduct("NEW", "Pro Phone", "Acme Labs", "devices", 800, 1));
            assertThat(catalog.findByNameStartingWith("Pro")).extracting(row -> row.id).containsExactly(restocked.id);
            assertThat(catalog.findByBrandEndingWith("Labs")).extracting(row -> row.id).containsExactly(restocked.id);
        }
    }

    @ParameterizedTest(name = "nullable catalog with pattern indexes={0}")
    @ValueSource(booleans = { true, false })
    void shouldSearchIncompleteCatalogEntriesWithoutMatchingNullNamesOrBrands(boolean patternIndexes) {
        var config = RealWorldSupport.configuration()
                .enablePrefixIndex(patternIndexes).enableSuffixIndex(patternIndexes).build();
        try (var factory = new MemrisRepositoryFactory(config); var arena = factory.createArena()) {
            var catalog = arena.createRepository(CatalogRepository.class);
            var incomplete = catalog.save(new CatalogProduct("DRAFT", null, null, "devices", 500, 0));
            var empty = catalog.save(new CatalogProduct("EMPTY", "", "", "devices", 600, 1));
            var complete = catalog.save(new CatalogProduct("READY", "Pro Phone", "Acme Labs", "devices", 800, 2));

            assertThat(catalog.findByNameStartingWith("")).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(empty.id, complete.id);
            assertThat(catalog.findByBrandEndingWith("")).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(empty.id, complete.id);
            assertThat(catalog.findByNameStartingWith(null)).isEmpty();
            assertThat(catalog.findByBrandEndingWith(null)).isEmpty();
            assertThat(catalog.findByPriceBetween(500, 500)).extracting(row -> row.id).containsExactly(incomplete.id);
        }
    }

    @Entity
    public static class CatalogProduct {
        @Id
        public Long id;
        @Index(type = Index.IndexType.HASH)
        public String sku;
        @Index(type = Index.IndexType.PREFIX)
        public String name;
        @Index(type = Index.IndexType.SUFFIX)
        public String brand;
        @Index(type = Index.IndexType.HASH)
        public String category;
        @Index(type = Index.IndexType.BTREE)
        public long price;
        public int stock;

        public CatalogProduct() {
        }

        public CatalogProduct(String sku, String name, String brand, String category, long price, int stock) {
            this.sku = sku;
            this.name = name;
            this.brand = brand;
            this.category = category;
            this.price = price;
            this.stock = stock;
        }
    }

    public interface CatalogRepository extends MemrisRepository<CatalogProduct> {
        CatalogProduct save(CatalogProduct product);
        Optional<CatalogProduct> findById(Long id);
        Optional<CatalogProduct> findBySku(String sku);
        List<CatalogProduct> findByNameStartingWith(String prefix);
        List<CatalogProduct> findByBrandEndingWith(String suffix);
        List<CatalogProduct> findByPriceBetween(long min, long max);
        List<CatalogProduct> findByCategoryAndStockGreaterThanOrderByPriceAsc(String category, int stock);
        long countByCategoryAndStockGreaterThan(String category, int stock);
        boolean existsByCategoryAndStockGreaterThan(String category, int stock);
        long count();
        void deleteById(Long id);
        void deleteAll();
    }
}
