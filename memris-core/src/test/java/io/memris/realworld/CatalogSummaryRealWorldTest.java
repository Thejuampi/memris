package io.memris.realworld;

import io.memris.core.Param;
import io.memris.core.Product;
import io.memris.core.Query;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogSummaryRealWorldTest {
    @Test
    void shouldReportAvailabilityAsProductsSellOutAreRestockedAndAreRemoved() {
        try (var factory = new MemrisRepositoryFactory(RealWorldSupport.configuration().build());
                var arena = factory.createArena()) {
            var catalog = arena.createRepository(CatalogSummaryRepository.class);
            assertThat(catalog.count()).isZero();
            assertThat(catalog.existsByName("Laptop")).isFalse();
            assertThat(catalog.countInStock()).isZero();

            var laptop = catalog.save(new Product("LAPTOP", "Laptop", 129999, 1));
            var mouse = catalog.save(new Product("MOUSE", "Mouse", 2999, 0));
            catalog.save(new Product("CABLE", "Cable", 999, 20));
            assertThat(catalog.count()).isEqualTo(3);
            assertThat(catalog.countByNameLike("%a%")).isEqualTo(2);
            assertThat(catalog.existsByName("Laptop")).isTrue();
            assertThat(catalog.existsByName("Missing")).isFalse();
            assertThat(catalog.existsBySkuQuery("LAPTOP")).isTrue();
            assertThat(catalog.existsBySkuQuery("MISSING")).isFalse();
            assertThat(catalog.countInStock()).isEqualTo(2);

            laptop.stock = 0;
            catalog.save(laptop);
            assertThat(catalog.countInStock()).isEqualTo(1);
            mouse.stock = 10;
            catalog.save(mouse);
            assertThat(catalog.countInStock()).isEqualTo(2);

            catalog.deleteById(laptop.id);
            assertThat(catalog.count()).isEqualTo(2);
            assertThat(catalog.existsByName("Laptop")).isFalse();
            assertThat(catalog.existsBySkuQuery("LAPTOP")).isFalse();
            assertThat(catalog.countByNameLike("%a%")).isEqualTo(1);
            catalog.deleteAll();
            assertThat(catalog.count()).isZero();
            assertThat(catalog.countInStock()).isZero();
        }
    }

    public interface CatalogSummaryRepository extends MemrisRepository<Product> {
        Product save(Product product);
        long count();
        long countByNameLike(String pattern);
        boolean existsByName(String name);
        void deleteById(Long id);
        void deleteAll();

        @Query("select p from Product p where p.sku = :sku")
        boolean existsBySkuQuery(@Param("sku") String sku);

        @Query("select count(p) from Product p where p.stock > 0")
        long countInStock();
    }
}
