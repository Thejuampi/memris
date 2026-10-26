package io.memris.realworld;

import io.memris.core.Product;

import io.memris.core.Query;

import io.memris.core.Param;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;



import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CatalogRankingRealWorldTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory(RealWorldSupport.configuration().build());
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldRankTheFiveMostExpensiveProducts() {
        TopKProductRepository repo = arena.createRepository(TopKProductRepository.class);
        
        // Create 100 products with varying prices
        for (int i = 0; i < 100; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // Request the five most expensive products.
        List<Product> top5 = repo.findTop5ByOrderByPriceDesc();

        assertThat(top5).extracting(product -> product.price).containsExactly(9900L, 9800L, 9700L, 9600L, 9500L);
    }

    @Test
    void shouldFindTheCheapestProduct() {
        TopKProductRepository repo = arena.createRepository(TopKProductRepository.class);
        
        // Create products
        for (int i = 0; i < 50; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // Request first by price ascending
        List<Product> first = repo.findFirstByOrderByPriceAsc();

        assertThat(first).extracting(product -> product.price).containsExactly(0L);
    }

    @Test
    void shouldApplyLimitWithQueryAnnotation() {
        TopKProductRepository repo = arena.createRepository(TopKProductRepository.class);
        
        for (int i = 0; i < 20; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        List<Product> top3 = repo.findTop3ByPriceGreaterThanOrderByPriceDesc(500L);

        assertThat(top3).extracting(product -> product.price).containsExactly(1900L, 1800L, 1700L);
    }

    public interface TopKProductRepository extends MemrisRepository<Product> {
        List<Product> findTop5ByOrderByPriceDesc();
        
        List<Product> findFirstByOrderByPriceAsc();
        
        @Query("select p from Product p where p.price > :minPrice order by p.price desc")
        List<Product> findTop3ByPriceGreaterThanOrderByPriceDesc(@Param("minPrice") long minPrice);
        
        Product save(Product product);
    }
}
