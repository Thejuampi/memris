package io.memris.core;

import io.memris.core.Query;

import io.memris.core.Param;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TopKLimitOptimizationTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldReturnTopKResultsWithoutSortingAllRows() {
        TopKProductRepository repo = arena.createRepository(TopKProductRepository.class);
        
        // Create 100 products with varying prices
        for (int i = 0; i < 100; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // Request top 5 by price - should only sort enough to get top 5
        List<Product> top5 = repo.findTop5ByOrderByPriceDesc();

        assertThat(top5).hasSize(5);
        // Highest prices should be first (9900, 9800, 9700, 9600, 9500)
        assertThat(top5.get(0).price).isEqualTo(9900L);
        assertThat(top5.get(1).price).isEqualTo(9800L);
        assertThat(top5.get(2).price).isEqualTo(9700L);
        assertThat(top5.get(3).price).isEqualTo(9600L);
        assertThat(top5.get(4).price).isEqualTo(9500L);
    }

    @Test
    void shouldReturnFirstResultWithoutSortingAll() {
        TopKProductRepository repo = arena.createRepository(TopKProductRepository.class);
        
        // Create products
        for (int i = 0; i < 50; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // Request first by price ascending
        List<Product> first = repo.findFirstByOrderByPriceAsc();

        assertThat(first).hasSize(1);
        assertThat(first.get(0).price).isEqualTo(0L);
    }

    @Test
    void shouldApplyLimitWithQueryAnnotation() {
        TopKProductRepository repo = arena.createRepository(TopKProductRepository.class);
        
        for (int i = 0; i < 20; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        List<Product> top3 = repo.findTop3ByPriceGreaterThanOrderByPriceDesc(500L);

        assertThat(top3).hasSize(3);
        // Products with price > 500: 600, 700, 800, 900, ... 1900
        // Top 3 should be 1900, 1800, 1700
        assertThat(top3.get(0).price).isGreaterThanOrEqualTo(1700L);
        assertThat(top3.get(1).price).isGreaterThanOrEqualTo(1700L);
        assertThat(top3.get(2).price).isGreaterThanOrEqualTo(1700L);
    }

    public interface TopKProductRepository extends MemrisRepository<Product> {
        List<Product> findTop5ByOrderByPriceDesc();
        
        List<Product> findFirstByOrderByPriceAsc();
        
        @Query("select p from Product p where p.price > :minPrice order by p.price desc")
        List<Product> findTop3ByPriceGreaterThanOrderByPriceDesc(@Param("minPrice") long minPrice);
        
        Product save(Product product);
    }
}
