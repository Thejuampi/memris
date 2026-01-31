package io.memris.spring;

import io.memris.core.Query;

import io.memris.core.Param;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
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
import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FastExistsCountTest {

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
    void shouldShortCircuitExistsWithoutFetchingAllRows() {
        FastQueryRepository repo = arena.createRepository(FastQueryRepository.class);
        
        // Create many products
        for (int i = 0; i < 1000; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // existsBy should short-circuit on first match
        long startTime = System.nanoTime();
        boolean exists = repo.existsByName("Product 0");
        long duration = System.nanoTime() - startTime;

        assertThat(exists).isTrue();
        // Should be very fast since it short-circuits
        assertThat(duration).isLessThan(100_000_000L); // 100ms threshold
    }

    @Test
    void shouldReturnFalseWhenNoMatchExists() {
        FastQueryRepository repo = arena.createRepository(FastQueryRepository.class);
        
        for (int i = 0; i < 100; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        boolean exists = repo.existsByName("NonExistent");

        assertThat(exists).isFalse();
    }

    @Test
    void shouldShortCircuitCountWithEarlyExit() {
        FastQueryRepository repo = arena.createRepository(FastQueryRepository.class);
        
        for (int i = 0; i < 500; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // countBy should use optimized count path
        long startTime = System.nanoTime();
        long count = repo.countByNameLike("Product%");
        long duration = System.nanoTime() - startTime;

        assertThat(count).isEqualTo(500);
        assertThat(duration).isLessThan(100_000_000L); // 100ms threshold
    }

    @Test
    void shouldFastCountAllUsingTableStats() {
        FastQueryRepository repo = arena.createRepository(FastQueryRepository.class);
        
        for (int i = 0; i < 100; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        long count = repo.count();

        assertThat(count).isEqualTo(100);
    }

    @Test
    void shouldFastExistsWithQueryAnnotation() {
        FastQueryRepository repo = arena.createRepository(FastQueryRepository.class);
        
        for (int i = 0; i < 1000; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        long startTime = System.nanoTime();
        boolean exists = repo.fastExistsBySku("SKU-0");
        long duration = System.nanoTime() - startTime;

        assertThat(exists).isTrue();
        assertThat(duration).isLessThan(100_000_000L);
    }

    @Test
    void shouldFastCountWithQueryAnnotation() {
        FastQueryRepository repo = arena.createRepository(FastQueryRepository.class);
        
        for (int i = 0; i < 200; i++) {
            int stock = i % 2 == 0 ? 10 : 0; // Half have stock, half don't
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, stock));
        }

        long count = repo.fastCountInStock();

        assertThat(count).isEqualTo(100); // Half of 200
    }

    public interface FastQueryRepository extends MemrisRepository<Product> {
        boolean existsByName(String name);
        
        long countByNameLike(String pattern);
        
        long count();
        
        @Query("select p from Product p where p.sku = :sku")
        boolean fastExistsBySku(@Param("sku") String sku);
        
        @Query("select count(p) from Product p where p.stock > 0")
        long fastCountInStock();
        
        Product save(Product product);
    }
}
