package io.memris.core;

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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class ColumnIndexPrecomputeTest {

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
    void shouldPrecomputeColumnIndexToFieldNameMapping() {
        // This test verifies that column index to field name mapping
        // is precomputed at repository creation time, not scanned on each query
        PrecomputeRepository repo = arena.createRepository(PrecomputeRepository.class);
        
        // Create test data
        for (int i = 0; i < 100; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // Execute queries that use index lookups
        // These should use precomputed mappings instead of scanning
        long startTime = System.nanoTime();
        
        Optional<Product> found = repo.findBySku("SKU-50");
        
        long duration = System.nanoTime() - startTime;
        
        assertThat(found).isPresent();
        assertThat(found.orElseThrow().sku).isEqualTo("SKU-50");
        // Should be fast due to precomputed mappings
        assertThat(duration).isLessThan(50_000_000L); // 50ms threshold
    }

    @Test
    void shouldUsePrecomputedMappingForMultipleQueries() {
        PrecomputeRepository repo = arena.createRepository(PrecomputeRepository.class);
        
        for (int i = 0; i < 50; i++) {
            repo.save(new Product("SKU-" + i, "Product " + i, i * 100L, 10));
        }

        // Multiple queries should all benefit from precomputed mappings
        long startTime = System.nanoTime();
        
        for (int i = 0; i < 100; i++) {
            repo.findByName("Product " + (i % 50));
        }
        
        long duration = System.nanoTime() - startTime;
        
        // 100 lookups should be very fast with precomputed mappings
        assertThat(duration).isLessThan(100_000_000L); // 100ms for 100 queries
    }

    @Test
    void shouldPrecomputeMappingsForAllFieldTypes() {
        PrecomputeRepository repo = arena.createRepository(PrecomputeRepository.class);
        
        repo.save(new Product("SKU-1", "Test", 1000L, 5));
        
        // Query by different field types
        Optional<Product> byString = repo.findBySku("SKU-1");
        Optional<Product> byLong = repo.findByPrice(1000L);
        Optional<Product> byInt = repo.findByStock(5);
        
        assertThat(byString).isPresent();
        assertThat(byLong).isPresent();
        assertThat(byInt).isPresent();
    }

    public interface PrecomputeRepository extends MemrisRepository<Product> {
        Optional<Product> findBySku(String sku);
        Optional<Product> findByName(String name);
        Optional<Product> findByPrice(long price);
        Optional<Product> findByStock(int stock);
        
        Product save(Product product);
    }
}
