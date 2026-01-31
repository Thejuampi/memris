package io.memris.core;

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

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryMethodDispatchTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        System.setProperty("memris.fail.method.lookup", "true");
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        System.clearProperty("memris.fail.method.lookup");
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldDispatchRepositoryMethodsByIndex() {
        DispatchProductRepository repo = arena.createRepository(DispatchProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 10));

        Optional<Product> found = repo.findBySku("SKU-1");

        assertThat(found).isPresent();
        assertThat(found.orElseThrow().sku).isEqualTo("SKU-1");
    }

    public interface DispatchProductRepository extends MemrisRepository<Product> {
        Product save(Product product);

        Optional<Product> findBySku(String sku);
    }
}
