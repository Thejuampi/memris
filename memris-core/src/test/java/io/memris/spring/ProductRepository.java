package io.memris.spring;

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

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Product entity.
 */
public interface ProductRepository extends MemrisRepository<Product> {
    
    /**
     * Find product by SKU (Stock Keeping Unit).
     */
    Optional<Product> findBySku(String sku);
    
    /**
     * Find products by name containing keyword (case-sensitive).
     */
    List<Product> findByNameContaining(String keyword);
    
    /**
     * Find products within a price range (in cents).
     */
    List<Product> findByPriceBetween(long min, long max);
    
    /**
     * Find products with stock quantity greater than specified amount.
     */
    List<Product> findByStockGreaterThan(int quantity);
    
    /**
     * Save a product entity.
     */
    Product save(Product product);
    
    /**
     * Find product by ID.
     */
    Optional<Product> findById(Long id);
    
    /**
     * Find all products.
     */
    List<Product> findAll();
}
