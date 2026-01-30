package io.memris.spring;

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
