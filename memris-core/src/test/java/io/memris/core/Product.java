package io.memris.core;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;
import io.memris.core.Id;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;

/**
 * Product entity for e-commerce domain.
 * 
 * Uses simple field names to avoid case-sensitivity issues in query parsing.
 * Prices are stored in cents (e.g., $19.99 = 1999).
 */
public class Product {
    @Id
    public Long id;
    public String sku;
    public String name;
    public long price; // Stored in cents - simple name
    public int stock; // Simple name
    
    // Default constructor
    public Product() {}
    
    public Product(String sku, String name, long price, int stock) {
        this.sku = sku;
        this.name = name;
        this.price = price;
        this.stock = stock;
    }
    
    /**
     * Helper method to convert cents to dollars for display
     */
    public double getPriceDollars() {
        return price / 100.0;
    }
}
