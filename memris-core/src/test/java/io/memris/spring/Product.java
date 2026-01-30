package io.memris.spring;

/**
 * Product entity for e-commerce domain.
 * 
 * Uses simple field names to avoid case-sensitivity issues in query parsing.
 * Prices are stored in cents (e.g., $19.99 = 1999).
 */
public class Product {
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
