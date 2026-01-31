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

/**
 * OrderItem entity for e-commerce domain.
 * Represents a line item in an order.
 * 
 * Uses simple field names to avoid case-sensitivity issues in query parsing.
 */
public class OrderItem {
    public Long id;
    public long order; // Foreign key to Order - simple name
    public long product; // Foreign key to Product - simple name
    public int qty; // Quantity - simple name
    public long price; // Unit price in cents - simple name
    
    // Default constructor
    public OrderItem() {}
    
    public OrderItem(long order, long product, int qty, long price) {
        this.order = order;
        this.product = product;
        this.qty = qty;
        this.price = price;
    }
    
    /**
     * Calculate subtotal for this line item
     */
    public long getSubtotal() {
        return price * qty;
    }
    
    /**
     * Helper method to convert cents to dollars for display
     */
    public double getPriceDollars() {
        return price / 100.0;
    }
}
