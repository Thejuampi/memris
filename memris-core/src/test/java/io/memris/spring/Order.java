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
 * Order entity for e-commerce domain.
 * 
 * Uses simple field names to avoid case-sensitivity issues in query parsing.
 */
public class Order {
    public Long id;
    public long customer; // Foreign key to Customer - simple name
    public long date; // epoch millis - simple name
    public String status; // "PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"
    public long total; // Total in cents - simple name
    
    // Default constructor
    public Order() {
        this.date = System.currentTimeMillis();
    }
    
    public Order(long customer, String status, long total) {
        this.customer = customer;
        this.date = System.currentTimeMillis();
        this.status = status;
        this.total = total;
    }
    
    /**
     * Helper method to convert cents to dollars for display
     */
    public double getTotalDollars() {
        return total / 100.0;
    }
}
