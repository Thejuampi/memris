package io.memris.core;

import io.memris.core.Entity;
import io.memris.core.Id;

/**
 * Customer entity for e-commerce domain.
 * 
 * Uses simple field names to avoid case-sensitivity issues in query parsing.
 */
@Entity
public class Customer {
    @Id
    public Long id;
    public String email;
    public String name;
    public String phone;
    public long created; // epoch millis - simple name
    
    // Default constructor
    public Customer() {
        this.created = System.currentTimeMillis();
    }
    
    public Customer(String email, String name, String phone) {
        this.email = email;
        this.name = name;
        this.phone = phone;
        this.created = System.currentTimeMillis();
    }
}
