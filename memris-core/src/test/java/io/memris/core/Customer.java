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

/**
 * Customer entity for e-commerce domain.
 * 
 * Uses simple field names to avoid case-sensitivity issues in query parsing.
 */
public class Customer {
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
