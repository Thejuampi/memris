package io.memris.spring;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Customer entity.
 */
public interface CustomerRepository extends MemrisRepository<Customer> {
    
    /**
     * Find customer by email address.
     */
    Optional<Customer> findByEmail(String email);
    
    /**
     * Find customers by name (case-insensitive, partial match).
     */
    List<Customer> findByNameContainingIgnoreCase(String name);
    
    /**
     * Find customers created after a specific timestamp.
     */
    List<Customer> findByCreatedGreaterThan(long date);
    
    /**
     * Check if a customer with the given email exists.
     */
    boolean existsByEmail(String email);
    
    /**
     * Save a customer entity.
     */
    Customer save(Customer customer);
    
    /**
     * Find customer by ID.
     */
    Optional<Customer> findById(Long id);
    
    /**
     * Find all customers.
     */
    List<Customer> findAll();
}
