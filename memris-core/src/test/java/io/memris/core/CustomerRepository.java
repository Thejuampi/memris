package io.memris.core;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;
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
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;

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

    /**
     * Delete customer by ID.
     */
    void deleteById(Long id);

    /**
     * Find customers by name not equal to given name.
     */
    List<Customer> findByNameNot(String name);

    List<Customer> queryByEmail(String email);

    Optional<Customer> findByEmailOrderByNameAsc(String email);

    Optional<Customer> findByEmailOrderByNameDesc(String email);

    List<Customer> findByNameNotNullAndNameNot(String name);

    List<Customer> findByNameLikeIgnoreCase(String pattern);

    List<Customer> findByNameStartingWithIgnoreCase(String prefix);

    List<Customer> findByNameEndingWithIgnoreCase(String suffix);

    List<Customer> findByNameIgnoreCase(String name);

    List<Customer> findByNameEqualsIgnoreCase(String name);

    List<Customer> findByNameIsIgnoreCase(String name);

    List<Customer> findByNameAndEmailAllIgnoreCaseOrderByCreatedAsc(String name, String email);

    List<Customer> findByNameOrEmailAllIgnoreCase(String name, String email);

    List<Customer> findByNameIsNotNull();

    long removeByEmailIgnoreCase(String email);
}
