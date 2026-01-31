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
 * Repository interface for Order entity.
 */
public interface OrderRepository extends MemrisRepository<Order> {

    /**
     * Find all orders for a specific customer.
     */
    List<Order> findByCustomerId(Long customerId);

    /**
     * Find orders by customer and status.
     */
    List<Order> findByCustomerIdAndStatus(Long customerId, String status);

    /**
     * Find orders within a date range.
     */
    List<Order> findByDateBetween(long start, long end);

    /**
     * Find orders with status and total greater than specified amount.
     */
    List<Order> findByStatusAndTotalGreaterThan(String status, long minTotal);

    /**
     * Count orders by status.
     */
    long countByStatus(String status);

    /**
     * Save an order entity.
     */
    Order save(Order order);

    /**
     * Find order by ID.
     */
    Optional<Order> findById(Long id);

    /**
     * Find all orders.
     */
    List<Order> findAll();
}
