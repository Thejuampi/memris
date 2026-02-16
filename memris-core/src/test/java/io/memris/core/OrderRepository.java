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

    /**
     * Delete order by ID.
     */
    void deleteById(Long id);

    /**
     * Find orders by status, ordered by total descending.
     */
    List<Order> findByStatusOrderByTotalDesc(String status);

    /**
     * Find top 3 orders by status, ordered by ID ascending.
     */
    List<Order> findTop3ByStatusOrderByIdAsc(String status);

    /**
     * Find first order by customer ID.
     */
    List<Order> findFirstByCustomerId(Long customerId);

    /**
     * Find orders with status in list.
     */
    List<Order> findByStatusIn(List<String> statuses);

    /**
     * Find orders by status and total greater than or equal to.
     */
    List<Order> findByStatusAndTotalGreaterThanEqual(String status, long minTotal);
}
