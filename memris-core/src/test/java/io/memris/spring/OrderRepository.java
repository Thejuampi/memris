package io.memris.spring;

import java.util.List;
import java.util.Optional;

/**
 * Repository interface for Order entity.
 */
public interface OrderRepository extends MemrisRepository<Order> {
    
    /**
     * Find all orders for a specific customer.
     */
    List<Order> findByCustomer(Long customer);
    
    /**
     * Find orders by customer and status.
     */
    List<Order> findByCustomerAndStatus(Long customer, String status);
    
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
