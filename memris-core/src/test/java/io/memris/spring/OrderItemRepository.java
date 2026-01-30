package io.memris.spring;

import java.util.List;

/**
 * Repository interface for OrderItem entity.
 */
public interface OrderItemRepository extends MemrisRepository<OrderItem> {
    
    /**
     * Find all items in a specific order.
     */
    List<OrderItem> findByOrder(Long order);
    
    /**
     * Find all order items for a specific product.
     */
    List<OrderItem> findByProduct(Long product);
    
    /**
     * Find order items with quantity greater than specified amount.
     */
    List<OrderItem> findByQtyGreaterThan(int minQty);
    
    /**
     * Save an order item entity.
     */
    OrderItem save(OrderItem orderItem);
    
    /**
     * Find all order items.
     */
    List<OrderItem> findAll();
}
