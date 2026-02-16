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
