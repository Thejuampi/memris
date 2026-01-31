package io.memris.core;

import io.memris.core.ManyToOne;

/**
 * Order entity for projection tests and e-commerce tests.
 */
public class Order {
    public Long id;
    public long total;
    public String status;
    public long date;

    /**
     * Customer ID for e-commerce tests that don't use relationships.
     */
    public Long customerId;

    @ManyToOne
    public Customer customer;

    public Order() {
    }

    public Order(long total, Customer customer) {
        this.total = total;
        this.customer = customer;
        this.customerId = customer != null ? customer.id : null;
    }

    /**
     * Constructor for e-commerce tests that use customer ID directly.
     */
    public Order(Long customerId, String status, int total) {
        this.customerId = customerId;
        this.total = total;
        this.status = status;
    }
}
