package io.memris.spring.plan.entities;

import jakarta.persistence.*;

/**
 * Address entity for nested property tests.
 */
@Entity
public final class Address {
    @Id
    private Long id;
    private String city;
    private String state;

    public Address() {}

    public Address(Long id, String city, String state) {
        this.id = id;
        this.city = city;
        this.state = state;
    }

    public Long getId() { return id; }
    public String getCity() { return city; }
    public String getState() { return state; }
}
