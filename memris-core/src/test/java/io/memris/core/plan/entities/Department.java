package io.memris.core.plan.entities;

import jakarta.persistence.*;

/**
 * Department entity for nested property tests.
 */
@Entity
public final class Department {
    @Id
    private Long id;
    private String name;

    @ManyToOne
    private Address address;

    public Department() {}

    public Department(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Department(Long id, String name, Address address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public Address getAddress() { return address; }
}
