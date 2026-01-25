package io.memris.spring.plan.entities;

import jakarta.persistence.*;

/**
 * Nested entity for joined property resolution tests.
 */
@Entity
public final class NestedEntity {
    @Id
    private Long id;

    private String name;

    @ManyToOne
    private Department department;

    @ManyToOne
    private Address address;

    @ManyToOne
    private Account account;

    public NestedEntity() {}

    public NestedEntity(Long id, String name, Department department, Address address, Account account) {
        this.id = id;
        this.name = name;
        this.department = department;
        this.address = address;
        this.account = account;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public Department getDepartment() { return department; }
    public Address getAddress() { return address; }
    public Account getAccount() { return account; }
}
