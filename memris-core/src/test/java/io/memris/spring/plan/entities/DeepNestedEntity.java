package io.memris.spring.plan.entities;

import jakarta.persistence.*;

/**
 * Deeply nested entity for multi-level property resolution tests.
 */
@Entity
public final class DeepNestedEntity {
    @Id
    private Long id;
    private String name;

    @ManyToOne
    private Department department;

    @ManyToOne
    private Account account;

    public DeepNestedEntity() {}

    public DeepNestedEntity(Long id, String name, Department department, Account account) {
        this.id = id;
        this.name = name;
        this.department = department;
        this.account = account;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public Department getDepartment() { return department; }
    public Account getAccount() { return account; }
}
