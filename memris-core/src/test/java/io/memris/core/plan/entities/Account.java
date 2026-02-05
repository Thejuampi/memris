package io.memris.core.plan.entities;

import io.memris.core.Entity;

/**
 * Account entity for nested property tests.
 */
@Entity
public final class Account {
    private Long id;
    private String email;
    private String phone;

    public Account() {}

    public Account(Long id, String email, String phone) {
        this.id = id;
        this.email = email;
        this.phone = phone;
    }

    public Long getId() { return id; }
    public String getEmail() { return email; }
    public String getPhone() { return phone; }
}
