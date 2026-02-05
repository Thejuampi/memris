package io.memris.core.plan.entities;

import io.memris.core.Entity;

import java.time.LocalDate;

/**
 * Simple entity for basic property resolution tests.
 */
@Entity
public final class SimpleEntity {
    private Long id;

    private String name;
    private int age;
    private boolean active;
    private LocalDate birthday;

    public SimpleEntity() {}

    public SimpleEntity(Long id, String name, int age, boolean active, LocalDate birthday) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.active = active;
        this.birthday = birthday;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public int getAge() { return age; }
    public boolean getActive() { return active; }
    public LocalDate getBirthday() { return birthday; }
}
