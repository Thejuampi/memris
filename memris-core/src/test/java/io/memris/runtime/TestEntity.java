package io.memris.runtime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class TestEntity {
    @Id
    public Long id;
    public String name;
    public int age;

    public TestEntity() {
    }

    public TestEntity(Long id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }
}
