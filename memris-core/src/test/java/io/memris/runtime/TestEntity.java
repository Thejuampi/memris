package io.memris.runtime;

import io.memris.core.Index;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class TestEntity {
    @Id
    public Long id;
    
    @Index(type = Index.IndexType.PREFIX)
    public String name;
    
    public int age;
    public String department;

    public TestEntity() {
    }

    public TestEntity(Long id, String name, int age) {
        this.id = id;
        this.name = name;
        this.age = age;
    }

    public TestEntity(Long id, String name, int age, String department) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.department = department;
    }
}
