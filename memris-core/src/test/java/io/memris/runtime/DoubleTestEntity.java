package io.memris.runtime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class DoubleTestEntity {
    @Id
    public Long id;
    public double value;

    public DoubleTestEntity() {
    }

    public DoubleTestEntity(Long id, double value) {
        this.id = id;
        this.value = value;
    }
}
