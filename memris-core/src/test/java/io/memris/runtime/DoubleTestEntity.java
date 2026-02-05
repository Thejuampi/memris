package io.memris.runtime;

import io.memris.core.Entity;

@Entity
public class DoubleTestEntity {
    public Long id;
    public double value;

    public DoubleTestEntity() {
    }

    public DoubleTestEntity(Long id, double value) {
        this.id = id;
        this.value = value;
    }
}
