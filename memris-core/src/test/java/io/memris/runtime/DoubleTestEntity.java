package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;

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
