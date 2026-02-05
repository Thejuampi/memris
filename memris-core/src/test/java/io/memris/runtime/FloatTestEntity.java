package io.memris.runtime;

import io.memris.core.Entity;

@Entity
public class FloatTestEntity {
    public Long id;
    public float value;

    public FloatTestEntity() {
    }

    public FloatTestEntity(Long id, float value) {
        this.id = id;
        this.value = value;
    }
}
