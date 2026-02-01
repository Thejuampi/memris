package io.memris.runtime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class FloatTestEntity {
    @Id
    public Long id;
    public float value;

    public FloatTestEntity() {
    }

    public FloatTestEntity(Long id, float value) {
        this.id = id;
        this.value = value;
    }
}
