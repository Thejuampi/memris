package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;

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
