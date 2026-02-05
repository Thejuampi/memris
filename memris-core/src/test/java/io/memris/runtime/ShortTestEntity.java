package io.memris.runtime;

import io.memris.core.Entity;

@Entity
public class ShortTestEntity {
    public Long id;
    public short value;

    public ShortTestEntity() {
    }

    public ShortTestEntity(Long id, short value) {
        this.id = id;
        this.value = value;
    }
}
