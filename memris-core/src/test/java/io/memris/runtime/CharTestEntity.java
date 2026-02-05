package io.memris.runtime;

import io.memris.core.Entity;

@Entity
public class CharTestEntity {
    public Long id;
    public char value;

    public CharTestEntity() {
    }

    public CharTestEntity(Long id, char value) {
        this.id = id;
        this.value = value;
    }
}
