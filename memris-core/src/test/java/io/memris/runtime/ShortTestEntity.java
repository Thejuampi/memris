package io.memris.runtime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class ShortTestEntity {
    @Id
    public Long id;
    public short value;

    public ShortTestEntity() {
    }

    public ShortTestEntity(Long id, short value) {
        this.id = id;
        this.value = value;
    }
}
