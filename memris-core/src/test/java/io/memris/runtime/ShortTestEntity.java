package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;

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
