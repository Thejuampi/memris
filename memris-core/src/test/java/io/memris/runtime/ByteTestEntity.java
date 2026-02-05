package io.memris.runtime;

import io.memris.core.Entity;

@Entity
public class ByteTestEntity {
    public Long id;
    public byte value;

    public ByteTestEntity() {
    }

    public ByteTestEntity(Long id, byte value) {
        this.id = id;
        this.value = value;
    }
}
