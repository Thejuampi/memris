package io.memris.runtime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class ByteTestEntity {
    @Id
    public Long id;
    public byte value;

    public ByteTestEntity() {
    }

    public ByteTestEntity(Long id, byte value) {
        this.id = id;
        this.value = value;
    }
}
