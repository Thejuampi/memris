package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;

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
