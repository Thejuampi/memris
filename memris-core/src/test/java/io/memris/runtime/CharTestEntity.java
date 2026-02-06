package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.Id;

@Entity
public class CharTestEntity {
    @Id
    public Long id;
    public char value;

    public CharTestEntity() {
    }

    public CharTestEntity(Long id, char value) {
        this.id = id;
        this.value = value;
    }
}
