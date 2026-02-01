package io.memris.runtime;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

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
