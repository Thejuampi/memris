package io.memris.core.plan.entities;

import io.memris.core.Entity;
import io.memris.core.Id;

/**
 * Entity with embedded value object.
 */
@Entity
public final class EmbeddedEntity {
    @Id
    private Long id;
    private String username;

    private EmbeddedValueObject profile;

    public EmbeddedEntity() {}

    public EmbeddedEntity(Long id, String username, EmbeddedValueObject profile) {
        this.id = id;
        this.username = username;
        this.profile = profile;
    }

    public Long getId() { return id; }
    public String getUsername() { return username; }
    public EmbeddedValueObject getProfile() { return profile; }
}
