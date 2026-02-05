package io.memris.core.plan.entities;

import io.memris.core.Entity;
import io.memris.core.ManyToOne;

/**
 * Self-referential entity for recursive relationship tests.
 */
@Entity
public final class SelfReferentialEntity {
    private Long id;
    private String name;

    @ManyToOne
    private SelfReferentialEntity parent;

    public SelfReferentialEntity() {}

    public SelfReferentialEntity(Long id, String name, SelfReferentialEntity parent) {
        this.id = id;
        this.name = name;
        this.parent = parent;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public SelfReferentialEntity getParent() { return parent; }
}
