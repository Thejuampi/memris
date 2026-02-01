package io.memris.runtime;

import io.memris.core.Index;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class IndexedEntity {
    @Id
    public Long id;

    @Index(type = Index.IndexType.HASH)
    public String category;

    public int score;

    public IndexedEntity() {
    }

    public IndexedEntity(Long id, String category, int score) {
        this.id = id;
        this.category = category;
        this.score = score;
    }
}
