package io.memris.runtime;

import io.memris.repository.MemrisRepository;

import java.util.List;

public interface IndexedEntityRepository extends MemrisRepository<IndexedEntity> {
    IndexedEntity save(IndexedEntity entity);

    void delete(IndexedEntity entity);

    List<IndexedEntity> findByCategory(String category);

    long countByCategory(String category);
}
