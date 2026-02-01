package io.memris.runtime;

import io.memris.repository.MemrisRepository;
import java.util.List;

public interface CharTestRepository extends MemrisRepository<CharTestEntity> {
    CharTestEntity save(CharTestEntity entity);

    List<CharTestEntity> findByValueBetween(char min, char max);
}
