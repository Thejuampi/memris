package io.memris.runtime;

import io.memris.repository.MemrisRepository;
import java.util.List;

public interface ShortTestRepository extends MemrisRepository<ShortTestEntity> {
    ShortTestEntity save(ShortTestEntity entity);

    List<ShortTestEntity> findByValueBetween(short min, short max);
}
