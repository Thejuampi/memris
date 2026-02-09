package io.memris.runtime;

import io.memris.repository.MemrisRepository;
import java.util.List;

public interface FloatTestRepository extends MemrisRepository<FloatTestEntity> {
    FloatTestEntity save(FloatTestEntity entity);

    List<FloatTestEntity> findByValueBetween(float min, float max);

    List<FloatTestEntity> findByOrderByValueAsc();

    List<FloatTestEntity> findTop2ByOrderByValueAsc();
}
