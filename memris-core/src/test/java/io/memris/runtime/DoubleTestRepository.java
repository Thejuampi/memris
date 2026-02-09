package io.memris.runtime;

import io.memris.repository.MemrisRepository;
import java.util.List;

public interface DoubleTestRepository extends MemrisRepository<DoubleTestEntity> {
    DoubleTestEntity save(DoubleTestEntity entity);

    List<DoubleTestEntity> findByValueBetween(double min, double max);

    List<DoubleTestEntity> findByOrderByValueAsc();

    List<DoubleTestEntity> findTop2ByOrderByValueAsc();
}
