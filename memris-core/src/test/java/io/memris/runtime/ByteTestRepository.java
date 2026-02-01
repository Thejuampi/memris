package io.memris.runtime;

import io.memris.repository.MemrisRepository;
import java.util.List;

public interface ByteTestRepository extends MemrisRepository<ByteTestEntity> {
    ByteTestEntity save(ByteTestEntity entity);

    List<ByteTestEntity> findByValueBetween(byte min, byte max);
}
