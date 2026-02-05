package io.memris.spring.data.repository;

import io.memris.repository.MemrisRepository;
import org.springframework.data.repository.Repository;

public interface MemrisSpringRepository<T, ID> extends MemrisRepository<T>, Repository<T, ID> {
}
