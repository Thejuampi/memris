package io.memris.spring.data.repository;

import io.memris.repository.MemrisRepository;
import org.springframework.data.repository.CrudRepository;

public interface MemrisCrudRepository<T, ID> extends MemrisRepository<T>, CrudRepository<T, ID> {
}
