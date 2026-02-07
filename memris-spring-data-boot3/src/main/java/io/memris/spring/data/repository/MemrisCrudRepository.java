package io.memris.spring.data.repository;

import io.memris.repository.MemrisRepository;
import org.springframework.data.repository.CrudRepository;

/**
 * Convenience repository combining Memris operations with Spring CRUD semantics.
 *
 * @param <T> aggregate type
 * @param <ID> identifier type
 */
public interface MemrisCrudRepository<T, ID> extends MemrisRepository<T>, CrudRepository<T, ID> {
}
