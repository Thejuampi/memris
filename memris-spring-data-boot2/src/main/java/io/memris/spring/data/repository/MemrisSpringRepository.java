package io.memris.spring.data.repository;

import io.memris.repository.MemrisRepository;
import org.springframework.data.repository.Repository;

/**
 * Base marker repository for Memris-backed Spring Data repositories.
 *
 * @param <T> aggregate type
 * @param <ID> identifier type
 */
public interface MemrisSpringRepository<T, ID> extends MemrisRepository<T>, Repository<T, ID> {
}
