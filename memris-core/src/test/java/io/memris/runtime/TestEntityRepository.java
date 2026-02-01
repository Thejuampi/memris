package io.memris.runtime;

import io.memris.repository.MemrisRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface TestEntityRepository extends MemrisRepository<TestEntity> {
    
    TestEntity save(TestEntity entity);
    
    List<TestEntity> saveAll(Iterable<TestEntity> entities);
    
    Optional<TestEntity> findById(Long id);
    
    List<TestEntity> findAll();
    
    boolean existsById(Long id);
    
    long count();
    
    void deleteById(Long id);
    
    void delete(TestEntity entity);
    
    void deleteAll();
    
    long deleteAllById(Iterable<Long> ids);
    
    // Query methods
    List<TestEntity> findByName(String name);

    List<TestEntity> findByNameIn(List<String> names);

    List<TestEntity> findByNameIn(String[] names);
    
    List<TestEntity> findByAgeGreaterThan(int age);
    
    List<TestEntity> findByAgeBetween(int min, int max);
    
    long countByName(String name);
    
    boolean existsByName(String name);
    
    List<TestEntity> findByNameAndAge(String name, int age);
    
    List<TestEntity> findByNameOrAge(String name, int age);

    List<TestEntity> findByOrderByAgeAsc();

    
    List<TestEntity> findTop2ByOrderByAgeAsc();

    Set<TestEntity> findByIdIn(Set<Long> ids);

    Set<TestEntity> findByIdIn(long... ids);
}
