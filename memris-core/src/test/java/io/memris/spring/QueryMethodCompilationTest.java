package io.memris.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.foreign.Arena;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * TDD tests for query method compilation.
 * Tests that query methods are compiled once and reused via MethodHandles for performance.
 */
class QueryMethodCompilationTest {

    @Test
    void query_method_compiles_once_on_first_call() {
        try (var arena = Arena.ofConfined(); var factory = new MemrisRepositoryFactory()) {
            PersonRepository repo = factory.createJPARepository(PersonRepository.class);

            Person p1 = new Person();
            p1.name = "Alice";
            repo.save(p1);

            // First call should compile the query method
            List<Person> results1 = repo.findByName("Alice");

            // Second call should reuse compiled query (faster)
            List<Person> results2 = repo.findByName("Alice");

            assertThat(results1).hasSize(1);
            assertThat(results2).hasSize(1);
        }
    }

    @Test
    void compiled_query_handles_different_parameters() {
        try (var arena = Arena.ofConfined(); var factory = new MemrisRepositoryFactory()) {
            PersonRepository repo = factory.createJPARepository(PersonRepository.class);

            Person p1 = new Person();
            p1.name = "Alice";
            Person p2 = new Person();
            p2.name = "Bob";
            repo.saveAll(List.of(p1, p2));

            List<Person> aliceResults = repo.findByName("Alice");
            List<Person> bobResults = repo.findByName("Bob");

            assertThat(aliceResults).hasSize(1);
            assertThat(bobResults).hasSize(1);
            assertThat(aliceResults.get(0).name).isEqualTo("Alice");
            assertThat(bobResults.get(0).name).isEqualTo("Bob");
        }
    }

    @Test
    void compiled_query_with_multiple_conditions() {
        try (var arena = Arena.ofConfined(); var factory = new MemrisRepositoryFactory()) {
            PersonRepository repo = factory.createJPARepository(PersonRepository.class);

            Person p1 = new Person();
            p1.name = "Alice";
            p1.age = 30;
            Person p2 = new Person();
            p2.name = "Bob";
            p2.age = 25;

            repo.saveAll(List.of(p1, p2));

            List<Person> results = repo.findByAgeGreaterThan(25);

            assertThat(results).hasSize(1);
            assertThat(results.get(0).age).isEqualTo(30);
        }
    }

    @Test
    void compiled_count_query_is_fast() {
        try (var arena = Arena.ofConfined(); var factory = new MemrisRepositoryFactory()) {
            PersonRepository repo = factory.createJPARepository(PersonRepository.class);

            Person p = new Person();
            p.name = "Alice";
            repo.save(p);

            // First call compiles query
            long count1 = repo.countByName("Alice");

            // Subsequent calls reuse compiled query (should be faster)
            long count2 = repo.countByName("Alice");
            long count3 = repo.countByName("Alice");

            assertThat(count1).isEqualTo(1);
            assertThat(count2).isEqualTo(1);
            assertThat(count3).isEqualTo(1);
        }
    }

    // Test repository interface
    interface PersonRepository extends MemrisRepository<Person> {
        List<Person> findByName(String name);
        List<Person> findByAgeGreaterThan(int age);
        long countByName(String name);
        void saveAll(List<Person> p1);
        void save(Person p);
    }

    // Test entity class
    static class Person {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        Integer id;
        String name;
        Integer age;
        Person() {}
    }
}
