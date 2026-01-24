package io.memris.spring;

import io.memris.storage.ffm.FfmTable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MemrisRepositoryFactoryTest {

    @Test
    void save_and_findBy_should_work() {
        try (var factory = new MemrisRepositoryFactory()) {
            UserRepository repo = factory.createJPARepository(UserRepository.class);

            User user1 = new User(1, "Alice", 30);
            User user2 = new User(2, "Bob", 25);
            User user3 = new User(3, "Charlie", 30);

            repo.save(user1);
            repo.save(user2);
            repo.save(user3);

            List<User> thirties = repo.findByAge(30);
            assertThat(thirties).hasSize(2);

            List<User> all = repo.findAll();
            assertThat(all).hasSize(3);

            assertThat(repo.count()).isEqualTo(3);
        }
    }

    @Test
    void findByIn_should_return_matching_rows() {
        try (var factory = new MemrisRepositoryFactory()) {
            UserRepository repo = factory.createJPARepository(UserRepository.class);

            for (int i = 0; i < 10; i++) {
                repo.save(new User(i, "User" + i, i % 3));
            }

            List<User> result = repo.findByIn("age", List.of(0, 2));
            assertThat(result).hasSize(7);
        }
    }

    @Test
    void findByBetween_should_return_rows_in_range() {
        try (var factory = new MemrisRepositoryFactory()) {
            UserRepository repo = factory.createJPARepository(UserRepository.class);

            for (int i = 0; i < 100; i++) {
                repo.save(new User(i, "User" + i, i));
            }

            List<User> result = repo.findByBetween("age", 20, 30);
            assertThat(result).hasSize(11);
        }
    }

    static class User {
        int id;
        String name;
        int age;

        User() {}

        User(int id, String name, int age) {
            this.id = id;
            this.name = name;
            this.age = age;
        }
    }

    interface UserRepository extends MemrisRepository<User> {
        void save(User u);
        List<User> findByAge(int age);
        List<User> findAll();
        long count();
        List<User> findByIn(String property, List<Integer> values);
        List<User> findByBetween(String property, int from, int to);
    }
}
