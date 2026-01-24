package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class DynamicRepositoryTest {

    @Test
    void findByProcessor() {
        try (var factory = new MemrisRepositoryFactory()) {
            ComputerRepository repo = factory.createJPARepository(ComputerRepository.class);

            repo.save(new Computer("Dell", "Intel i7", 16));
            repo.save(new Computer("HP", "Intel i5", 8));
            repo.save(new Computer("Apple", "Intel i7", 16));

            List<Computer> intel7s = repo.findByProcessor("Intel i7");
            assertThat(intel7s).hasSize(2);
            assertThat(intel7s).extracting(c -> c.brand).containsExactlyInAnyOrder("Dell", "Apple");
        }
    }

    @Test
    void findByRamGreaterThan() {
        try (var factory = new MemrisRepositoryFactory()) {
            ComputerRepository repo = factory.createJPARepository(ComputerRepository.class);

            repo.save(new Computer("Dell", "Intel i7", 8));
            repo.save(new Computer("HP", "Intel i5", 16));
            repo.save(new Computer("Apple", "Intel i7", 32));

            //todo assert something here
            List<Computer> all = repo.findAll();

            List<Computer> highRam = repo.findByRamGreaterThan(16);

            // TODO USE THIS for something
            List<Computer> byRam = repo.findByRam(16);

            assertThat(highRam).hasSize(1);
            assertThat(highRam.getFirst().brand).isEqualTo("Apple");
        }
    }

    @Test
    void findByRamLessThanEqual() {
        try (var factory = new MemrisRepositoryFactory()) {
            ComputerRepository repo = factory.createJPARepository(ComputerRepository.class);

            repo.save(new Computer("A", "Intel i7", 8));
            repo.save(new Computer("B", "Intel i5", 16));
            repo.save(new Computer("C", "Intel i7", 32));

            List<Computer> lowRam = repo.findByRamLessThanEqual(16);
            assertThat(lowRam).hasSize(2);
        }
    }

    @Test
    void findByProcessorNotEqual() {
        try (var factory = new MemrisRepositoryFactory()) {
            ComputerRepository repo = factory.createJPARepository(ComputerRepository.class);

            repo.save(new Computer("A", "Intel i7", 8));
            repo.save(new Computer("B", "Intel i5", 16));
            repo.save(new Computer("C", "Intel i7", 32));

            List<Computer> notI5 = repo.findByProcessorNotEqual("Intel i5");
            assertThat(notI5).hasSize(2);
            assertThat(notI5).extracting(c -> c.processor).containsOnly("Intel i7");
        }
    }

    @Test
    void findByBrand() {
        try (var factory = new MemrisRepositoryFactory()) {
            ComputerRepository repo = factory.createJPARepository(ComputerRepository.class);

            repo.save(new Computer("Dell", "Intel i7", 16));
            repo.save(new Computer("Dell", "Intel i5", 8));
            repo.save(new Computer("HP", "Intel i7", 16));

            List<Computer> dellComputers = repo.findByBrand("Dell");
            assertThat(dellComputers).hasSize(2);
        }
    }

    @Entity
    static final class Computer {
        @Id
        int id;
        String brand;
        String processor;
        int ram;

        Computer() {}

        Computer(String brand, String processor, int ram) {
            this.brand = brand;
            this.processor = processor;
            this.ram = ram;
        }
    }

    interface ComputerRepository extends MemrisRepository<Computer> {
        void save(Computer c);
        List<Computer> findByProcessor(String processor);
        List<Computer> findByRamGreaterThan(int ram);
        List<Computer> findByRamLessThanEqual(int ram);
        List<Computer> findByProcessorNotEqual(String processor);
        List<Computer> findByBrand(String brand);
        List<Computer> findAll();
        List<Computer> findByRam(int ram);

        // DO NOT DO THIS
//        List<Computer> findBy(String property, Object value);
    }
}
