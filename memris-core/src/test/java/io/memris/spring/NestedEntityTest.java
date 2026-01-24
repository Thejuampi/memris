package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;

import static org.assertj.core.api.Assertions.assertThat;

class NestedEntityTest {

    @Test
    void onetoone_nested_cascade_save() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ComputerRepository computers = factory.createJPARepository(ComputerRepository.class);
            ProcessorRepository processors = factory.createJPARepository(ProcessorRepository.class);
            MemoryRepository memories = factory.createJPARepository(MemoryRepository.class);
            MotherBoardRepository motherboards = factory.createJPARepository(MotherBoardRepository.class);

            Computer computer = new Computer();
            computer.processor = new Processor("Intel i9");
            computer.memory = new Memory("64GB");
            computer.motherboard = new MotherBoard("ASUS ROG");

            computers.save(computer);

            assertThat(computers.count()).isEqualTo(1);
            assertThat(processors.count()).isEqualTo(1);
            assertThat(memories.count()).isEqualTo(1);
            assertThat(motherboards.count()).isEqualTo(1);

            factory.close();
        }
    }

    @Entity
    static final class Computer {
        @Id
        int id;
        String model;
        @OneToOne
        Processor processor;
        @OneToOne
        Memory memory;
        @OneToOne
        MotherBoard motherboard;

        Computer() {}
    }

    interface ComputerRepository extends MemrisRepository<Computer> {
        void save(Computer c);
        long count();
    }

    @Entity
    static final class Processor {
        @Id
        int id;
        String name;

        Processor() {}

        Processor(String name) {
            this.name = name;
        }
    }

    interface ProcessorRepository extends MemrisRepository<Processor> {
        long count();
    }

    @Entity
    static final class Memory {
        @Id
        int id;
        String capacity;

        Memory() {}

        Memory(String capacity) {
            this.capacity = capacity;
        }
    }

    interface MemoryRepository extends MemrisRepository<Memory> {
        long count();
    }

    @Entity
    static final class MotherBoard {
        @Id
        int id;
        String model;

        MotherBoard() {}

        MotherBoard(String model) {
            this.model = model;
        }
    }

    interface MotherBoardRepository extends MemrisRepository<MotherBoard> {
        long count();
    }
}
