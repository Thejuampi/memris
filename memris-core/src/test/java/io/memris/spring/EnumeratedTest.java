package io.memris.spring;

import jakarta.persistence.Entity;
import jakarta.persistence.Enumerated;
import jakarta.persistence.EnumType;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EnumeratedTest {

    // Repository interfaces for JPA-style repository creation
    interface OrderWithStatusRepository extends MemrisRepository<OrderWithStatus> {
        void save(OrderWithStatus entity);
        long count();
        List<OrderWithStatus> findByStatus(String status);
        List<OrderWithStatus> findAll();
    }

    interface OrderWithPriorityRepository extends MemrisRepository<OrderWithPriority> {
        void save(OrderWithPriority entity);
        long count();
        List<OrderWithPriority> findByPriority(int priority);
        List<OrderWithPriority> findAll();
    }

    @Test
    void enumerated_string_value() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            OrderWithStatusRepository orders = factory.createJPARepository(OrderWithStatusRepository.class);

            orders.save(new OrderWithStatus(OrderStatus.PENDING));
            orders.save(new OrderWithStatus(OrderStatus.SHIPPED));
            orders.save(new OrderWithStatus(OrderStatus.DELIVERED));

            assertThat(orders.count()).isEqualTo(3);
//NOT EXPECTED, USE orders.findByStatus("PENDING")
            OrderWithStatus found = orders.findByStatus("PENDING").get(0);
            assertThat(found.status).isEqualTo(OrderStatus.PENDING);

            factory.close();
        }
    }

    @Test
    void enumerated_ordinal_value() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            OrderWithPriorityRepository orders = factory.createJPARepository(OrderWithPriorityRepository.class);

            orders.save(new OrderWithPriority(1, Priority.LOW));
            orders.save(new OrderWithPriority(2, Priority.MEDIUM));
            orders.save(new OrderWithPriority(3, Priority.HIGH));

            assertThat(orders.count()).isEqualTo(3);

//            USE orders.findByPriority(1)
            OrderWithPriority found = orders.findByPriority(1).get(0);
            assertThat(found.priority).isEqualTo(Priority.MEDIUM);

            factory.close();
        }
    }

    @Test
    void enumerated_null_enum() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            OrderWithStatusRepository orders = factory.createJPARepository(OrderWithStatusRepository.class);

            orders.save(new OrderWithStatus(null));

            assertThat(orders.count()).isEqualTo(1);

            OrderWithStatus found = orders.findAll().getFirst();
            assertThat(found.status).isNull();

            factory.close();
        }
    }

    @Test
    void enumerated_find_by_ordinal() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            OrderWithPriorityRepository orders = factory.createJPARepository(OrderWithPriorityRepository.class);

            orders.save(new OrderWithPriority(1, Priority.HIGH));
            orders.save(new OrderWithPriority(2, Priority.LOW));
            orders.save(new OrderWithPriority(3, Priority.MEDIUM));

//            USE orders.findByPriority(2)
            var highPriority = orders.findByPriority(2);
            assertThat(highPriority).hasSize(1);
            assertThat(highPriority.get(0).priority).isEqualTo(Priority.HIGH);

            factory.close();
        }
    }

    enum OrderStatus {
        PENDING,
        SHIPPED,
        DELIVERED
    }

    enum Priority {
        LOW,
        MEDIUM,
        HIGH
    }

    @Entity
    static final class OrderWithStatus {
        int id;
        @Enumerated(EnumType.STRING)
        OrderStatus status;

        OrderWithStatus() {}

        OrderWithStatus(OrderStatus status) {
            this.status = status;
        }
    }

    @Entity
    static final class OrderWithPriority {
        int id;
        @Enumerated(EnumType.ORDINAL)
        Priority priority;

        OrderWithPriority() {}

        OrderWithPriority(int id, Priority priority) {
            this.id = id;
            this.priority = priority;
        }
    }
}
