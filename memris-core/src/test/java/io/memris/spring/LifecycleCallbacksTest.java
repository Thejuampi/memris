package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PreUpdate;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LifecycleCallbacksTest {

    // Repository interfaces for JPA-style repository creation
    interface UserRepository extends MemrisRepository<User> {
        void save(User entity);
        List<User> findAll();
    }

    interface ProductRepository extends MemrisRepository<Product> {
        void save(Product entity);
        List<Product> findAll();
    }

    interface ItemRepository extends MemrisRepository<Item> {
        void update(Item entity);
    }

    interface RecordRepository extends MemrisRepository<Record> {
        void save(Record entity);
        List<Record> findAll();
    }

    @Test
    void prepersist_callback_invoked() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            UserRepository users = factory.createJPARepository(UserRepository.class);

            User user = new User("alice");
            users.save(user);

            var all = users.findAll();
            assertThat(all).hasSize(1);
            assertThat(all.get(0).passwordHash).isEqualTo("hashed:alice");

            factory.close();
        }
    }

    @Test
    void postload_callback_invoked() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ProductRepository products = factory.createJPARepository(ProductRepository.class);

            products.save(new Product("Widget", 100));
            products.save(new Product("Gadget", 200));

            var all = products.findAll();
            assertThat(all).hasSize(2);

            Product p1 = all.stream().filter(p -> p.name.equals("Widget")).findFirst().get();
            assertThat(p1.displayName).isEqualTo("Product: Widget");

            Product p2 = all.stream().filter(p -> p.name.equals("Gadget")).findFirst().get();
            assertThat(p2.displayName).isEqualTo("Product: Gadget");

            factory.close();
        }
    }

    @Test
    void preupdate_callback_invoked() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ItemRepository items = factory.createJPARepository(ItemRepository.class);

            Item item = new Item("test");
            items.update(item);

            assertThat(item.slug).isEqualTo("test");

            factory.close();
        }
    }

    @Test
    void multiple_callbacks_invoked() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            RecordRepository records = factory.createJPARepository(RecordRepository.class);

            Record record = new Record("test");
            records.save(record);

            var all = records.findAll();
            assertThat(all).hasSize(1);
            assertThat(all.get(0).computed).isEqualTo("COMPUTED:test");

            factory.close();
        }
    }

    @Entity
    static final class User {
        @Id
        int id;
        String username;
        String passwordHash;
        String rawPassword;

        User() {}

        User(String username) {
            this.username = username;
        }

        @PrePersist
        void hashPassword() {
            this.passwordHash = "hashed:" + username;
        }
    }

    @Entity
    static final class Product {
        @Id
        int id;
        String name;
        int stock;
        String displayName;

        Product() {}

        Product(String name, int stock) {
            this.name = name;
            this.stock = stock;
        }

        @PostLoad
        void computeDisplayName() {
            this.displayName = "Product: " + name;
        }
    }

    @Entity
    static final class Item {
        @Id
        int id;
        String name;
        String slug;

        Item() {}

        Item(String name) {
            this.name = name;
        }

        @PreUpdate
        void generateSlug() {
            this.slug = name.toLowerCase().replace(' ', '-');
        }
    }

    @Entity
    static final class Record {
        @Id
        int id;
        String name;
        String computed;

        Record() {}

        Record(String name) {
            this.name = name;
        }

        @PrePersist
        void prepersist() {
            // Does nothing, just for callback registration
        }

        @PostLoad
        void postload() {
            this.computed = "COMPUTED:" + name;
        }
    }
}
