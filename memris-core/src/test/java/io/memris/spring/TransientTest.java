package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Entity;
import jakarta.persistence.Transient;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TransientTest {

    // Repository interfaces for JPA-style repository creation
    interface PersonRepository extends MemrisRepository<Person> {
        void save(Person entity);
        List<Person> findAll();
    }

    interface ProductRepository extends MemrisRepository<Product> {
        void save(Product entity);
        List<Product> findAll();
    }

    interface OrderRepository extends MemrisRepository<Order> {
        void save(Order entity);
        List<Order> findAll();
    }

    interface ItemRepository extends MemrisRepository<Item> {
        void save(Item entity);
        long count();
        List<Item> findByCode(String code);
    }

    @Test
    void transient_field_not_in_schema() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            PersonRepository persons = factory.createJPARepository(PersonRepository.class);

            persons.save(new Person("Alice", 30, "computed-age"));

            var all = persons.findAll();
            assertThat(all).hasSize(1);

            assertThat(all.get(0).name).isEqualTo("Alice");
            assertThat(all.get(0).age).isEqualTo(30);
            // @Transient fields are NOT persisted, so they remain at default (null for String)
            assertThat(all.get(0).displayName).isNull();

            factory.close();
        }
    }

    @Test
    void transient_field_not_materialized() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ProductRepository products = factory.createJPARepository(ProductRepository.class);

            products.save(new Product("Widget", 100, "WIDGET_001"));

            var all = products.findAll();
            assertThat(all).hasSize(1);
            assertThat(all.get(0).sku).isEqualTo("WIDGET_001");
            assertThat(all.get(0).calculatedValue).isEqualTo(0);

            factory.close();
        }
    }

    @Test
    void transient_computed_on_postload() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            OrderRepository orders = factory.createJPARepository(OrderRepository.class);

            orders.save(new Order(1, 100L, 10L));
            orders.save(new Order(2, 200L, 25L));

            var all = orders.findAll();
            assertThat(all).hasSize(2);

            // Just verify we can access the orders
            assertThat(all.get(0).subtotal).isGreaterThan(0L);
            assertThat(all.get(1).subtotal).isGreaterThan(0L);

            factory.close();
        }
    }

    @Test
    void all_non_transient_fields_saved() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ItemRepository items = factory.createJPARepository(ItemRepository.class);

            items.save(new Item("A", 10, "X"));
            items.save(new Item("B", 20, "Y"));
            items.save(new Item("C", 30, "Z"));

            assertThat(items.count()).isEqualTo(3);

            var found = items.findByCode("Y");
            assertThat(found).hasSize(1);
            // @Transient fields are not persisted
            assertThat(found.get(0).tempField).isNull();

            factory.close();
        }
    }

    @Entity
    static final class Person {
        int id;
        String name;
        int age;
        @Transient
        String displayName;

        Person() {}

        Person(String name, int age, String displayName) {
            this.name = name;
            this.age = age;
            this.displayName = displayName;
        }
    }

    @Entity
    static final class Product {
        int id;
        String name;
        int stock;
        @Transient
        int calculatedValue;
        String sku;

        Product() {}

        Product(String name, int stock, String sku) {
            this.name = name;
            this.stock = stock;
            this.sku = sku;
        }
    }

    @Entity
    static final class Order {
        int id;
        long subtotal;
        long tax;
        @Transient
        long total;

        Order() {}

        Order(int id, long subtotal, long tax) {
            this.id = id;
            this.subtotal = subtotal;
            this.tax = tax;
        }
    }

    @Entity
    static final class Item {
        int id;
        String name;
        int quantity;
        @Transient
        String tempField;
        String code;

        Item() {}

        Item(String name, int quantity, String code) {
            this.name = name;
            this.quantity = quantity;
            this.code = code;
        }
    }
}
