package io.memris.spring;

import io.memris.spring.JoinResult;
import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import io.memris.storage.ffm.FfmTable;
import io.memris.storage.ffm.FfmTable.ColumnSpec;
import jakarta.persistence.Entity;
import jakarta.persistence.OneToOne;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MemrisRepositoryIntegrationTest {

    // Repository interfaces for JPA-style repository creation
    interface OrderRepository extends MemrisRepository<Order> {
        void save(Order entity);
        long count();
        List<Order> findByStatus(String status);
        List<Order> findByBetween(String property, Object min, Object max);
        List<Order> findByIn(String property, List<?> values);
        List<Order> findAll();
    }

    interface ItemRepository extends MemrisRepository<Item> {
        void save(Item entity);
        List<Item> findByCategory(String category);
    }

    interface ProductRepository extends MemrisRepository<Product> {
        void save(Product entity);
        List<Product> findByIn(String property, List<?> values);
    }

    interface CustomerRepository extends MemrisRepository<Customer> {
        void save(Customer entity);
    }

    interface PersonRepository extends MemrisRepository<Person> {
        void save(Person entity);
        List<Person> findAll();
    }

    interface AddressRepository extends MemrisRepository<Address> {
        List<Address> findAll();
        List<Address> findByCity(String city);
    }

    interface SimpleEntityRepository extends MemrisRepository<SimpleEntity> {
        void save(SimpleEntity entity);
        List<SimpleEntity> findAll();
    }

    @Test
    void full_crud_lifecycle() {
        try (Arena arena = Arena.ofConfined()) {
            FfmTable table = new FfmTable("orders", arena, List.of(
                    new ColumnSpec("id", int.class),
                    new ColumnSpec("customer_id", int.class),
                    new ColumnSpec("amount", long.class),
                    new ColumnSpec("status", String.class)
            ));
            
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            OrderRepository repo = factory.createJPARepository(OrderRepository.class);
            
            // Create
            Order o1 = new Order(1, 100, 5000L, "pending");
            Order o2 = new Order(2, 101, 3000L, "completed");
            Order o3 = new Order(3, 100, 7000L, "pending");
            Order o4 = new Order(4, 102, 1000L, "completed");
            Order o5 = new Order(5, 101, 8000L, "pending");
            
            repo.save(o1);
            repo.save(o2);
            repo.save(o3);
            repo.save(o4);
            repo.save(o5);
            
            assertThat(repo.count()).isEqualTo(5);
            
            // Read by property
            List<Order> pendingOrders = repo.findByStatus("pending");
            assertThat(pendingOrders).hasSize(3);
            assertThat(pendingOrders).allMatch(o -> "pending".equals(o.status));
            
            // Read by ID range
            List<Order> highValue = repo.findByBetween("amount", 5000L, 10000L);
            assertThat(highValue).hasSize(3);
            
            // Read by customer
            List<Order> customer100 = repo.findByIn("customer_id", List.of(100, 101));
            assertThat(customer100).hasSize(4);
            
            // Read all
            List<Order> all = repo.findAll();
            assertThat(all).hasSize(5);
            
            factory.close();
        }
    }
    
    @Test
    void findBy_with_no_matches() {
        try (Arena arena = Arena.ofConfined()) {
            FfmTable table = new FfmTable("items", arena, List.of(
                    new ColumnSpec("id", int.class),
                    new ColumnSpec("category", String.class)
            ));
            
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ItemRepository repo = factory.createJPARepository(ItemRepository.class);
            
            repo.save(new Item(1, "electronics"));
            repo.save(new Item(2, "books"));
            
            List<Item> result = repo.findByCategory("clothing");
            assertThat(result).isEmpty();
            
            factory.close();
        }
    }
    
    @Test
    void findByIn_with_single_value() {
        try (Arena arena = Arena.ofConfined()) {
            FfmTable table = new FfmTable("products", arena, List.of(
                    new ColumnSpec("id", int.class),
                    new ColumnSpec("price", long.class)
            ));

            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            ProductRepository repo = factory.createJPARepository(ProductRepository.class);

            for (int i = 0; i < 100; i++) {
                repo.save(new Product(i, (i % 10) * 100L));
            }

            List<Product> result = repo.findByIn("price", List.of(500L));
            assertThat(result).hasSize(10);

            factory.close();
        }
    }

    @Test
    void join_two_tables_on_foreign_key() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();

            CustomerRepository customers = factory.createJPARepository(CustomerRepository.class);
            customers.save(new Customer(1, "Alice", "alice@email.com"));
            customers.save(new Customer(2, "Bob", "bob@email.com"));
            customers.save(new Customer(3, "Carol", "carol@email.com"));

            OrderRepository orders = factory.createJPARepository(OrderRepository.class);
            orders.save(new Order(101, 1, 2500L, "pending"));
            orders.save(new Order(102, 1, 1800L, "completed"));
            orders.save(new Order(103, 2, 3200L, "pending"));
            orders.save(new Order(104, 4, 900L, "completed"));

            // Updated join signature: pass entity classes instead of repositories
            List<JoinResult<Customer, Order>> joined = factory.join(
                    Customer.class, "id",
                    Order.class, "customer_id"
            );

            assertThat(joined).hasSize(3);

            assertThat(joined)
                    .extracting(r -> r.left().name)
                    .containsExactlyInAnyOrder("Alice", "Alice", "Bob");

            assertThat(joined)
                    .extracting(r -> r.right().amount)
                    .containsExactlyInAnyOrder(2500L, 1800L, 3200L);

            for (JoinResult<Customer, Order> row : joined) {
                assertThat(row.left().id).isEqualTo(row.right().customer_id);
            }

            factory.close();
        }
    }

    @Test
    void join_with_no_matches() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();

            CustomerRepository customers = factory.createJPARepository(CustomerRepository.class);
            customers.save(new Customer(1, "Alice", "alice@email.com"));

            OrderRepository orders = factory.createJPARepository(OrderRepository.class);
            orders.save(new Order(101, 99, 1000L, "pending"));

            // Updated join signature: pass entity classes instead of repositories
            List<JoinResult<Customer, Order>> joined = factory.join(
                    Customer.class, "id",
                    Order.class, "customer_id"
            );

            assertThat(joined).isEmpty();

            factory.close();
        }
    }
    
    // Entity classes for testing
    static final class Order {
        int id;
        int customer_id;
        long amount;
        String status;
        
        Order() {}
        
        Order(int id, int customer_id, long amount, String status) {
            this.id = id;
            this.customer_id = customer_id;
            this.amount = amount;
            this.status = status;
        }
    }
    
    static final class Item {
        int id;
        String category;
        
        Item() {}
        
        Item(int id, String category) {
            this.id = id;
            this.category = category;
        }
    }
    
    static final class Product {
        int id;
        long price;
        
        Product() {}
        
        Product(int id, long price) {
            this.id = id;
            this.price = price;
        }
    }

    static final class Customer {
        int id;
        String name;
        String email;

        Customer() {}

        Customer(int id, String name, String email) {
            this.id = id;
            this.name = name;
            this.email = email;
        }
    }

    @Test
    void cascade_save_nested_entity() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();

            PersonRepository persons = factory.createJPARepository(PersonRepository.class);
            AddressRepository addresses = factory.createJPARepository(AddressRepository.class);

            Address home = new Address("123 Main St", "Springfield");
            Person alice = new Person("Alice", home);

            persons.save(alice);

            assertThat(addresses.findAll()).hasSize(1);
            assertThat(addresses.findAll().get(0).street).isEqualTo("123 Main St");
            assertThat(addresses.findAll().get(0).city).isEqualTo("Springfield");

            factory.close();
        }
    }

    @Test
    void cascade_save_multiple_entities() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();

            PersonRepository persons = factory.createJPARepository(PersonRepository.class);
            AddressRepository addresses = factory.createJPARepository(AddressRepository.class);

            persons.save(new Person("Alice", new Address("123 Main St", "Springfield")));
            persons.save(new Person("Bob", new Address("456 Oak Ave", "Shelbyville")));
            persons.save(new Person("Carol", new Address("789 Pine Rd", "Capital City")));

            assertThat(persons.findAll()).hasSize(3);
            assertThat(addresses.findAll()).hasSize(3);

            assertThat(addresses.findByCity("Springfield")).hasSize(1);
            assertThat(addresses.findByCity("Shelbyville")).hasSize(1);

            factory.close();
        }
    }

    @Test
    void entity_without_nested_still_works() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();

            SimpleEntityRepository simples = factory.createJPARepository(SimpleEntityRepository.class);

            simples.save(new SimpleEntity("standalone"));
            simples.save(new SimpleEntity("another"));

            assertThat(simples.findAll()).hasSize(2);
            assertThat(simples.findAll())
                    .extracting(s -> s.name)
                    .containsExactlyInAnyOrder("standalone", "another");

            factory.close();
        }
    }
}

@Entity
final class Address {
    int id;
    String street;
    String city;

    Address() {}

    Address(String street, String city) {
        this.street = street;
        this.city = city;
    }
}

@Entity
final class Person {
    int id;
    String name;
    @OneToOne
    Address address;

    Person() {}

    Person(String name, Address address) {
        this.name = name;
        this.address = address;
    }
}

@Entity
final class SimpleEntity {
    int id;
    String name;

    SimpleEntity() {}

    SimpleEntity(String name) {
        this.name = name;
    }
}
