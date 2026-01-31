package io.memris.core;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.*;

/**
 * E-Commerce Real World Integration Test
 * 
 * Tests the Memris repository infrastructure with a realistic e-commerce domain
 * model.
 * Uses simplified entities without relationships (foreign keys stored as Long
 * fields)
 * since full relationship support is not yet implemented.
 */
class ECommerceRealWorldTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldCreateAndFindCustomerByEmail() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        Customer customer = new Customer("john.doe@example.com", "John Doe", "555-1234");

        // When
        Customer saved = customerRepo.save(customer);
        Optional<Customer> found = customerRepo.findByEmail("john.doe@example.com");

        // Then
        assertThat(saved.id).isNotNull();
        assertThat(found).isPresent();
        Customer foundCustomer = found.orElseThrow();
        assertThat(foundCustomer.email).isEqualTo("john.doe@example.com");
        assertThat(foundCustomer.name).isEqualTo("John Doe");
    }

    @Test
    void shouldCheckCustomerExistsByEmail() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        Customer customer = new Customer("jane.smith@example.com", "Jane Smith", "555-5678");
        customerRepo.save(customer);

        // When & Then
        assertThat(customerRepo.existsByEmail("jane.smith@example.com")).isTrue();
        assertThat(customerRepo.existsByEmail("nonexistent@example.com")).isFalse();
    }

    @Test
    void shouldFindCustomersByNameContaining() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        customerRepo.save(new Customer("alice@example.com", "Alice Johnson", "555-1111"));
        customerRepo.save(new Customer("alicia@example.com", "Alicia Brown", "555-2222"));
        customerRepo.save(new Customer("bob@example.com", "Bob Smith", "555-3333"));

        // When
        List<Customer> results = customerRepo.findByNameContainingIgnoreCase("ali");

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).extracting(c -> c.name)
                .containsExactlyInAnyOrder("Alice Johnson", "Alicia Brown");
    }

    @Test
    void shouldCreateAndFindProductBySku() {
        // Given
        ProductRepository productRepo = arena.createRepository(ProductRepository.class);
        Product product = new Product("LAPTOP-001", "Gaming Laptop", 129999, 50); // $1,299.99

        // When
        Product saved = productRepo.save(product);
        Optional<Product> found = productRepo.findBySku("LAPTOP-001");

        // Then
        assertThat(saved.id).isNotNull();
        assertThat(found).isPresent();
        Product foundProduct = found.orElseThrow();
        assertThat(foundProduct.sku).isEqualTo("LAPTOP-001");
        assertThat(foundProduct.name).isEqualTo("Gaming Laptop");
        assertThat(foundProduct.price).isEqualTo(129999);
        assertThat(foundProduct.getPriceDollars()).isEqualTo(1299.99);
    }

    @Test
    void shouldFindProductsByPriceRange() {
        // Given
        ProductRepository productRepo = arena.createRepository(ProductRepository.class);
        productRepo.save(new Product("MOUSE-001", "Wireless Mouse", 2999, 100)); // $29.99
        productRepo.save(new Product("KEYBOARD-001", "Mechanical Keyboard", 14999, 75)); // $149.99
        productRepo.save(new Product("MONITOR-001", "4K Monitor", 49999, 30)); // $499.99
        productRepo.save(new Product("HEADSET-001", "Gaming Headset", 7999, 50)); // $79.99

        // When
        List<Product> results = productRepo.findByPriceBetween(5000, 20000);

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).extracting(p -> p.name)
                .containsExactlyInAnyOrder("Mechanical Keyboard", "Gaming Headset");
    }

    @Test
    void shouldFindProductsWithStockGreaterThan() {
        // Given
        ProductRepository productRepo = arena.createRepository(ProductRepository.class);
        productRepo.save(new Product("ITEM-001", "Item A", 1000, 10));
        productRepo.save(new Product("ITEM-002", "Item B", 2000, 50));
        productRepo.save(new Product("ITEM-003", "Item C", 3000, 100));

        // When
        List<Product> results = productRepo.findByStockGreaterThan(25);

        // Then
        assertThat(results).hasSize(2);
        assertThat(results).extracting(p -> p.name)
                .containsExactlyInAnyOrder("Item B", "Item C");
    }

    @Test
    void shouldCreateAndFindOrderWithItems() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        OrderRepository orderRepo = arena.createRepository(OrderRepository.class);
        OrderItemRepository orderItemRepo = arena.createRepository(OrderItemRepository.class);

        // Create customer
        Customer customer = new Customer("order.test@example.com", "Order Test", "555-9999");
        Customer savedCustomer = customerRepo.save(customer);

        // Create order
        Order order = new Order(savedCustomer.id, "PENDING", 24997); // $249.97
        Order savedOrder = orderRepo.save(order);

        // Create order items
        OrderItem item1 = new OrderItem(savedOrder.id, 1L, 2, 9999); // 2 x $99.99
        OrderItem item2 = new OrderItem(savedOrder.id, 2L, 1, 4999); // 1 x $49.99
        orderItemRepo.save(item1);
        orderItemRepo.save(item2);

        // When
        Optional<Order> foundOrder = orderRepo.findById(savedOrder.id);
        List<OrderItem> orderItems = orderItemRepo.findByOrder(savedOrder.id);

        // Then
        assertThat(foundOrder).isPresent();
        Order foundOrderEntity = foundOrder.orElseThrow();
        assertThat(foundOrderEntity.customerId).isEqualTo(savedCustomer.id);
        assertThat(foundOrderEntity.status).isEqualTo("PENDING");
        assertThat(orderItems).hasSize(2);
    }

    @Test
    void shouldFindOrdersByCustomerAndStatus() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        OrderRepository orderRepo = arena.createRepository(OrderRepository.class);

        Customer customer1 = customerRepo.save(new Customer("cust1@example.com", "Customer One", "555-1111"));
        Customer customer2 = customerRepo.save(new Customer("cust2@example.com", "Customer Two", "555-2222"));

        // Customer 1 orders
        orderRepo.save(new Order(customer1.id, "PENDING", 10000));
        orderRepo.save(new Order(customer1.id, "CONFIRMED", 20000));
        orderRepo.save(new Order(customer1.id, "PENDING", 15000));

        // Customer 2 orders
        orderRepo.save(new Order(customer2.id, "PENDING", 5000));

        // When
        List<Order> customer1Pending = orderRepo.findByCustomerIdAndStatus(customer1.id, "PENDING");

        // Then
        assertThat(customer1Pending).hasSize(2);
        assertThat(customer1Pending).allMatch(o -> o.customerId.equals(customer1.id));
        assertThat(customer1Pending).allMatch(o -> o.status.equals("PENDING"));
    }

    @Test
    void shouldCountOrdersByStatus() {
        // Given
        OrderRepository orderRepo = arena.createRepository(OrderRepository.class);

        orderRepo.save(new Order(1L, "PENDING", 10000));
        orderRepo.save(new Order(2L, "PENDING", 20000));
        orderRepo.save(new Order(3L, "CONFIRMED", 15000));
        orderRepo.save(new Order(4L, "SHIPPED", 5000));
        orderRepo.save(new Order(5L, "PENDING", 8000));

        // When
        long pendingCount = orderRepo.countByStatus("PENDING");

        // Then
        assertThat(pendingCount).isEqualTo(3);
    }

    @Test
    void shouldIsolateECommerceDataBetweenArenas() {
        // Given - two separate arenas
        MemrisArena arena1 = factory.createArena();
        MemrisArena arena2 = factory.createArena();

        CustomerRepository repo1 = arena1.createRepository(CustomerRepository.class);
        CustomerRepository repo2 = arena2.createRepository(CustomerRepository.class);

        // When - save in arena1
        Customer customer = new Customer("isolated@example.com", "Isolated User", "555-0000");
        Customer saved = repo1.save(customer);

        // Then - arena2 should not see the data
        assertThat(repo1.findByEmail("isolated@example.com")).isPresent();
        assertThat(repo2.findByEmail("isolated@example.com")).isEmpty();
        assertThat(repo1.findById(saved.id)).isPresent();
        assertThat(repo2.findById(saved.id)).isEmpty();
    }

    @Test
    void shouldFindAllCustomers() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        customerRepo.save(new Customer("a@example.com", "A User", "555-1111"));
        customerRepo.save(new Customer("b@example.com", "B User", "555-2222"));
        customerRepo.save(new Customer("c@example.com", "C User", "555-3333"));

        // When
        List<Customer> allCustomers = customerRepo.findAll();

        // Then
        assertThat(allCustomers).hasSize(3);
    }

    @Test
    void shouldFindAllProducts() {
        // Given
        ProductRepository productRepo = arena.createRepository(ProductRepository.class);
        productRepo.save(new Product("SKU-1", "Product 1", 1000, 10));
        productRepo.save(new Product("SKU-2", "Product 2", 2000, 20));
        productRepo.save(new Product("SKU-3", "Product 3", 3000, 30));
        productRepo.save(new Product("SKU-4", "Product 4", 4000, 40));

        // When
        List<Product> allProducts = productRepo.findAll();

        // Then
        assertThat(allProducts).hasSize(4);
    }
}
