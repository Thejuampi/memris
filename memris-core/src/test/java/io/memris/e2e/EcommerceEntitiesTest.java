package io.memris.e2e;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.JoinColumn;
import io.memris.core.ManyToOne;
import io.memris.core.OneToMany;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class EcommerceEntitiesTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;
    private CustomerRepository customerRepo;
    private ProductRepository productRepo;
    private OrderRepository orderRepo;
    private OrderItemRepository itemRepo;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
        arena = factory.createArena();
        customerRepo = arena.createRepository(CustomerRepository.class);
        productRepo = arena.createRepository(ProductRepository.class);
        orderRepo = arena.createRepository(OrderRepository.class);
        itemRepo = arena.createRepository(OrderItemRepository.class);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldRunEcommerceCheckoutLifecycleWithRealRepositories() {
        Customer customer = customerRepo.save(new Customer("buyer@example.com", "Buyer", "555-1010"));
        Product laptop = productRepo.save(new Product("SKU-LAPTOP", "Laptop", 129_999L, 5));
        Product mouse = productRepo.save(new Product("SKU-MOUSE", "Mouse", 2_999L, 25));

        Order order = orderRepo.save(new Order(customer.id, "PENDING", 0L));
        itemRepo.save(new OrderItem(order.id, laptop.id, 1, laptop.price));
        itemRepo.save(new OrderItem(order.id, mouse.id, 2, mouse.price));

        order.total = laptop.price + (mouse.price * 2);
        order.status = "CONFIRMED";
        orderRepo.save(order);

        assertThat(customerRepo.existsByEmail("buyer@example.com")).isTrue();
        assertThat(productRepo.findByPriceBetween(1_000L, 10_000L))
                .extracting(product -> product.sku)
                .containsExactly("SKU-MOUSE");

        assertThat(orderRepo.countByStatus("CONFIRMED")).isEqualTo(1L);
        assertThat(orderRepo.findByCustomerId(customer.id)).hasSize(1);
        assertThat(orderRepo.findByStatusIn(List.of("PENDING", "CONFIRMED"))).hasSize(1);
        assertThat(orderRepo.findByStatusAndTotalGreaterThanEqual("CONFIRMED", 100_000L)).hasSize(1);
        assertThat(orderRepo.findByStatusOrderByTotalDesc("CONFIRMED")).hasSize(1);
        assertThat(orderRepo.findTop3ByStatusOrderByIdAsc("CONFIRMED")).hasSize(1);

        assertThat(itemRepo.findByOrderId(order.id)).hasSize(2);
        assertThat(itemRepo.findByProductId(mouse.id)).hasSize(1);
        assertThat(itemRepo.findByQuantityGreaterThan(1)).hasSize(1);
    }

    @Test
    void shouldTraverseCustomerOrdersStatusAndHydrateJoinCollections() {
        Customer paidCustomer = customerRepo.save(new Customer("paid@example.com", "Paid Customer", "555-2020"));
        Customer pendingCustomer = customerRepo.save(new Customer("pending@example.com", "Pending Customer", "555-3030"));

        orderRepo.save(new Order(paidCustomer, "PAID", 25_000L));
        orderRepo.save(new Order(paidCustomer, "PENDING", 10_000L));
        orderRepo.save(new Order(pendingCustomer, "PENDING", 5_000L));

        List<Customer> customers = customerRepo.findByOrdersStatus("PAID");
        assertThat(customers).hasSize(1);

        Customer resolved = customers.get(0);
        assertThat(resolved.email).isEqualTo("paid@example.com");
        assertThat(resolved.orders).isNotNull();
        assertThat(resolved.orders).hasSize(2);
        assertThat(resolved.orders).extracting(order -> order.status).contains("PAID", "PENDING");
    }

    @Test
    void shouldHandleUpdatesDeletesAndNullAwareRelationshipHydration() {
        Customer customer = customerRepo.save(new Customer("old@example.com", "Old Name", "555-4040"));
        customer.email = "new@example.com";
        customer.name = "New Name";
        customerRepo.save(customer);

        assertThat(customerRepo.findByEmail("old@example.com")).isEmpty();
        assertThat(customerRepo.findByEmail("new@example.com")).isPresent();

        Product orphanProduct = productRepo.save(new Product("SKU-ORPHAN", "Orphan", 1_000L, 1));
        Order order = orderRepo.save(new Order(customer.id, "PAID", 1_000L));

        itemRepo.save(new OrderItem(order.id, orphanProduct.id, 1, orphanProduct.price));
        itemRepo.save(new OrderItem(order.id, null, 1, 0L));

        List<OrderItem> items = itemRepo.findByOrderId(order.id);
        assertThat(items).hasSize(2);
        assertThat(items.stream().filter(item -> item.productId == null).findFirst())
                .isPresent()
                .get()
                .extracting(item -> item.product)
                .isNull();

        customerRepo.deleteById(customer.id);
        Optional<Customer> deleted = customerRepo.findById(customer.id);
        assertThat(deleted).isEmpty();
    }

    @Entity
    public static class Customer {
        @Id
        public Long id;

        @Index(type = Index.IndexType.HASH)
        public String email;

        public String name;
        public String phone;
        public long created;

        @OneToMany(mappedBy = "customer")
        public List<Order> orders;

        public Customer() {
            this.created = System.currentTimeMillis();
        }

        public Customer(String email, String name, String phone) {
            this.email = email;
            this.name = name;
            this.phone = phone;
            this.created = System.currentTimeMillis();
        }
    }

    @Entity
    public static class Product {
        @Id
        public Long id;

        @Index(type = Index.IndexType.HASH)
        public String sku;

        public String name;
        public long price;
        public int stock;

        public Product() {
        }

        public Product(String sku, String name, long price, int stock) {
            this.sku = sku;
            this.name = name;
            this.price = price;
            this.stock = stock;
        }
    }

    @Entity
    public static class Order {
        @Id
        public Long id;

        public Long customerId;

        @Index(type = Index.IndexType.HASH)
        public String status;

        public long total;
        public long date;

        @ManyToOne
        @JoinColumn(name = "customer_id")
        public Customer customer;

        @OneToMany(mappedBy = "order")
        public List<OrderItem> items;

        public Order() {
        }

        public Order(Long customerId, String status, long total) {
            this.customerId = customerId;
            this.status = status;
            this.total = total;
            this.date = System.currentTimeMillis();
        }

        public Order(Customer customer, String status, long total) {
            this.customer = customer;
            this.customerId = customer != null ? customer.id : null;
            this.status = status;
            this.total = total;
            this.date = System.currentTimeMillis();
        }
    }

    @Entity
    public static class OrderItem {
        @Id
        public Long id;

        public Long orderId;
        public Long productId;
        public int quantity;
        public long unitPrice;

        @ManyToOne
        @JoinColumn(name = "order_id")
        public Order order;

        @ManyToOne
        @JoinColumn(name = "product_id")
        public Product product;

        public OrderItem() {
        }

        public OrderItem(Long orderId, Long productId, int quantity, long unitPrice) {
            this.orderId = orderId;
            this.productId = productId;
            this.quantity = quantity;
            this.unitPrice = unitPrice;
        }
    }

    public interface CustomerRepository extends MemrisRepository<Customer> {
        Customer save(Customer customer);

        Optional<Customer> findById(Long id);

        Optional<Customer> findByEmail(String email);

        List<Customer> findByNameContainingIgnoreCase(String name);

        List<Customer> findByOrdersStatus(String status);

        List<Customer> findAll();

        boolean existsByEmail(String email);

        void deleteById(Long id);
    }

    public interface ProductRepository extends MemrisRepository<Product> {
        Product save(Product product);

        Optional<Product> findById(Long id);

        Optional<Product> findBySku(String sku);

        List<Product> findByPriceBetween(long min, long max);

        List<Product> findByStockGreaterThan(int stock);

        List<Product> findAll();

        List<Product> findTop3ByOrderByPriceDesc();
    }

    public interface OrderRepository extends MemrisRepository<Order> {
        Order save(Order order);

        Optional<Order> findById(Long id);

        List<Order> findAll();

        List<Order> findByCustomerId(Long customerId);

        List<Order> findByCustomerIdAndStatus(Long customerId, String status);

        List<Order> findByStatus(String status);

        List<Order> findByStatusOrderByTotalDesc(String status);

        List<Order> findByStatusIn(List<String> statuses);

        List<Order> findByStatusAndTotalGreaterThanEqual(String status, long total);

        List<Order> findTop3ByStatusOrderByIdAsc(String status);

        long countByStatus(String status);

        void deleteById(Long id);
    }

    public interface OrderItemRepository extends MemrisRepository<OrderItem> {
        OrderItem save(OrderItem item);

        List<OrderItem> findAll();

        List<OrderItem> findByOrderId(Long orderId);

        List<OrderItem> findByProductId(Long productId);

        List<OrderItem> findByQuantityGreaterThan(int minQty);
    }
}
