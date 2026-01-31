package io.memris.core;

import io.memris.core.Query;

import io.memris.core.Param;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.GenerationType;
import io.memris.core.Index;

import io.memris.repository.MemrisRepositoryFactory;
import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class QueryAnnotationIntegrationTest {

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
    void shouldFindProductBySkuUsingQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 10));
        repo.save(new Product("SKU-2", "Product 2", 2000, 20));

        Optional<Product> found = repo.findBySkuQuery("SKU-2");

        assertThat(found).isPresent();
        assertThat(found.orElseThrow().name).isEqualTo("Product 2");
    }

    @Test
    void shouldFindProductsByPriceBetweenUsingQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 10));
        repo.save(new Product("SKU-2", "Product 2", 2000, 20));
        repo.save(new Product("SKU-3", "Product 3", 3000, 30));

        List<Product> results = repo.findByPriceRange(1500, 3000);

        assertThat(results).hasSize(2);
        assertThat(results).extracting(p -> p.sku)
                .containsExactlyInAnyOrder("SKU-2", "SKU-3");
    }

    @Test
    void shouldFindProductsBySkuInUsingQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 10));
        repo.save(new Product("SKU-2", "Product 2", 2000, 20));
        repo.save(new Product("SKU-3", "Product 3", 3000, 30));

        List<Product> results = repo.findBySkus(List.of("SKU-1", "SKU-3"));

        assertThat(results).hasSize(2);
        assertThat(results).extracting(p -> p.sku)
                .containsExactlyInAnyOrder("SKU-1", "SKU-3");
    }

    @Test
    void shouldFindCustomersByNameIlikeUsingQuery() {
        QueryCustomerRepository repo = arena.createRepository(QueryCustomerRepository.class);
        repo.save(new Customer("alice@example.com", "Alice Johnson", "555-1111"));
        repo.save(new Customer("alicia@example.com", "ALICIA Brown", "555-2222"));
        repo.save(new Customer("bob@example.com", "Bob Smith", "555-3333"));

        List<Customer> results = repo.findByNameIlike("%ali%");

        assertThat(results).hasSize(2);
        assertThat(results).extracting(c -> c.name)
                .containsExactlyInAnyOrder("Alice Johnson", "ALICIA Brown");
    }

    @Test
    void shouldOrderProductsByPriceDescUsingQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 10));
        repo.save(new Product("SKU-2", "Product 2", 3000, 20));
        repo.save(new Product("SKU-3", "Product 3", 2000, 30));

        List<Product> results = repo.findAllOrderByPriceDesc();

        assertThat(results).hasSize(3);
        assertThat(results.get(0).sku).isEqualTo("SKU-2");
        assertThat(results.get(1).sku).isEqualTo("SKU-3");
        assertThat(results.get(2).sku).isEqualTo("SKU-1");
    }

    @Test
    void shouldApplyOrAndParenthesesUsingQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 5));
        repo.save(new Product("SKU-2", "Product 2", 2000, 50));
        repo.save(new Product("SKU-3", "Product 3", 5000, 100));

        List<Product> results = repo.findAffordableOrSpecific(10, 3000, "SKU-3");

        assertThat(results).hasSize(2);
        assertThat(results).extracting(p -> p.sku)
                .containsExactlyInAnyOrder("SKU-2", "SKU-3");
    }

    @Test
    void shouldProjectOrderSummaries() {
        QueryOrderRepository repo = arena.createRepository(QueryOrderRepository.class);
        QueryCustomerRepository customerRepo = arena.createRepository(QueryCustomerRepository.class);
        Customer alice = customerRepo.save(new Customer("alice@example.com", "Alice", "123"));
        Customer bob = customerRepo.save(new Customer("bob@example.com", "Bob", "456"));

        repo.save(new Order(1500, alice));
        repo.save(new Order(2500, bob));

        List<OrderSummary> results = repo.findSummaries(2000);

        assertThat(results).hasSize(1);
        assertThat(results.get(0).customerName()).isEqualTo("Bob");
        assertThat(results.get(0).total()).isEqualTo(2500);
    }

    @Test
    void shouldCountWithQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 0));
        repo.save(new Product("SKU-2", "Product 2", 2000, 10));
        repo.save(new Product("SKU-3", "Product 3", 3000, 5));

        long count = repo.countInStock();

        assertThat(count).isEqualTo(2);
    }

    @Test
    void shouldExistsWithQuery() {
        QueryProductRepository repo = arena.createRepository(QueryProductRepository.class);
        repo.save(new Product("SKU-1", "Product 1", 1000, 0));

        assertThat(repo.existsBySkuQuery("SKU-1")).isTrue();
        assertThat(repo.existsBySkuQuery("SKU-2")).isFalse();
    }

    public interface QueryProductRepository extends MemrisRepository<Product> {
        @Query("select p from Product p where p.sku = :sku")
        Optional<Product> findBySkuQuery(@Param("sku") String sku);

        @Query("select p from Product p where p.price between :min and :max")
        List<Product> findByPriceRange(@Param("min") long min, @Param("max") long max);

        @Query("select p from Product p where p.sku in :skus")
        List<Product> findBySkus(@Param("skus") List<String> skus);

        @Query("select p from Product p order by p.price desc")
        List<Product> findAllOrderByPriceDesc();

        @Query("select p from Product p where (p.stock > :minStock and p.price < :maxPrice) or p.sku = :sku")
        List<Product> findAffordableOrSpecific(@Param("minStock") int minStock, @Param("maxPrice") long maxPrice,
                @Param("sku") String sku);

        @Query("select count(p) from Product p where p.stock > 0")
        long countInStock();

        @Query("select p from Product p where p.sku = :sku")
        boolean existsBySkuQuery(@Param("sku") String sku);

        Product save(Product product);
    }

    public interface QueryCustomerRepository extends MemrisRepository<Customer> {
        @Query("select c from Customer c where c.name ilike :pattern")
        List<Customer> findByNameIlike(@Param("pattern") String pattern);

        Customer save(Customer customer);
    }

    public interface QueryOrderRepository extends MemrisRepository<Order> {
        @Query("select o.total as total, o.customer.name as customerName from Order o where o.total >= :min")
        List<OrderSummary> findSummaries(@Param("min") long min);

        Order save(Order order);
    }

    public record OrderSummary(long total, String customerName) {
    }
}
