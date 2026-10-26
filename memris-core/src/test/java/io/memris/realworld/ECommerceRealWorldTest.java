package io.memris.realworld;

import io.memris.core.Id;
import io.memris.core.JoinTable;
import io.memris.core.ManyToMany;
import io.memris.core.Customer;
import io.memris.core.CustomerRepository;
import io.memris.core.Order;
import io.memris.core.OrderRepository;
import io.memris.core.Product;
import io.memris.core.ProductRepository;

import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.repository.MemrisArena;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.*;

/**
 * E-Commerce Real World Integration Test
 * 
 * Tests the Memris repository infrastructure with a realistic e-commerce domain
 * model.
 * Covers customer query contracts, typed analytics, and many-to-many hydration
 * through the public repository API.
 */
class ECommerceRealWorldTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory(RealWorldSupport.configuration().build());
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
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
        assertThat(results.stream().map(customer -> customer.name).collect(java.util.stream.Collectors.toSet()))
                .isEqualTo(Set.of("Alice Johnson", "Alicia Brown"));
    }

    @ParameterizedTest(name = "customer lookup aliases under {0}")
    @ValueSource(strings = { "en-US", "tr-TR", "lt-LT" })
    @ResourceLock(Resources.LOCALE)
    void shouldFindAndRemoveCustomersWithCaseInsensitiveAliases(String languageTag) {
        var previousLocale = Locale.getDefault();
        var previousDisplayLocale = Locale.getDefault(Locale.Category.DISPLAY);
        var previousFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.forLanguageTag(languageTag));
            var customers = arena.createRepository(CustomerRepository.class);
            var first = new Customer("ALICE@example.com", "IZMIR", "555-1111");
            first.created = 1;
            first = customers.save(first);
            var second = new Customer("alice@EXAMPLE.com", "izmir", "555-2222");
            second.created = 2;
            second = customers.save(second);
            var bob = customers.save(new Customer("bob@example.com", "Bob", "555-3333"));
            var draft = customers.save(new Customer(null, null, "555-4444"));

            assertThat(customers.findByNameIgnoreCase("Izmir")).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(first.id, second.id);
            assertThat(customers.findByNameEqualsIgnoreCase("Izmir")).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(first.id, second.id);
            assertThat(customers.findByNameIsIgnoreCase("Izmir")).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(first.id, second.id);
            assertThat(customers.findByNameAndEmailAllIgnoreCaseOrderByCreatedAsc("Izmir", "Alice@Example.Com"))
                    .extracting(row -> row.id).containsExactly(first.id, second.id);
            assertThat(customers.findByNameOrEmailAllIgnoreCase("Izmir", "BOB@EXAMPLE.COM"))
                    .extracting(row -> row.id).containsExactlyInAnyOrder(first.id, second.id, bob.id);
            assertThat(customers.findByNameIsNotNull()).extracting(row -> row.id)
                    .containsExactlyInAnyOrder(first.id, second.id, bob.id);

            assertThat(customers.removeByEmailIgnoreCase("Alice@Example.Com")).isEqualTo(2);
            assertThat(customers.removeByEmailIgnoreCase("Alice@Example.Com")).isZero();
            assertThat(customers.findByNameIsNotNull()).extracting(row -> row.id).containsExactly(bob.id);
            assertThat(customers.findById(draft.id)).isPresent();
        } finally {
            Locale.setDefault(previousLocale);
            Locale.setDefault(Locale.Category.DISPLAY, previousDisplayLocale);
            Locale.setDefault(Locale.Category.FORMAT, previousFormatLocale);
        }
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
        var actual = new ProductLookupSnapshot(
                saved.id != null,
                found.isPresent(),
                found.map(value -> new ProductView(value.sku, value.name, value.price, value.stock)).orElse(null),
                found.map(Product::getPriceDollars).orElse(0.0)
        );
        var expected = new ProductLookupSnapshot(
                true,
                true,
                new ProductView(product.sku, product.name, product.price, product.stock),
                1299.99
        );
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
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
        assertThat(new ArenaIsolationSnapshot(
                repo1.findByEmail("isolated@example.com").isPresent(),
                repo2.findByEmail("isolated@example.com").isPresent(),
                repo1.findById(saved.id).isPresent(),
                repo2.findById(saved.id).isPresent()))
                .usingRecursiveComparison()
                .isEqualTo(new ArenaIsolationSnapshot(true, false, true, false));
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

    @Test
    void shouldHandleNullParameters() {
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        OrderRepository orderRepo = arena.createRepository(OrderRepository.class);
        customerRepo.save(new Customer("test@example.com", "Test User", "555-1234"));
        orderRepo.save(new Order(1L, "PENDING", 10000));

        // String equality with a null argument matches null values, rather than rejecting the argument.
        assertThat(customerRepo.findByEmail(null)).isEmpty();
        assertThat(customerRepo.existsByEmail(null)).isFalse();
        assertThat(orderRepo.countByStatus(null)).isZero();

        Customer withoutEmail = customerRepo.save(new Customer(null, "No Email", "555-5678"));
        orderRepo.save(new Order(1L, null, 20000));
        orderRepo.save(new Order(2L, null, 30000));

        assertThat(customerRepo.findByEmail(null)).map(customer -> customer.id).contains(withoutEmail.id);
        assertThat(customerRepo.existsByEmail(null)).isTrue();
        assertThat(orderRepo.countByStatus(null)).isEqualTo(2);
        assertThat(customerRepo.findByNameContainingIgnoreCase(null)).isEmpty();
    }

    @Test
    void shouldHandleNullFieldValuesInQueries() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        Customer c1 = new Customer(null, "Alice Johnson", "555-1111");
        Customer c2 = new Customer("alice2@example.com", null, "555-2222");
        Customer c3 = new Customer("alice3@example.com", "Alice Smith", "555-3333");
        customerRepo.save(c1);
        customerRepo.save(c2);
        customerRepo.save(c3);

        // When
        List<Customer> results = customerRepo.findByNameContainingIgnoreCase("ali");

        // Only the queried field affects matching; a null email does not exclude Alice Johnson.
        assertThat(results).extracting(customer -> customer.id).containsExactlyInAnyOrder(c1.id, c3.id);
        assertThat(customerRepo.findById(c2.id)).get().extracting(customer -> customer.name).isNull();
    }

    @Test
    void shouldHandleEmptyStringAndWhitespace() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        customerRepo.save(new Customer("test1@example.com", "Alice", "555-1111"));
        customerRepo.save(new Customer("test2@example.com", "Bob", "555-2222"));
        customerRepo.save(new Customer("test3@example.com", "Charlie", "555-3333"));

        // When
        List<Customer> emptyResults = customerRepo.findByNameContainingIgnoreCase("");
        List<Customer> whitespaceResults = customerRepo.findByNameContainingIgnoreCase("   ");

        // Then - empty string typically matches all non-null values
        assertThat(new EmptyAndWhitespaceSnapshot(emptyResults.size(), whitespaceResults.size()))
                .usingRecursiveComparison()
                .isEqualTo(new EmptyAndWhitespaceSnapshot(3, 0));
    }

    @ParameterizedTest(name = "case folding with default locale {0}")
    @ValueSource(strings = { "en-US", "tr-TR", "lt-LT" })
    @ResourceLock(Resources.LOCALE)
    void shouldHandleCaseFoldingCorrectness(String languageTag) {
        var previousLocale = Locale.getDefault();
        var previousDisplayLocale = Locale.getDefault(Locale.Category.DISPLAY);
        var previousFormatLocale = Locale.getDefault(Locale.Category.FORMAT);
        try {
            Locale.setDefault(Locale.forLanguageTag(languageTag));
            var customerRepo = arena.createRepository(CustomerRepository.class);
            customerRepo.save(new Customer("test1@example.com", "İstanbul", "555-1111"));
            customerRepo.save(new Customer("test2@example.com", "straße", "555-2222"));
            customerRepo.save(new Customer("test3@example.com", "München", "555-3333"));
            customerRepo.save(new Customer("test4@example.com", "IZMIR", "555-4444"));
            customerRepo.save(new Customer("test5@example.com", "I\u0301ris", "555-5555"));
            customerRepo.save(new Customer("test6@example.com", null, "555-6666"));

            assertThat(customerRepo.findByNameContainingIgnoreCase("i")).extracting(customer -> customer.name)
                    .containsExactlyInAnyOrder("İstanbul", "IZMIR", "I\u0301ris");
            assertThat(customerRepo.findByNameLikeIgnoreCase("%I%")).extracting(customer -> customer.name)
                    .containsExactlyInAnyOrder("İstanbul", "IZMIR", "I\u0301ris");
            assertThat(customerRepo.findByNameStartingWithIgnoreCase("I")).extracting(customer -> customer.name)
                    .containsExactlyInAnyOrder("İstanbul", "IZMIR", "I\u0301ris");
            assertThat(customerRepo.findByNameEndingWithIgnoreCase("ir")).extracting(customer -> customer.name)
                    .containsExactly("IZMIR");
            assertThat(customerRepo.findByNameContainingIgnoreCase("i\u0301")).extracting(customer -> customer.name)
                    .containsExactly("I\u0301ris");
            // ROOT lowercasing does not expand the German sharp s to "ss".
            assertThat(customerRepo.findByNameContainingIgnoreCase("ss")).isEmpty();
            assertThat(customerRepo.findByNameContainingIgnoreCase("ß")).extracting(customer -> customer.name)
                    .containsExactly("straße");
        } finally {
            Locale.setDefault(previousLocale);
            Locale.setDefault(Locale.Category.DISPLAY, previousDisplayLocale);
            Locale.setDefault(Locale.Category.FORMAT, previousFormatLocale);
        }
    }

    @Test
    void shouldHandleDuplicateNaturalKeys() {
        var customerRepo = arena.createRepository(CustomerRepository.class);
        Customer first = customerRepo.save(new Customer("duplicate@example.com", "First User", "555-1111"));
        Customer second = customerRepo.save(new Customer("duplicate@example.com", "Second User", "555-2222"));

        // Email is not unique. Only the ID identifies an existing row for an update.
        assertThat(first.id).isNotEqualTo(second.id);
        assertThat(customerRepo.queryByEmail("duplicate@example.com")).extracting(customer -> customer.id)
                .containsExactlyInAnyOrder(first.id, second.id);

        first.name = "Updated First User";
        assertThat(customerRepo.save(first).id).isEqualTo(first.id);
        assertThat(customerRepo.findById(first.id)).get().extracting(customer -> customer.name)
                .isEqualTo("Updated First User");
        assertThat(customerRepo.findById(second.id)).get().extracting(customer -> customer.name)
                .isEqualTo("Second User");
        assertThat(customerRepo.findAll()).hasSize(2);

        customerRepo.deleteById(first.id);
        assertThat(customerRepo.queryByEmail("duplicate@example.com")).extracting(customer -> customer.id)
                .containsExactly(second.id);
    }

    @Test
    void shouldHandleIdHandling() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        Customer customerWithId = new Customer("withid@example.com", "With ID", "555-1111");
        customerWithId.id = 42L;
        Customer customerWithoutId = new Customer("withoutid@example.com", "Without ID", "555-2222");

        // When
        Customer savedWithId = customerRepo.save(customerWithId);
        Customer savedWithoutId = customerRepo.save(customerWithoutId);

        // Then - pre-set ID should be preserved or replaced based on implementation
        assertThat(new PresetIdSnapshot(savedWithId.id != null, savedWithoutId.id != null)).usingRecursiveComparison()
                .isEqualTo(new PresetIdSnapshot(true, true));
    }

    @Test
    void shouldHandleNumericRangeOverflow() {
        // Given
        ProductRepository productRepo = arena.createRepository(ProductRepository.class);
        productRepo.save(new Product("P1", "Product 1", Integer.MAX_VALUE - 1, 10));
        productRepo.save(new Product("P2", "Product 2", Integer.MAX_VALUE, 10));

        // When
        List<Product> all = productRepo.findAll();

        // Then - values should be stored correctly without overflow
        assertThat(all).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new Product("P1", "Product 1", Integer.MAX_VALUE - 1, 10),
                new Product("P2", "Product 2", Integer.MAX_VALUE, 10)
        );
    }

    @Test
    void shouldHandleCrossEntityIsolationWithinSameArena() {
        // Given
        CustomerRepository customerRepo = arena.createRepository(CustomerRepository.class);
        ProductRepository productRepo = arena.createRepository(ProductRepository.class);

        // When
        Customer customer = customerRepo.save(new Customer("test@example.com", "Test User", "555-1234"));
        Product product = productRepo.save(new Product("SKU-001", "Product 1", 1000, 10));

        // Then - customer ID should not be found as product SKU and vice versa
        assertThat(productRepo.findBySku(customer.id.toString())).isEmpty();
        assertThat(customerRepo.findByEmail("SKU-001")).isEmpty();
    }

    @Test
    void shouldHandleReturnTypeCardinality() {
        var customerRepo = arena.createRepository(CustomerRepository.class);
        assertThat(customerRepo.findByEmail("duplicate@example.com")).isEmpty();
        assertThat(customerRepo.findByEmailOrderByNameAsc("duplicate@example.com")).isEmpty();
        assertThat(customerRepo.findByEmailOrderByNameDesc("duplicate@example.com")).isEmpty();
        assertThat(customerRepo.queryByEmail("duplicate@example.com")).isEmpty();

        Customer first = customerRepo.save(new Customer("duplicate@example.com", "First", "555-1111"));
        assertThat(customerRepo.findByEmail("duplicate@example.com")).map(customer -> customer.id).contains(first.id);
        assertThat(customerRepo.findByEmailOrderByNameAsc("duplicate@example.com"))
                .map(customer -> customer.id).contains(first.id);

        Customer second = customerRepo.save(new Customer("duplicate@example.com", "Second", "555-2222"));
        assertThat(customerRepo.findByEmail("duplicate@example.com"))
                .hasValueSatisfying(customer -> assertThat(customer.id).isIn(first.id, second.id));
        // Optional selects the first match; ordering makes that choice deterministic for non-unique fields.
        assertThat(customerRepo.findByEmailOrderByNameAsc("duplicate@example.com"))
                .map(customer -> customer.id).contains(first.id);
        assertThat(customerRepo.findByEmailOrderByNameDesc("duplicate@example.com"))
                .map(customer -> customer.id).contains(second.id);
        assertThat(customerRepo.queryByEmail("duplicate@example.com")).extracting(customer -> customer.id)
                .containsExactlyInAnyOrder(first.id, second.id);
        assertThat(customerRepo.findByEmailOrderByNameAsc("missing@example.com")).isEmpty();
    }

    @Test
    void shouldHandleInQueriesEmptyList() {
        // Given
        OrderRepository orderRepo = arena.createRepository(OrderRepository.class);
        orderRepo.save(new Order(1L, "PENDING", 10000));

        // When
        List<Order> results = orderRepo.findByStatusIn(List.of());

        // Then - empty list should return empty results
        assertThat(results).isEmpty();
    }

    @Test
    void shouldHandleNotNegation() {
        var customerRepo = arena.createRepository(CustomerRepository.class);
        Customer c1 = customerRepo.save(new Customer("test1@example.com", "Alice", "555-1111"));
        Customer c2 = customerRepo.save(new Customer("test2@example.com", "Bob", "555-2222"));
        Customer c3 = customerRepo.save(new Customer("test3@example.com", "Charlie", "555-3333"));
        Customer c4 = customerRepo.save(new Customer("test4@example.com", null, "555-4444"));

        // When
        List<Customer> results = customerRepo.findByNameNot("Alice");

        // NE is the complement of EQ, so null names are included when the argument is non-null.
        assertThat(results).extracting(customer -> customer.id).containsExactlyInAnyOrder(c2.id, c3.id, c4.id);
        assertThat(customerRepo.findByNameNot(null)).extracting(customer -> customer.id)
                .containsExactlyInAnyOrder(c1.id, c2.id, c3.id);
        assertThat(customerRepo.findByNameNotNullAndNameNot("Alice")).extracting(customer -> customer.id)
                .containsExactlyInAnyOrder(c2.id, c3.id);
    }

    @Test
    void shouldQueryTypedFieldsAcrossHandlers() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        var baseInstant = Instant.parse("2025-01-01T00:00:00Z");
        var baseDate = LocalDate.of(2025, 1, 1);
        var baseDateTime = LocalDateTime.of(2025, 1, 1, 0, 0);

        repo.save(new AnalyticsEvent(
            "Pro Plan",
            true,
            (byte) 5,
            (short) 200,
            'B',
            10,
            1500L,
            1.5f,
            99.9,
            new BigDecimal("19.99"),
            new BigInteger("1000"),
            baseInstant.plusSeconds(60),
            baseDate.plusDays(1),
            baseDateTime.plusHours(1),
            Date.from(baseInstant.plusSeconds(120))
        ));
        repo.save(new AnalyticsEvent(
            "Starter",
            false,
            (byte) 1,
            (short) 50,
            'D',
            3,
            200L,
            0.2f,
            12.5,
            new BigDecimal("9.99"),
            new BigInteger("10"),
            baseInstant.minusSeconds(60),
            baseDate.minusDays(1),
            baseDateTime.minusHours(1),
            Date.from(baseInstant.minusSeconds(120))
        ));
        repo.save(new AnalyticsEvent(
            "Enterprise Pro",
            true,
            (byte) 9,
            (short) 500,
            'A',
            20,
            5000L,
            3.1f,
            250.0,
            new BigDecimal("19.99"),
            new BigInteger("9999"),
            baseInstant.plusSeconds(3600),
            baseDate.plusDays(5),
            baseDateTime.plusHours(5),
            Date.from(baseInstant.plusSeconds(7200))
        ));

        var snapshot = new HandlerCoverageSnapshot(
            repo.findByTitleLike("%Pro%").size(),
            repo.findByTitleNotLike("%Starter%").size(),
            repo.findByTitleContaining("Plan").size(),
            repo.findByTitleNotContaining("Enterprise").size(),
            repo.findByActiveTrue().size(),
            repo.findByActiveFalse().size(),
            repo.findByPriorityGreaterThan((byte) 4).size(),
            repo.findByRatingLessThan((short) 300).size(),
            repo.findByGradeGreaterThanEqual('B').size(),
            repo.findByQuantityLessThanEqual(10).size(),
            repo.findByRevenueGreaterThan(1000L).size(),
            repo.findByRatioGreaterThan(1.0f).size(),
            repo.findByCostLessThan(200.0).size(),
            repo.findByAmount(new BigDecimal("19.99")).size(),
            repo.findByBigCountNot(new BigInteger("10")).size(),
            repo.findByEventTimeGreaterThan(baseInstant).size(),
            repo.findByShipDateLessThan(baseDate).size(),
            repo.findByProcessedAtGreaterThanEqual(baseDateTime).size(),
            repo.findByLegacyDateLessThan(Date.from(baseInstant)).size()
        );

        var expected = new HandlerCoverageSnapshot(
            2,
            2,
            1,
            2,
            2,
            1,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            2,
            1,
            2,
            1
        );

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldCoverNullsAndStringOperators() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        repo.save(new AnalyticsEvent(
            "Pro Starter",
            true,
            (byte) 1,
            (short) 10,
            'A',
            1,
            100L,
            0.5f,
            10.0,
            new BigDecimal("1.00"),
            new BigInteger("1"),
            Instant.parse("2025-02-01T00:00:00Z"),
            LocalDate.of(2025, 2, 1),
            LocalDateTime.of(2025, 2, 1, 0, 0),
            new Date(1706745600000L)
        ));
        repo.save(new AnalyticsEvent(
            "Enterprise",
            false,
            (byte) 2,
            (short) 20,
            'B',
            2,
            200L,
            1.5f,
            20.0,
            new BigDecimal("2.00"),
            new BigInteger("2"),
            Instant.parse("2025-02-02T00:00:00Z"),
            LocalDate.of(2025, 2, 2),
            LocalDateTime.of(2025, 2, 2, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            null,
            true,
            (byte) 3,
            (short) 30,
            'C',
            3,
            300L,
            2.5f,
            30.0,
            new BigDecimal("3.00"),
            new BigInteger("3"),
            Instant.parse("2025-02-03T00:00:00Z"),
            LocalDate.of(2025, 2, 3),
            LocalDateTime.of(2025, 2, 3, 0, 0),
            null
        ));

        var snapshot = new StringOperatorSnapshot(
            repo.findByTitleStartingWith("Pro").size(),
            repo.findByTitleEndingWith("ter").size(),
            repo.findByTitleNotContaining("prise").size(),
            repo.findByTitleNotLike("%Pro%").size(),
            repo.findByTitleIsNull().size(),
            repo.findByLegacyDateNotNull().size()
        );

        var expected = new StringOperatorSnapshot(1, 1, 2, 2, 1, 1);

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldCoverInNotInAndNullsAcrossTypes() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        repo.save(new AnalyticsEvent(
            "Alpha",
            true,
            (byte) 1,
            (short) 10,
            'A',
            1,
            100L,
            0.1f,
            10.0,
            new BigDecimal("1.00"),
            new BigInteger("1"),
            Instant.parse("2025-03-01T00:00:00Z"),
            LocalDate.of(2025, 3, 1),
            LocalDateTime.of(2025, 3, 1, 0, 0),
            new Date(1740787200000L)
        ));
        repo.save(new AnalyticsEvent(
            "Bravo",
            false,
            (byte) 2,
            (short) 20,
            'B',
            2,
            200L,
            0.2f,
            20.0,
            new BigDecimal("2.00"),
            new BigInteger("2"),
            Instant.parse("2025-03-02T00:00:00Z"),
            LocalDate.of(2025, 3, 2),
            LocalDateTime.of(2025, 3, 2, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Charlie",
            true,
            (byte) 3,
            (short) 30,
            'C',
            3,
            300L,
            0.3f,
            30.0,
            new BigDecimal("3.00"),
            new BigInteger("3"),
            Instant.parse("2025-03-03T00:00:00Z"),
            LocalDate.of(2025, 3, 3),
            LocalDateTime.of(2025, 3, 3, 0, 0),
            null
        ));

        var snapshot = new InOperatorSnapshot(
            repo.findByTitleIn(List.of("Alpha", "Charlie")).size(),
            repo.findByTitleNotIn(List.of("Alpha", "Charlie")).size(),
            repo.findByPriorityIn(new byte[] { 1, 3 }).size(),
            repo.findByQuantityNotIn(List.of(1, 2)).size(),
            repo.findByAmountIn(List.of(new BigDecimal("2.00"), new BigDecimal("3.00"))).size(),
            repo.findByBigCountNotIn(List.of(new BigInteger("1"))).size(),
            repo.findByEventTimeIn(List.of(Instant.parse("2025-03-01T00:00:00Z"), Instant.parse("2025-03-03T00:00:00Z"))).size(),
            repo.findByShipDateNotIn(List.of(LocalDate.of(2025, 3, 2))).size(),
            repo.findByProcessedAtIn(List.of(LocalDateTime.of(2025, 3, 2, 0, 0))).size(),
            repo.findByLegacyDateIsNull().size()
        );

        var expected = new InOperatorSnapshot(2, 1, 2, 1, 2, 2, 2, 2, 1, 2);

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldCoverBetweenAndNotStringPrefixes() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        var instantStart = Instant.parse("2025-04-01T00:00:00Z");
        var instantMiddle = Instant.parse("2025-04-02T00:00:00Z");
        var instantEnd = Instant.parse("2025-04-03T00:00:00Z");

        repo.save(new AnalyticsEvent(
            "Pro Alpha",
            true,
            (byte) 1,
            (short) 10,
            'A',
            1,
            100L,
            0.1f,
            10.0,
            new BigDecimal("1.00"),
            new BigInteger("1"),
            instantStart,
            LocalDate.of(2025, 4, 1),
            LocalDateTime.of(2025, 4, 1, 0, 0),
            new Date(1743465600000L)
        ));
        repo.save(new AnalyticsEvent(
            "Beta End",
            false,
            (byte) 2,
            (short) 20,
            'B',
            2,
            200L,
            0.2f,
            20.0,
            new BigDecimal("2.00"),
            new BigInteger("2"),
            instantMiddle,
            LocalDate.of(2025, 4, 2),
            LocalDateTime.of(2025, 4, 2, 0, 0),
            new Date(1743552000000L)
        ));
        repo.save(new AnalyticsEvent(
            "Gamma End",
            true,
            (byte) 3,
            (short) 30,
            'C',
            3,
            300L,
            0.3f,
            30.0,
            new BigDecimal("3.00"),
            new BigInteger("3"),
            instantEnd,
            LocalDate.of(2025, 4, 3),
            LocalDateTime.of(2025, 4, 3, 0, 0),
            new Date(1743638400000L)
        ));

        var snapshot = new BetweenOperatorSnapshot(
            repo.findByTitleNotStartingWith("Pro").size(),
            repo.findByTitleNotEndingWith("End").size(),
            repo.findByEventTimeBetween(instantStart, instantEnd).size(),
            repo.findByShipDateBetween(LocalDate.of(2025, 4, 1), LocalDate.of(2025, 4, 2)).size(),
            repo.findByProcessedAtBetween(LocalDateTime.of(2025, 4, 2, 0, 0), LocalDateTime.of(2025, 4, 3, 0, 0)).size(),
            repo.findByLegacyDateBetween(new Date(1743465600000L), new Date(1743552000000L)).size()
        );

        var expected = new BetweenOperatorSnapshot(2, 1, 3, 2, 2, 2);

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldCoverOrCombinations() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        repo.save(new AnalyticsEvent(
            "Alpha",
            true,
            (byte) 1,
            (short) 10,
            'A',
            1,
            100L,
            0.1f,
            10.0,
            new BigDecimal("1.00"),
            new BigInteger("1"),
            Instant.parse("2025-05-01T00:00:00Z"),
            LocalDate.of(2025, 5, 1),
            LocalDateTime.of(2025, 5, 1, 0, 0),
            new Date(1746057600000L)
        ));
        repo.save(new AnalyticsEvent(
            "Bravo",
            false,
            (byte) 2,
            (short) 20,
            'B',
            2,
            200L,
            0.2f,
            20.0,
            new BigDecimal("2.00"),
            new BigInteger("2"),
            Instant.parse("2025-05-02T00:00:00Z"),
            LocalDate.of(2025, 5, 2),
            LocalDateTime.of(2025, 5, 2, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Charlie",
            true,
            (byte) 3,
            (short) 30,
            'C',
            3,
            300L,
            0.3f,
            30.0,
            new BigDecimal("3.00"),
            new BigInteger("3"),
            Instant.parse("2025-05-03T00:00:00Z"),
            LocalDate.of(2025, 5, 3),
            LocalDateTime.of(2025, 5, 3, 0, 0),
            null
        ));

        var snapshot = new OrOperatorSnapshot(
            repo.findByTitleStartingWithOrQuantityGreaterThan("Al", 2).size(),
            repo.findByActiveFalseOrCostLessThan(15.0).size(),
            repo.findByEventTimeGreaterThanOrLegacyDateIsNull(Instant.parse("2025-05-02T12:00:00Z")).size()
        );

        var expected = new OrOperatorSnapshot(2, 2, 2);

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldCoverTopAndOrdering() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        repo.save(new AnalyticsEvent(
            "Alpha",
            true,
            (byte) 1,
            (short) 10,
            'A',
            1,
            100L,
            0.1f,
            10.0,
            new BigDecimal("1.00"),
            new BigInteger("1"),
            Instant.parse("2025-06-01T00:00:00Z"),
            LocalDate.of(2025, 6, 1),
            LocalDateTime.of(2025, 6, 1, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Bravo",
            false,
            (byte) 2,
            (short) 20,
            'B',
            2,
            200L,
            0.2f,
            20.0,
            new BigDecimal("2.00"),
            new BigInteger("2"),
            Instant.parse("2025-06-02T00:00:00Z"),
            LocalDate.of(2025, 6, 2),
            LocalDateTime.of(2025, 6, 2, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Charlie",
            true,
            (byte) 3,
            (short) 30,
            'C',
            3,
            300L,
            0.3f,
            30.0,
            new BigDecimal("3.00"),
            new BigInteger("3"),
            Instant.parse("2025-06-03T00:00:00Z"),
            LocalDate.of(2025, 6, 3),
            LocalDateTime.of(2025, 6, 3, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Delta",
            true,
            (byte) 4,
            (short) 40,
            'D',
            4,
            400L,
            0.4f,
            40.0,
            new BigDecimal("4.00"),
            new BigInteger("4"),
            Instant.parse("2025-06-04T00:00:00Z"),
            LocalDate.of(2025, 6, 4),
            LocalDateTime.of(2025, 6, 4, 0, 0),
            null
        ));

        var snapshot = new OrderingSnapshot(
            repo.findTop2ByActiveTrueOrderByRevenueDesc().size(),
            repo.findTop2ByActiveTrueOrderByRevenueDesc().get(0).revenue,
            repo.findFirstByActiveTrueOrderByTitleAsc().title
        );

        var expected = new OrderingSnapshot(2, 400L, "Alpha");

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldCoverDistinct() {
        var repo = arena.createRepository(AnalyticsEventRepository.class);

        repo.save(new AnalyticsEvent(
            "Duplicate",
            true,
            (byte) 1,
            (short) 10,
            'A',
            1,
            100L,
            0.1f,
            10.0,
            new BigDecimal("1.00"),
            new BigInteger("1"),
            Instant.parse("2025-07-01T00:00:00Z"),
            LocalDate.of(2025, 7, 1),
            LocalDateTime.of(2025, 7, 1, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Duplicate",
            false,
            (byte) 2,
            (short) 20,
            'B',
            2,
            200L,
            0.2f,
            20.0,
            new BigDecimal("2.00"),
            new BigInteger("2"),
            Instant.parse("2025-07-02T00:00:00Z"),
            LocalDate.of(2025, 7, 2),
            LocalDateTime.of(2025, 7, 2, 0, 0),
            null
        ));
        repo.save(new AnalyticsEvent(
            "Unique",
            true,
            (byte) 3,
            (short) 30,
            'C',
            3,
            300L,
            0.3f,
            30.0,
            new BigDecimal("3.00"),
            new BigInteger("3"),
            Instant.parse("2025-07-03T00:00:00Z"),
            LocalDate.of(2025, 7, 3),
            LocalDateTime.of(2025, 7, 3, 0, 0),
            null
        ));

        var snapshot = new DistinctSnapshot(
            repo.findDistinctByTitle("Duplicate").size(),
            repo.findDistinctByTitle("Unique").size()
        );

        var expected = new DistinctSnapshot(2, 1);

        assertThat(snapshot).usingRecursiveComparison().isEqualTo(expected);
    }

    @Test
    void shouldHydrateManyToManyJoinCollections() {
        var studentRepo = arena.createRepository(ManyToManyStudentRepository.class);
        var courseRepo = arena.createRepository(ManyToManyCourseRepository.class);

        var student1 = studentRepo.save(new ManyToManyStudent("Student One"));
        var student2 = studentRepo.save(new ManyToManyStudent("Student Two"));
        var math = courseRepo.save(new ManyToManyCourse("Math"));
        var physics = courseRepo.save(new ManyToManyCourse("Physics"));

        // Create join table entries manually by using intermediate entities
        // Student1 enrolled in Math and Physics
        student1.courses = List.of(math, physics);
        studentRepo.save(student1);

        // Student2 enrolled in Math only
        student2.courses = List.of(math);
        studentRepo.save(student2);

        // Query students by course name
        var mathStudents = studentRepo.findByCoursesName("Math");

        var physicsStudents = studentRepo.findByCoursesName("Physics");
        var actual = new ManyToManySnapshot(
                mathStudents.stream().map(student -> student.name).collect(java.util.stream.Collectors.toSet()),
                physicsStudents.size(),
                physicsStudents.get(0).name
        );
        var expected = new ManyToManySnapshot(Set.of("Student One", "Student Two"), 1, "Student One");
        assertThat(actual).usingRecursiveComparison().isEqualTo(expected);
    }

    private record ProductLookupSnapshot(boolean idAssigned, boolean found, ProductView product, double dollars) {
    }

    private record ArenaIsolationSnapshot(boolean arena1Email, boolean arena2Email, boolean arena1Id, boolean arena2Id) {
    }

    private record EmptyAndWhitespaceSnapshot(int emptySize, int whitespaceSize) {
    }

    private record PresetIdSnapshot(boolean presetAssigned, boolean generatedAssigned) {
    }

    private record ManyToManySnapshot(Set<String> mathStudents, int physicsCount, String physicsName) {
    }

    private record ProductView(String sku, String name, long price, int stock) {
    }

    public static class ManyToManyStudent {
        @Id
        public Long id;
        public String name;

        @ManyToMany
        @JoinTable(name = "student_course", joinColumn = "student_id", inverseJoinColumn = "course_id")
        public List<ManyToManyCourse> courses;

        public ManyToManyStudent() {
        }

        public ManyToManyStudent(String name) {
            this.name = name;
        }
    }

    public static class ManyToManyCourse {
        @Id
        public Long id;
        public String name;

        @ManyToMany(mappedBy = "courses")
        public List<ManyToManyStudent> students;

        public ManyToManyCourse() {
        }

        public ManyToManyCourse(String name) {
            this.name = name;
        }
    }

    public interface ManyToManyStudentRepository extends MemrisRepository<ManyToManyStudent> {
        ManyToManyStudent save(ManyToManyStudent student);

        List<ManyToManyStudent> findByCoursesName(String name);
    }

    public interface ManyToManyCourseRepository extends MemrisRepository<ManyToManyCourse> {
        ManyToManyCourse save(ManyToManyCourse course);
    }

    public static class AnalyticsEvent {
        @Id
        public Long id;
        public String title;
        public boolean active;
        public byte priority;
        public short rating;
        public char grade;
        public int quantity;
        public long revenue;
        public float ratio;
        public double cost;
        public BigDecimal amount;
        public BigInteger bigCount;
        public Instant eventTime;
        public LocalDate shipDate;
        public LocalDateTime processedAt;
        public Date legacyDate;

        public AnalyticsEvent() {
        }

        public AnalyticsEvent(
            String title,
            boolean active,
            byte priority,
            short rating,
            char grade,
            int quantity,
            long revenue,
            float ratio,
            double cost,
            BigDecimal amount,
            BigInteger bigCount,
            Instant eventTime,
            LocalDate shipDate,
            LocalDateTime processedAt,
            Date legacyDate
        ) {
            this.title = title;
            this.active = active;
            this.priority = priority;
            this.rating = rating;
            this.grade = grade;
            this.quantity = quantity;
            this.revenue = revenue;
            this.ratio = ratio;
            this.cost = cost;
            this.amount = amount;
            this.bigCount = bigCount;
            this.eventTime = eventTime;
            this.shipDate = shipDate;
            this.processedAt = processedAt;
            this.legacyDate = legacyDate;
        }
    }

    private record HandlerCoverageSnapshot(
        int titleLike,
        int titleNotLike,
        int titleContaining,
        int titleNotContaining,
        int activeTrue,
        int activeFalse,
        int priorityGreaterThan,
        int ratingLessThan,
        int gradeGreaterThanEqual,
        int quantityLessThanEqual,
        int revenueGreaterThan,
        int ratioGreaterThan,
        int costLessThan,
        int amountEquals,
        int bigCountNot,
        int eventTimeAfter,
        int shipDateBefore,
        int processedAtGreaterThanEqual,
        int legacyDateLessThan
    ) {
    }

    private record StringOperatorSnapshot(
        int startingWith,
        int endingWith,
        int notContaining,
        int notLike,
        int titleIsNull,
        int legacyDateIsNotNull
    ) {
    }

    private record InOperatorSnapshot(
        int titleIn,
        int titleNotIn,
        int priorityIn,
        int quantityNotIn,
        int amountIn,
        int bigCountNotIn,
        int eventTimeIn,
        int shipDateNotIn,
        int processedAtIn,
        int legacyDateIsNull
    ) {
    }

    private record BetweenOperatorSnapshot(
        int titleNotStartingWith,
        int titleNotEndingWith,
        int eventTimeBetween,
        int shipDateBetween,
        int processedAtBetween,
        int legacyDateBetween
    ) {
    }

    private record OrOperatorSnapshot(
        int titleStartingWithOrQuantityGreaterThan,
        int activeFalseOrCostLessThan,
        int eventTimeGreaterThanOrLegacyDateIsNull
    ) {
    }

    private record OrderingSnapshot(
        int top2ByActiveSize,
        long top2FirstRevenue,
        String firstByActiveTitle
    ) {
    }

    private record DistinctSnapshot(
        int distinctDuplicate,
        int distinctUnique
    ) {
    }

    public interface AnalyticsEventRepository extends MemrisRepository<AnalyticsEvent> {
        AnalyticsEvent save(AnalyticsEvent event);

        List<AnalyticsEvent> findByTitleLike(String pattern);

        List<AnalyticsEvent> findByTitleNotLike(String pattern);

        List<AnalyticsEvent> findByTitleContaining(String keyword);

        List<AnalyticsEvent> findByTitleNotContaining(String keyword);

        List<AnalyticsEvent> findByTitleStartingWith(String prefix);

        List<AnalyticsEvent> findByTitleEndingWith(String suffix);

        List<AnalyticsEvent> findByTitleNotStartingWith(String prefix);

        List<AnalyticsEvent> findByTitleNotEndingWith(String suffix);

        List<AnalyticsEvent> findByTitleIn(List<String> titles);

        List<AnalyticsEvent> findByTitleNotIn(List<String> titles);

        List<AnalyticsEvent> findByTitleStartingWithOrQuantityGreaterThan(String prefix, int quantity);

        List<AnalyticsEvent> findByActiveTrue();

        List<AnalyticsEvent> findByActiveFalse();

        List<AnalyticsEvent> findByActiveFalseOrCostLessThan(double cost);

        List<AnalyticsEvent> findByPriorityGreaterThan(byte priority);

        List<AnalyticsEvent> findByPriorityIn(byte[] priorities);

        List<AnalyticsEvent> findByRatingLessThan(short rating);

        List<AnalyticsEvent> findByGradeGreaterThanEqual(char grade);

        List<AnalyticsEvent> findByQuantityLessThanEqual(int quantity);

        List<AnalyticsEvent> findByQuantityNotIn(List<Integer> quantities);

        List<AnalyticsEvent> findByRevenueGreaterThan(long revenue);

        List<AnalyticsEvent> findByRatioGreaterThan(float ratio);

        List<AnalyticsEvent> findByCostLessThan(double cost);

        List<AnalyticsEvent> findByAmount(BigDecimal amount);

        List<AnalyticsEvent> findByAmountIn(List<BigDecimal> amounts);

        List<AnalyticsEvent> findByBigCountNot(BigInteger bigCount);

        List<AnalyticsEvent> findByBigCountNotIn(List<BigInteger> bigCounts);

        List<AnalyticsEvent> findByEventTimeGreaterThan(Instant eventTime);

        List<AnalyticsEvent> findByEventTimeGreaterThanOrLegacyDateIsNull(Instant eventTime);

        List<AnalyticsEvent> findByEventTimeBetween(Instant start, Instant end);

        List<AnalyticsEvent> findByEventTimeIn(List<Instant> eventTimes);

        List<AnalyticsEvent> findByShipDateLessThan(LocalDate shipDate);

        List<AnalyticsEvent> findByShipDateBetween(LocalDate start, LocalDate end);

        List<AnalyticsEvent> findByShipDateNotIn(List<LocalDate> shipDates);

        List<AnalyticsEvent> findByProcessedAtGreaterThanEqual(LocalDateTime processedAt);

        List<AnalyticsEvent> findByProcessedAtBetween(LocalDateTime start, LocalDateTime end);

        List<AnalyticsEvent> findByProcessedAtIn(List<LocalDateTime> processedAt);

        List<AnalyticsEvent> findByLegacyDateLessThan(Date legacyDate);

        List<AnalyticsEvent> findByLegacyDateBetween(Date start, Date end);

        List<AnalyticsEvent> findByTitleIsNull();

        List<AnalyticsEvent> findByLegacyDateNotNull();

        List<AnalyticsEvent> findByLegacyDateIsNull();

        List<AnalyticsEvent> findTop2ByActiveTrueOrderByRevenueDesc();

        AnalyticsEvent findFirstByActiveTrueOrderByTitleAsc();

        List<AnalyticsEvent> findDistinctByTitle(String title);
    }
}
