package io.memris.spring;

import org.junit.jupiter.api.*;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.*;
import java.math.BigDecimal;

import static java.lang.StringTemplate.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Real-world E-Commerce System Test
 * 
 * This comprehensive test demonstrates a complex e-commerce domain model with:
 * - One-to-One: Customer <-> Account
 * - One-to-Many: Customer -> Orders, Category -> Products
 * - Many-to-One: Order -> Customer, OrderItem -> Product
 * - Many-to-Many: Product <-> Category, Order <-> Coupon
 * - Self-Referential: Category (parent/children hierarchical tree)
 * - Enum Mapping: OrderStatus (STRING), PaymentType (ORDINAL)
 * - Indexed Fields: email, sku, barcode, phone, name, slug, postalCode, code
 * - Embedded Types: Dimensions (inline, no separate table)
 * - Lifecycle Callbacks: @PrePersist, @PostLoad
 * - Transient Fields: Computed on load (fullName, discountedPrice)
 * - Composite Queries: findBy patterns with multiple conditions
 */
public class ECommerceRealWorldTest {
    static MemrisRepositoryFactory factory;
    
    // ====== ENTITY DEFINITIONS ======
    
    /** One-to-One: Customer has exactly one Account */
    @Entity
    public static class Account {
        @Id int id;
        @Index String email;
        String passwordHash;
        boolean emailVerified;
        LocalDateTime createdAt;
        LocalDateTime lastLoginAt;
        
        @OneToOne(mappedBy = "account")
        Customer customer;
    }
    
    /** One-to-One: Customer has one Account (bidirectional relationship) */
    @Entity
    public static class Customer {
        @Id int id;
        String firstName;
        String lastName;
        @Index String phone;
        
        @OneToOne
        Account account;  // Foreign key: account_id
        
        @OneToMany(mappedBy = "customer")
        List<Order> orders = new ArrayList<>();
        
        @ManyToOne
        Address billingAddress;
        
        @ManyToOne
        Address shippingAddress;
        
        /** Transient field computed via @PostLoad callback */
        @Transient String fullName;
        
        @PostLoad
        void onLoad() {
            fullName = "%s %s".formatted(firstName, lastName);
        }
    }
    
    /** Self-referential entity for hierarchical category tree */
    @Entity
    public static class Category {
        @Id int id;
        @Index String name;
        @Index String slug;
        String description;
        
        /** Parent category (self-referential Many-to-One) */
        @ManyToOne
        Category parent;
        
        /** Child categories (self-referential One-to-Many) */
        @OneToMany(mappedBy = "parent")
        List<Category> children = new ArrayList<>();
        
        @OneToMany
        List<Product> products = new ArrayList<>();
    }
    
    /** Product with many-to-many categories and multiple indexed fields */
    @Entity
    public static class Product {
        @Id int id;
        @Index String name;
        @Index String sku;  // Stock Keeping Unit - indexed for fast lookup
        @Index String barcode;  // Indexed for barcode scanning
        String description;
        
        BigDecimal price;
        int stockQuantity;
        double weightKg;
        
        /** @Embeddable: Inline component, no separate table */
        Dimensions dimensions;
        
        @ManyToOne
        Supplier supplier;
        
        /** Many-to-Many: Product belongs to multiple categories */
        @ManyToMany
        List<Category> categories = new ArrayList<>();
        
        @OneToMany(mappedBy = "product")
        List<OrderItem> orderItems = new ArrayList<>();
        
        LocalDateTime createdAt;
        
        /** Transient field for computed discount price */
        @Transient BigDecimal discountedPrice;
        
        @PrePersist
        void onCreate() {
            createdAt = LocalDateTime.now();
        }
    }
    
    /** @Embeddable: Inline component (flattened into parent table) */
    @Embeddable
    public static class Dimensions {
        double lengthCm;
        double widthCm;
        double heightCm;
    }
    
    /** Supplier with one-to-many products */
    @Entity
    public static class Supplier {
        @Id int id;
        @Index String name;  // Company name indexed
        String contactEmail;
        String contactPhone;
        String website;
        
        @OneToMany(mappedBy = "supplier")
        List<Product> products = new ArrayList<>();
        
        @ManyToOne
        Address address;
    }
    
    /** Address entity referenced by multiple entities */
    @Entity
    public static class Address {
        @Id int id;
        String street;
        String city;
        String state;
        @Index String postalCode;  // Indexed for shipping calculations
        String country;
        double latitude;
        double longitude;
        String addressType;  // BILLING, SHIPPING, BOTH
    }
    
    /** Order with enum mapping and complex relationships */
    @Entity
    public static class Order {
        @Id int id;
        String orderNumber;  // e.g., "ORD-2024-001234"
        LocalDateTime orderDate;
        LocalDateTime shippedAt;
        LocalDateTime deliveredAt;
        
        /** Enum mapped as STRING (stores enum name) */
        @Enumerated(EnumType.STRING)
        OrderStatus status;
        
        /** Enum mapped as ORDINAL (stores enum index: 0, 1, 2...) */
        @Enumerated(EnumType.ORDINAL)
        PaymentType paymentType;
        
        BigDecimal subtotal;
        BigDecimal taxAmount;
        BigDecimal shippingCost;
        BigDecimal discountAmount;
        BigDecimal totalAmount;
        
        @ManyToOne
        Customer customer;
        
        @OneToMany(mappedBy = "order")
        List<OrderItem> items = new ArrayList<>();
        
        /** Many-to-Many: Order can use multiple coupons */
        @ManyToMany
        List<Coupon> appliedCoupons = new ArrayList<>();
        
        @PrePersist
        void generateOrderNumber() {
            if (orderNumber == null) {
                orderNumber = "ORD-" + java.time.Year.now().getValue() + "-" + 
                    String.format("%06d", new Random().nextInt(999999));
            }
        }
    }
    
    /** Order item with many-to-one relationships */
    @Entity
    public static class OrderItem {
        @Id int id;
        int quantity;
        BigDecimal unitPrice;
        BigDecimal discountPercent;
        BigDecimal lineTotal;
        
        @ManyToOne
        Order order;
        
        @ManyToOne
        Product product;
    }
    
    /** Many-to-Many: Coupon can be applied to multiple orders */
    @Entity
    public static class Coupon {
        @Id int id;
        @Index String code;  // e.g., "SAVE20", "HOLIDAY10" - indexed for fast lookup
        String description;
        BigDecimal discountPercent;
        BigDecimal discountAmount;
        LocalDateTime validFrom;
        LocalDateTime validUntil;
        int maxUsageCount;
        int currentUsageCount;
        boolean active;
    }
    
    /** Order status enum (mapped as STRING) */
    public enum OrderStatus {
        PENDING, CONFIRMED, PROCESSING, SHIPPED, DELIVERED, CANCELLED, REFUNDED
    }
    
    /** Payment type enum (mapped as ORDINAL) */
    public enum PaymentType {
        CREDIT_CARD, DEBIT_CARD, PAYPAL, BANK_TRANSFER, COD
    }
    
    // ====== CUSTOM REPOSITORY INTERFACES ======
    /** Dynamic query methods for Customer entity */
    public interface CustomerRepository extends MemrisRepository<Customer> {
        Customer findByAccountEmail(String email);
        List<Customer> findByPhone(String phone);
        List<Customer> findByFirstNameAndLastName(String firstName, String lastName);
        List<Customer> findByLastNameContaining(String namePart);
        List<Customer> findByIdGreaterThan(int id);
        Customer findById(int id);  // Add missing method
        void save(Customer c);
        long count();
    }
    
    /** Dynamic query methods for Product entity */
    public interface ProductRepository extends MemrisRepository<Product> {
        Product findBySku(String sku);
        Product findByBarcode(String barcode);
        List<Product> findByName(String name);
        List<Product> findByPriceGreaterThan(BigDecimal price);
        List<Product> findByPriceLessThan(BigDecimal price);
        List<Product> findByPriceBetween(BigDecimal min, BigDecimal max);
        List<Product> findByStockQuantityLessThan(int threshold);
        // Multi-condition query - now supported!
        List<Product> findByNameContainingAndPriceLessThan(String namePart, BigDecimal maxPrice);
        List<Product> findBySkuStartingWith(String prefix);
        List<Product> findByNameIn(List<String> names);
        void save(Product p);
        long count();
        List<Product> findAll();
    }
    
    /** Dynamic query methods for Category entity */
    public interface CategoryRepository extends MemrisRepository<Category> {
        List<Category> findByParentIsNull();  // IsNull condition supported!
        List<Category> findByParentId(int parentId);
        Category findBySlug(String slug);
        List<Category> findByNameContaining(String keyword);
        void save(Category c);
        List<Category> findAll();
        long count();
    }
    
    /** Dynamic query methods for Order entity */
    public interface OrderRepository extends MemrisRepository<Order> {
        Order findByOrderNumber(String orderNumber);
        List<Order> findByStatus(OrderStatus status);
        List<Order> findByStatusInOrderStatusList(List<OrderStatus> statuses);
        List<Order> findByOrderDateBetween(LocalDateTime start, LocalDateTime end);
        List<Order> findByOrderDateAfter(LocalDateTime date);
        List<Order> findByOrderDateBefore(LocalDateTime date);
        List<Order> findByCustomerId(int customerId);
        List<Order> findByTotalAmountGreaterThan(BigDecimal amount);
        List<Order> findByTotalAmountBetween(BigDecimal min, BigDecimal max);
        // Multi-condition query - now supported!
        List<Order> findByStatusAndTotalAmountGreaterThan(OrderStatus status, BigDecimal amount);
        Order findById(int id);
        void save(Order o);
        long count();
    }
    
    /** Dynamic query methods for Coupon entity */
    public interface CouponRepository extends MemrisRepository<Coupon> {
        Coupon findByCode(String code);
        List<Coupon> findByActiveTrue();
        // Multi-condition queries now supported!
        List<Coupon> findByActiveTrueAndValidUntilAfter(LocalDateTime now);
        void save(Coupon c);
        long count();
    }
    
    // ====== TEST SETUP ======
    
    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        factory.close();
    }
    
    // ====== MAIN INTEGRATION TEST ======
    
    @Test
    void testEcommerceComplexScenario() {
        System.out.println("=== E-Commerce Real-World Integration Test ===\n");
        
        // Create repositories with dynamic query methods
            CustomerRepository customerRepo = factory.createJPARepository(CustomerRepository.class);
            ProductRepository productRepo = factory.createJPARepository(ProductRepository.class);
            CategoryRepository categoryRepo = factory.createJPARepository(CategoryRepository.class);
            OrderRepository orderRepo = factory.createJPARepository(OrderRepository.class);
            CouponRepository couponRepo = factory.createJPARepository(CouponRepository.class);
        
        // === 1. Create Category Hierarchy (Self-Referential) ===
        System.out.println("Creating category hierarchy (self-referential)...");
        
        Category electronics = new Category();
        electronics.name = "Electronics";
        electronics.slug = "electronics";
        electronics.description = "Electronic devices and accessories";
        categoryRepo.save(electronics);
        
        Category computers = new Category();
        computers.name = "Computers";
        computers.slug = "computers";
        computers.parent = electronics;  // Self-reference
        categoryRepo.save(computers);
        
        Category phones = new Category();
        phones.name = "Phones";
        phones.slug = "phones";
        phones.parent = electronics;  // Self-reference
        categoryRepo.save(phones);
        
        Category clothing = new Category();
        clothing.name = "Clothing";
        clothing.slug = "clothing";
        categoryRepo.save(clothing);
        
        // Verify hierarchy
        List<Category> allCategories = categoryRepo.findAll();
        long rootCount = allCategories.stream().filter(c -> c.parent == null).count();
        assertEquals(2, rootCount);  // Electronics and Clothing
        
        List<Category> electronicsChildren = categoryRepo.findByParentId(electronics.id);
        assertEquals(2, electronicsChildren.size());  // Computers and Phones
        
        System.out.println("✓ Created " + categoryRepo.count() + " categories");
        System.out.println("  - Root categories: " + rootCount);
        System.out.println("  - Electronics children: " + electronicsChildren.size() + "\n");
        
        // === 2. Create Products with Indexed Fields ===
        System.out.println("Creating products with indexed fields...");
        
        Product laptop = new Product();
        laptop.name = "ProBook Laptop 15";
        laptop.sku = "PRO-LP-001";  // Indexed
        laptop.barcode = "8901234567890";  // Indexed
        laptop.description = "High-performance laptop with 16GB RAM";
        laptop.price = new BigDecimal("1299.99");
        laptop.stockQuantity = 50;
        laptop.weightKg = 2.5;
        laptop.dimensions = new Dimensions();
        laptop.dimensions.lengthCm = 35.0;
        laptop.dimensions.widthCm = 24.0;
        laptop.dimensions.heightCm = 2.0;
        laptop.categories.add(computers);
        productRepo.save(laptop);
        
        Product phone = new Product();
        phone.name = "SmartPhone X Pro";
        phone.sku = "SP-XP-001";  // Indexed
        phone.barcode = "8901234567891";  // Indexed
        phone.description = "Latest smartphone with AI camera";
        phone.price = new BigDecimal("999.99");
        phone.stockQuantity = 100;
        phone.weightKg = 0.2;
        phone.dimensions = new Dimensions();
        phone.dimensions.lengthCm = 15.0;
        phone.dimensions.widthCm = 7.5;
        phone.dimensions.heightCm = 0.8;
        phone.categories.add(phones);
        productRepo.save(phone);
        
        Product jacket = new Product();
        jacket.name = "Winter Jacket XL";
        jacket.sku = "WTR-JK-001";
        jacket.barcode = "8901234567892";
        jacket.description = "Warm winter jacket";
        jacket.price = new BigDecimal("149.99");
        jacket.stockQuantity = 200;
        jacket.weightKg = 1.2;
        productRepo.save(jacket);
        
        System.out.println("✓ Created " + productRepo.count() + " products\n");
        
        // === 3. Create Customers with One-to-One Account ===
        System.out.println("Creating customers with One-to-One account...");
        
        Account acc1 = new Account();
        acc1.email = "john.doe@example.com";  // Indexed
        acc1.passwordHash = "hashed_password_123";
        acc1.emailVerified = true;
        acc1.createdAt = LocalDateTime.now().minusDays(30);
        factory.getTable(Account.class).insert(
            acc1.id, acc1.email, acc1.passwordHash, acc1.emailVerified,
            acc1.createdAt, acc1.lastLoginAt
        );
        
        Customer john = new Customer();
        john.firstName = "John";
        john.lastName = "Doe";
        john.phone = "+1-555-1234";  // Indexed
        john.account = acc1;
        customerRepo.save(john);
        
        Account acc2 = new Account();
        acc2.email = "jane.smith@example.com";  // Indexed
        acc2.passwordHash = "hashed_password_456";
        acc2.emailVerified = true;
        acc2.createdAt = LocalDateTime.now().minusDays(15);
        factory.getTable(Account.class).insert(
            acc2.id, acc2.email, acc2.passwordHash, acc2.emailVerified,
            acc2.createdAt, acc2.lastLoginAt
        );
        
        Customer jane = new Customer();
        jane.firstName = "Jane";
        jane.lastName = "Smith";
        jane.phone = "+1-555-5678";
        jane.account = acc2;
        customerRepo.save(jane);
        
        System.out.println("✓ Created " + customerRepo.count() + " customers\n");
        
        // === 4. Create Coupons with Indexed Code ===
        System.out.println("Creating coupons with indexed codes...");
        
        Coupon save20 = new Coupon();
        save20.code = "SAVE20";  // Indexed
        save20.description = "20% off your order";
        save20.discountPercent = new BigDecimal("20");
        save20.validFrom = LocalDateTime.now().minusDays(1);
        save20.validUntil = LocalDateTime.now().plusDays(30);
        save20.maxUsageCount = 100;
        save20.active = true;
        couponRepo.save(save20);
        
        Coupon holiday10 = new Coupon();
        holiday10.code = "HOLIDAY10";  // Indexed
        holiday10.description = "$10 off orders over $100";
        holiday10.discountAmount = new BigDecimal("10");
        holiday10.validFrom = LocalDateTime.now();
        holiday10.validUntil = LocalDateTime.now().plusDays(7);
        holiday10.maxUsageCount = 50;
        holiday10.active = true;
        couponRepo.save(holiday10);
        
        System.out.println("✓ Created " + couponRepo.count() + " coupons\n");
        
        // === 5. Create Orders with Complex Relationships ===
        System.out.println("Creating orders with Many-to-One and Many-to-Many...");
        
        Order order1 = new Order();
        order1.orderDate = LocalDateTime.now().minusDays(2);
        order1.status = OrderStatus.DELIVERED;  // Enum (STRING)
        order1.paymentType = PaymentType.CREDIT_CARD;  // Enum (ORDINAL)
        order1.subtotal = new BigDecimal("1499.99");
        order1.taxAmount = new BigDecimal("135.00");
        order1.shippingCost = new BigDecimal("9.99");
        order1.discountAmount = BigDecimal.ZERO;
        order1.totalAmount = order1.subtotal.add(order1.taxAmount).add(order1.shippingCost);
        order1.customer = john;
        order1.shippedAt = order1.orderDate.plusDays(1);
        order1.deliveredAt = order1.shippedAt.plusDays(2);
        orderRepo.save(order1);
        
        Order order2 = new Order();
        order2.orderDate = LocalDateTime.now().minusDays(1);
        order2.status = OrderStatus.SHIPPED;
        order2.paymentType = PaymentType.PAYPAL;
        order2.subtotal = new BigDecimal("149.99");
        order2.taxAmount = new BigDecimal("13.50");
        order2.shippingCost = new BigDecimal("4.99");
        order2.discountAmount = new BigDecimal("10.00");  // HOLIDAY10 coupon
        order2.totalAmount = order2.subtotal.add(order2.taxAmount)
            .add(order2.shippingCost).subtract(order2.discountAmount);
        order2.customer = jane;
        order2.appliedCoupons.add(holiday10);  // Many-to-Many
        order2.shippedAt = LocalDateTime.now();
        orderRepo.save(order2);
        
        System.out.println("✓ Created " + orderRepo.count() + " orders\n");
        
        // === 6. RUN QUERY TESTS ===
        System.out.println("=== Running 15 Query Tests ===\n");
        
        // Test 1: Find product by indexed SKU
        Product foundLaptop = productRepo.findBySku("PRO-LP-001");
        assertNotNull(foundLaptop);
        assertEquals("ProBook Laptop 15", foundLaptop.name);
        System.out.println("✓ Test 1: Find by SKU (indexed) - " + foundLaptop.name);
        
        // Test 2: Find products by price range - using BETWEEN (now supported!)
        List<Product> midPriceProducts = productRepo.findByPriceBetween(
            new BigDecimal("500"), new BigDecimal("1000"));
        assertEquals(1, midPriceProducts.size());
        assertEquals("SmartPhone X Pro", midPriceProducts.get(0).name);
        System.out.println("✓ Test 2: Price range ($500-$1000) - found " + midPriceProducts.size());
        
        // Test 3: Find products with low stock
        List<Product> lowStock = productRepo.findByStockQuantityLessThan(75);
        assertTrue(lowStock.stream().anyMatch(p -> p.name.equals("ProBook Laptop 15")));
        System.out.println("✓ Test 3: Low stock (< 75) - found " + lowStock.size());
        
        // Test 4: Find orders by status
        List<Order> deliveredOrders = orderRepo.findByStatus(OrderStatus.DELIVERED);
        assertEquals(1, deliveredOrders.size());
        assertEquals(order1.orderNumber, deliveredOrders.get(0).orderNumber);
        System.out.println("✓ Test 4: Orders by status (DELIVERED) - found " + deliveredOrders.size());
        
        // Test 5: Find orders by amount range
        List<Order> expensiveOrders = orderRepo.findByTotalAmountGreaterThan(new BigDecimal("1400"));
        assertEquals(1, expensiveOrders.size());
        System.out.println("✓ Test 5: Orders by amount (>$1400) - found " + expensiveOrders.size());
        
        // Test 6: Find customers by name pattern
        List<Customer> smiths = customerRepo.findByLastNameContaining("Sm");
        assertEquals(1, smiths.size());
        assertEquals("Smith", smiths.get(0).lastName);
        System.out.println("✓ Test 6: Customer name pattern (contains 'Sm') - found " + smiths.size());
        
        // Test 7: Find category by slug (indexed)
        Category comps = categoryRepo.findBySlug("computers");
        assertNotNull(comps);
        assertEquals("Computers", comps.name);
        System.out.println("✓ Test 7: Category by slug (indexed) - " + comps.name);
        
        // Test 8: Find active coupons
        List<Coupon> activeCoupons = couponRepo.findByActiveTrue();
        assertEquals(2, activeCoupons.size());
        System.out.println("✓ Test 8: Active coupons - found " + activeCoupons.size());
        
        // Test 9: Find root categories (using findByParentIsNull - now supported!)
        List<Category> roots = categoryRepo.findByParentIsNull();
        assertEquals(2, roots.size());
        System.out.println("✓ Test 9: Root categories (parent=null) - found " + roots.size());
        
        // Test 10: Multi-condition query - findByStatusAndTotalAmountGreaterThan (now supported!)
        List<Order> highValueShipped = orderRepo.findByStatusAndTotalAmountGreaterThan(
            OrderStatus.SHIPPED, new BigDecimal("100"));
        assertEquals(1, highValueShipped.size());
        System.out.println("✓ Test 10: Status=SHIPPED AND amount>100 - found " + highValueShipped.size());
        
        // Test 11: Multi-condition query - findByNameContainingAndPriceLessThan (now supported!)
        List<Product> proProducts = productRepo.findByNameContainingAndPriceLessThan(
            "Pro", new BigDecimal("1500"));
        assertEquals(2, proProducts.size());
        System.out.println("✓ Test 11: Name contains 'Pro' AND price < $1500 - found " + proProducts.size());
        
        // Test 12: @PostLoad transient field
        Customer loadedJohn = customerRepo.findById(john.id);
        assertNotNull(loadedJohn.fullName);
        assertEquals("John Doe", loadedJohn.fullName);
        System.out.println("✓ Test 12: @PostLoad transient field - " + loadedJohn.fullName);
        
        // Test 13: Find orders in date range - using findByOrderDateBetween (now supported!)
        LocalDateTime threeDaysAgo = LocalDateTime.now().minusDays(3);
        LocalDateTime yesterday = LocalDateTime.now().minusDays(1);
        List<Order> recentOrders = orderRepo.findByOrderDateBetween(threeDaysAgo, yesterday);
        assertEquals(2, recentOrders.size());
        System.out.println("✓ Test 13: Orders in date range (3 days ago to yesterday) - found " + recentOrders.size());
        
        // Test 14: Enum mapping verification
        Order loadedOrder = orderRepo.findById(order1.id);
        assertEquals(OrderStatus.DELIVERED, loadedOrder.status);  // STRING mapping
        assertEquals(PaymentType.CREDIT_CARD, loadedOrder.paymentType);  // ORDINAL mapping
        System.out.println("✓ Test 14: Enum mapping - status=" + loadedOrder.status + ", payment=" + loadedOrder.paymentType);
        
        // Test 15: Find customer by related entity field
        Customer foundByEmail = customerRepo.findByAccountEmail("john.doe@example.com");
        assertNotNull(foundByEmail);
        assertEquals("John", foundByEmail.firstName);
        System.out.println("✓ Test 15: Find by related entity (account email) - " + foundByEmail.firstName + " " + foundByEmail.lastName);
        
        // === TDD Tests for Unsupported Patterns ===
        
        // Test 16: findByPriceBetween (BETWEEN clause)
        List<Product> priceRangeProducts = productRepo.findByPriceBetween(new BigDecimal("100"), new BigDecimal("1000"));
        assertEquals(2, priceRangeProducts.size());  // SmartPhone ($999.99) and Jacket ($149.99)
        System.out.println("✓ Test 16: Price BETWEEN $100-$1000 - found " + priceRangeProducts.size());
        
        // Test 17: findByTotalAmountBetween (BETWEEN clause)
        List<Order> amountRangeOrders = orderRepo.findByTotalAmountBetween(new BigDecimal("100"), new BigDecimal("1600"));
        assertEquals(2, amountRangeOrders.size());  // Both orders
        System.out.println("✓ Test 17: Total amount BETWEEN $100-$1600 - found " + amountRangeOrders.size());
        
        // Test 18: findByStatusInOrderStatusList (IN clause)
        List<OrderStatus> statuses = List.of(OrderStatus.SHIPPED, OrderStatus.DELIVERED);
        List<Order> statusInOrders = orderRepo.findByStatusInOrderStatusList(statuses);
        assertEquals(2, statusInOrders.size());  // Both orders
        System.out.println("✓ Test 18: Status IN (SHIPPED, DELIVERED) - found " + statusInOrders.size());
        
        // Test 19: findByNameIn (IN clause)
        List<String> productNames = List.of("ProBook Laptop 15", "SmartPhone X Pro");
        List<Product> nameInProducts = productRepo.findByNameIn(productNames);
        assertEquals(2, nameInProducts.size());
        System.out.println("✓ Test 19: Name IN (ProBook, SmartPhone) - found " + nameInProducts.size());
        
        // Test 20: findBySkuStartingWith (LIKE prefix)
        List<Product> skuPrefixProducts = productRepo.findBySkuStartingWith("PRO-");
        assertEquals(1, skuPrefixProducts.size());  // Only ProBook Laptop
        assertEquals("ProBook Laptop 15", skuPrefixProducts.get(0).name);
        System.out.println("✓ Test 20: SKU starting with 'PRO-' - found " + skuPrefixProducts.size());
        
        // === Summary ===
        System.out.println("\n=== Test Summary ===");
        System.out.println("Categories: " + categoryRepo.count());
        System.out.println("Products: " + productRepo.count());
        System.out.println("Customers: " + customerRepo.count());
        System.out.println("Orders: " + orderRepo.count());
        System.out.println("Coupons: " + couponRepo.count());
        System.out.println("\n✅ All 15 real-world query tests passed!");
    }
    
    // ====== INDEX PERFORMANCE TEST ======
    
    @Test
    void testIndexPerformance() {
        System.out.println("\n=== Index Performance Test ===\n");
        
        ProductRepository productRepo = factory.createJPARepository(ProductRepository.class);
        
        // Create 100 products for index testing
        for (int i = 0; i < 100; i++) {
            Product p = new Product();
            p.name = "Product " + i;
            p.sku = "SKU-" + String.format("%04d", i);  // Indexed
            p.barcode = "BC" + String.format("%010d", i);  // Indexed
            p.price = new BigDecimal(10 + (i % 90));
            p.stockQuantity = i;
            productRepo.save(p);
        }
        
        // Test indexed SKU lookup
        long start = System.nanoTime();
        Product found = productRepo.findBySku("SKU-0050");
        long elapsed = System.nanoTime() - start;
        assertNotNull(found);
        System.out.println("Indexed SKU lookup (SKU-0050): " + (elapsed / 1000) + " μs");
        
        // Test indexed barcode lookup
        start = System.nanoTime();
        Product byBarcode = productRepo.findByBarcode("BC0000000050");
        elapsed = System.nanoTime() - start;
        assertNotNull(byBarcode);
        System.out.println("Indexed barcode lookup: " + (elapsed / 1000) + " μs");
        
        // Test range query (uses RangeIndex/BTree)
        start = System.nanoTime();
        List<Product> priceRange = productRepo.findByPriceBetween(new BigDecimal("25"), new BigDecimal("75"));
        elapsed = System.nanoTime() - start;
        assertFalse(priceRange.isEmpty());
        System.out.println("Range query (price 25-75): " + (elapsed / 1000) + " μs, found " + priceRange.size());
        
        System.out.println("\n✅ Index performance test complete!");
    }
}
