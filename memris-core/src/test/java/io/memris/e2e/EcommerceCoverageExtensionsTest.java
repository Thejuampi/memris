package io.memris.e2e;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.JoinColumn;
import io.memris.core.JoinTable;
import io.memris.core.ManyToMany;
import io.memris.core.ManyToOne;
import io.memris.core.OneToMany;
import io.memris.core.PostLoad;
import io.memris.core.Query;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class EcommerceCoverageExtensionsTest {

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
    void shouldOrderByNullableNumericColumnsAcrossMultiColumnSorts() {
        SortMatrixRepository repo = arena.createRepository(SortMatrixRepository.class);
        seedSortMatrix(repo);

        List<SortMatrixEntity> byNullableInt = repo.findByOrderByNullableIntAscAndNameAsc();
        List<SortMatrixEntity> byNullableLong = repo.findByOrderByNullableLongAscAndNameAsc();
        List<SortMatrixEntity> byNullableFloat = repo.findByOrderByNullableFloatAscAndNameAsc();
        List<SortMatrixEntity> byNullableDouble = repo.findByOrderByNullableDoubleAscAndNameAsc();

        assertThat(byNullableInt).hasSize(5);
        assertThat(byNullableLong).hasSize(5);
        assertThat(byNullableFloat).hasSize(5);
        assertThat(byNullableDouble).hasSize(5);
        assertThat(byNullableDouble).extracting(entity -> entity.name).containsExactlyInAnyOrder("A", "B", "C", "D", "E");
    }

    @Test
    void shouldOrderByPrimitiveNumericColumnsAcrossMultiColumnSorts() {
        SortMatrixRepository repo = arena.createRepository(SortMatrixRepository.class);
        seedSortMatrix(repo);

        List<SortMatrixEntity> byPrimitiveInt = repo.findByOrderByPrimitiveIntAscAndNameAsc();
        List<SortMatrixEntity> byPrimitiveLong = repo.findByOrderByPrimitiveLongAscAndNameAsc();
        List<SortMatrixEntity> byPrimitiveFloat = repo.findByOrderByPrimitiveFloatAscAndNameAsc();
        List<SortMatrixEntity> byPrimitiveDouble = repo.findByOrderByPrimitiveDoubleAscAndNameAsc();
        List<SortMatrixEntity> byPrimitiveDoubleDesc = repo.findAllOrderByPrimitiveDoubleDescAndNameAsc();

        assertThat(byPrimitiveInt).hasSize(5);
        assertThat(byPrimitiveLong).hasSize(5);
        assertThat(byPrimitiveFloat).hasSize(5);
        assertThat(byPrimitiveDouble).hasSize(5);

        assertThat(byPrimitiveDoubleDesc).hasSize(5);
        assertThat(byPrimitiveDoubleDesc.get(0).primitiveDouble)
                .isGreaterThanOrEqualTo(byPrimitiveDoubleDesc.get(byPrimitiveDoubleDesc.size() - 1).primitiveDouble);
    }

    @Test
    void shouldHydrateManyToOneUsingReferencedNonIdColumn() {
        CodeAccountRepository accountRepo = arena.createRepository(CodeAccountRepository.class);
        CodeInvoiceRepository invoiceRepo = arena.createRepository(CodeInvoiceRepository.class);

        CodeAccount acme = accountRepo.save(new CodeAccount(1, 1, "Acme"));
        CodeAccount beta = accountRepo.save(new CodeAccount(2, 2, "Beta"));

        CodeInvoice invoice = new CodeInvoice(1, "INV-10");
        invoice.account = acme;
        invoiceRepo.save(invoice);
        CodeInvoice second = new CodeInvoice(2, "INV-20");
        second.account = beta;
        invoiceRepo.save(second);

        List<CodeInvoice> byAccountName = invoiceRepo.findByAccountName("Acme");
        List<CodeInvoice> byAccountCode = invoiceRepo.findByAccountExternalCode(1);

        assertThat(byAccountName).hasSize(1);
        assertThat(byAccountName.get(0).reference).isEqualTo("INV-10");
        assertThat(byAccountName.get(0).account).isNotNull();
        assertThat(byAccountName.get(0).account.id).isEqualTo(acme.id);
        assertThat(byAccountName.get(0).account.loadedByPostLoad).isTrue();

        assertThat(byAccountCode).hasSize(1);
        assertThat(byAccountCode.get(0).account.externalCode).isEqualTo(1);
    }

    @Test
    void shouldHydrateOneToManySetAndInvokePostLoad() {
        SetCustomerRepository customerRepo = arena.createRepository(SetCustomerRepository.class);
        SetOrderRepository orderRepo = arena.createRepository(SetOrderRepository.class);

        SetCustomer customer = customerRepo.save(new SetCustomer("Set Customer"));
        orderRepo.save(new SetOrder(customer, "PAID"));
        orderRepo.save(new SetOrder(customer, "PENDING"));

        List<SetCustomer> results = customerRepo.findByOrdersStatus("PAID");
        assertThat(results).hasSize(1);

        SetCustomer resolved = results.get(0);
        assertThat(resolved.orders).isInstanceOf(Set.class);
        assertThat(resolved.orders).extracting(order -> order.status).contains("PAID", "PENDING");
        assertThat(resolved.orders).allMatch(order -> order.loadedByPostLoad);
    }

    @Test
    void shouldFilterManyToManyAcrossNonLongIds() {
        StudentIntRepository studentRepo = arena.createRepository(StudentIntRepository.class);
        CourseIntRepository courseRepo = arena.createRepository(CourseIntRepository.class);

        CourseInt math = new CourseInt(100, "Math");
        CourseInt physics = new CourseInt(200, "Physics");
        courseRepo.save(math);
        courseRepo.save(physics);

        StudentInt alice = new StudentInt(1, "Alice");
        alice.courses = List.of(math, physics);
        studentRepo.save(alice);

        StudentInt bob = new StudentInt(2, "Bob");
        bob.courses = List.of(math);
        studentRepo.save(bob);

        List<StudentInt> mathStudents = studentRepo.findByCoursesName("Math");
        List<StudentInt> physicsStudents = studentRepo.findByCoursesName("Physics");

        assertThat(mathStudents).extracting(student -> student.name).containsExactlyInAnyOrder("Alice", "Bob");
        assertThat(physicsStudents).extracting(student -> student.name).containsExactly("Alice");
    }

    @Test
    void shouldRoundTripMaterializationForWideTypeEntityWithNulls() {
        WideEntityRepository repo = arena.createRepository(WideEntityRepository.class);

        WideEntity full = WideEntity.full("full-row");
        WideEntity partial = WideEntity.partial("partial-row");

        repo.save(full);
        repo.save(partial);

        WideEntity loadedFull = repo.findByTitle("full-row").get(0);
        WideEntity loadedPartial = repo.findByTitle("partial-row").get(0);

        assertThat(loadedFull.id).isNotNull();
        assertThat(loadedFull.window).isNotNull();
        assertThat(loadedFull.window.startDate).isEqualTo(LocalDate.of(2025, 8, 1));
        assertThat(loadedFull.audit).isNotNull();
        assertThat(loadedFull.audit.createdBy).isEqualTo("system");
        assertThat(loadedFull.amount).isEqualByComparingTo(new BigDecimal("12.34"));
        assertThat(loadedFull.bigCount).isEqualTo(new BigInteger("1234"));

        assertThat(loadedPartial.title).isEqualTo("partial-row");
        assertThat(loadedPartial.legacyDate).isNull();
        assertThat(loadedPartial.window).isNull();
        assertThat(repo.findByLegacyDateIsNull()).extracting(entity -> entity.title).contains("partial-row");
    }

    @Test
    void shouldDispatchIndexedPredicatesAndNullOperators() {
        IndexedTicketRepository repo = arena.createRepository(IndexedTicketRepository.class);

        repo.save(new IndexedTicket("PENDING", 500L, "A", 1));
        repo.save(new IndexedTicket("PENDING", 1500L, null, 2));
        repo.save(new IndexedTicket("PAID", 3500L, "B", 3));
        repo.save(new IndexedTicket("PAID", 4500L, null, 4));
        repo.save(new IndexedTicket("CANCELLED", 250L, "C", 5));

        assertThat(repo.findByStatus("PENDING")).hasSize(2);
        assertThat(repo.findByAmountGreaterThan(1000L)).hasSize(3);
        assertThat(repo.findByAmountBetween(200L, 2000L)).hasSize(3);
        assertThat(repo.findByStatusIn(List.of("PENDING", "PAID"))).hasSize(4);
        assertThat(repo.findByTagIsNull()).hasSize(2);
        assertThat(repo.findByTagNotNull()).hasSize(3);
        assertThat(repo.findTop5ByStatusOrderByAmountDesc("PAID")).extracting(ticket -> ticket.amount)
                .containsExactly(4500L, 3500L);
    }

    @Test
    void shouldRemainCorrectUnderInsertUpdateDeleteReinsertChurn() {
        ChurnOrderRepository repo = arena.createRepository(ChurnOrderRepository.class);
        List<ChurnOrder> created = new ArrayList<>();

        for (int i = 0; i < 300; i++) {
            ChurnOrder order = new ChurnOrder(
                    (i % 2 == 0) ? "PENDING" : "PAID",
                    i * 10L,
                    "CUST-" + (i % 25),
                    1);
            created.add(repo.save(order));
        }

        for (int i = 0; i < 80; i++) {
            ChurnOrder order = created.get(i);
            order.status = "SHIPPED";
            order.total = order.total + 500L;
            order.version = 2;
            repo.save(order);
        }

        for (int i = 0; i < 40; i++) {
            repo.deleteById(created.get(i * 3).id);
        }

        for (int i = 0; i < 50; i++) {
            repo.save(new ChurnOrder("PENDING", 10_000L + i, "NEW-" + i, 1));
        }

        assertThat(repo.findAll()).hasSize(310);
        assertThat(repo.countByStatus("SHIPPED")).isGreaterThan(0);
        assertThat(repo.findByCustomerKey("CUST-1")).isNotEmpty();

        List<ChurnOrder> topPending = repo.findTop5ByStatusOrderByTotalDesc("PENDING");
        assertThat(topPending).hasSize(5);
        for (int i = 1; i < topPending.size(); i++) {
            assertThat(topPending.get(i - 1).total).isGreaterThanOrEqualTo(topPending.get(i).total);
        }

        Optional<ChurnOrder> sample = repo.findById(created.get(250).id);
        assertThat(sample).isPresent();
    }

    private static void seedSortMatrix(SortMatrixRepository repo) {
        repo.save(new SortMatrixEntity("A", true, null, 9L, 5.0f, 2.0, 4, 40L, 4.0f, 4.0));
        repo.save(new SortMatrixEntity("B", true, 1, 7L, null, 3.0, 1, 10L, 1.0f, 1.0));
        repo.save(new SortMatrixEntity("C", true, 1, null, 1.0f, null, 1, 10L, 1.0f, 0.5));
        repo.save(new SortMatrixEntity("D", true, 3, 3L, 2.0f, 1.5, 2, 20L, 2.0f, 2.0));
        repo.save(new SortMatrixEntity("E", true, 2, 1L, 0.5f, 0.75, 0, 5L, 0.5f, 0.75));
    }

    @Entity
    public static class SortMatrixEntity {
        @Id
        public Long id;
        public String name;
        public boolean active;

        public Integer nullableInt;
        public Long nullableLong;
        public Float nullableFloat;
        public Double nullableDouble;

        public int primitiveInt;
        public long primitiveLong;
        public float primitiveFloat;
        public double primitiveDouble;

        public SortMatrixEntity() {
        }

        public SortMatrixEntity(String name,
                boolean active,
                Integer nullableInt,
                Long nullableLong,
                Float nullableFloat,
                Double nullableDouble,
                int primitiveInt,
                long primitiveLong,
                float primitiveFloat,
                double primitiveDouble) {
            this.name = name;
            this.active = active;
            this.nullableInt = nullableInt;
            this.nullableLong = nullableLong;
            this.nullableFloat = nullableFloat;
            this.nullableDouble = nullableDouble;
            this.primitiveInt = primitiveInt;
            this.primitiveLong = primitiveLong;
            this.primitiveFloat = primitiveFloat;
            this.primitiveDouble = primitiveDouble;
        }
    }

    public interface SortMatrixRepository extends MemrisRepository<SortMatrixEntity> {
        SortMatrixEntity save(SortMatrixEntity entity);

        @Query("select s from SortMatrixEntity s order by s.nullableInt asc, s.name asc")
        List<SortMatrixEntity> findByOrderByNullableIntAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.nullableLong asc, s.name asc")
        List<SortMatrixEntity> findByOrderByNullableLongAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.nullableFloat asc, s.name asc")
        List<SortMatrixEntity> findByOrderByNullableFloatAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.nullableDouble asc, s.name asc")
        List<SortMatrixEntity> findByOrderByNullableDoubleAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.primitiveInt asc, s.name asc")
        List<SortMatrixEntity> findByOrderByPrimitiveIntAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.primitiveLong asc, s.name asc")
        List<SortMatrixEntity> findByOrderByPrimitiveLongAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.primitiveFloat asc, s.name asc")
        List<SortMatrixEntity> findByOrderByPrimitiveFloatAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.primitiveDouble asc, s.name asc")
        List<SortMatrixEntity> findByOrderByPrimitiveDoubleAscAndNameAsc();

        @Query("select s from SortMatrixEntity s order by s.primitiveDouble desc, s.name asc")
        List<SortMatrixEntity> findAllOrderByPrimitiveDoubleDescAndNameAsc();
    }

    @Entity
    public static class CodeAccount {
        @Id
        public Integer id;

        @Index(type = Index.IndexType.HASH)
        public Integer externalCode;

        public String name;
        public boolean loadedByPostLoad;

        public CodeAccount() {
        }

        public CodeAccount(Integer id, Integer externalCode, String name) {
            this.id = id;
            this.externalCode = externalCode;
            this.name = name;
        }

        @PostLoad
        public void markLoaded() {
            this.loadedByPostLoad = true;
        }
    }

    @Entity
    public static class CodeInvoice {
        @Id
        public Long id;
        public Integer accountCode;
        public String reference;

        @ManyToOne
        @JoinColumn(name = "account_code", referencedColumnName = "externalCode")
        public CodeAccount account;

        public CodeInvoice() {
        }

        public CodeInvoice(Integer accountCode, String reference) {
            this.accountCode = accountCode;
            this.reference = reference;
        }
    }

    public interface CodeAccountRepository extends MemrisRepository<CodeAccount> {
        CodeAccount save(CodeAccount account);
    }

    public interface CodeInvoiceRepository extends MemrisRepository<CodeInvoice> {
        CodeInvoice save(CodeInvoice invoice);

        List<CodeInvoice> findByAccountName(String name);

        List<CodeInvoice> findByAccountExternalCode(Integer externalCode);

        Optional<CodeInvoice> findById(Long id);
    }

    @Entity
    public static class SetCustomer {
        @Id
        public Long id;
        public String name;

        @OneToMany(mappedBy = "customer")
        public Set<SetOrder> orders;

        public SetCustomer() {
        }

        public SetCustomer(String name) {
            this.name = name;
        }
    }

    @Entity
    public static class SetOrder {
        @Id
        public Long id;
        public Long customerId;
        public String status;
        public boolean loadedByPostLoad;

        @ManyToOne
        @JoinColumn(name = "customer_id")
        public SetCustomer customer;

        public SetOrder() {
        }

        public SetOrder(SetCustomer customer, String status) {
            this.customer = customer;
            this.customerId = customer != null ? customer.id : null;
            this.status = status;
        }

        @PostLoad
        public void markLoaded() {
            this.loadedByPostLoad = true;
        }
    }

    public interface SetCustomerRepository extends MemrisRepository<SetCustomer> {
        SetCustomer save(SetCustomer customer);

        List<SetCustomer> findByOrdersStatus(String status);
    }

    public interface SetOrderRepository extends MemrisRepository<SetOrder> {
        SetOrder save(SetOrder order);
    }

    @Entity
    public static class StudentInt {
        @Id
        public Integer id;
        public String name;

        @ManyToMany
        @JoinTable(name = "student_int_course", joinColumn = "student_id", inverseJoinColumn = "course_id")
        public List<CourseInt> courses;

        public StudentInt() {
        }

        public StudentInt(Integer id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    @Entity
    public static class CourseInt {
        @Id
        public Integer id;
        public String name;

        @ManyToMany(mappedBy = "courses")
        public List<StudentInt> students;

        public CourseInt() {
        }

        public CourseInt(Integer id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public interface StudentIntRepository extends MemrisRepository<StudentInt> {
        StudentInt save(StudentInt student);

        List<StudentInt> findByCoursesName(String name);
    }

    public interface CourseIntRepository extends MemrisRepository<CourseInt> {
        CourseInt save(CourseInt course);
    }

    public static class ShippingWindow {
        public LocalDate startDate;
        public LocalDate endDate;

        public ShippingWindow() {
        }

        public ShippingWindow(LocalDate startDate, LocalDate endDate) {
            this.startDate = startDate;
            this.endDate = endDate;
        }
    }

    public static class AuditTrail {
        public String createdBy;
        public Long sequence;

        public AuditTrail() {
        }

        public AuditTrail(String createdBy, Long sequence) {
            this.createdBy = createdBy;
            this.sequence = sequence;
        }
    }

    @Entity
    public static class WideEntity {
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

        public Boolean optionalActive;
        public Byte optionalPriority;
        public Short optionalRating;
        public Character optionalGrade;
        public Integer optionalQuantity;
        public Long optionalRevenue;
        public Float optionalRatio;
        public Double optionalCost;

        public BigDecimal amount;
        public BigInteger bigCount;
        public Instant eventTime;
        public LocalDate shipDate;
        public LocalDateTime processedAt;
        public Date legacyDate;

        public ShippingWindow window;
        public AuditTrail audit;

        public WideEntity() {
        }

        public static WideEntity full(String title) {
            WideEntity entity = new WideEntity();
            entity.title = title;
            entity.active = true;
            entity.priority = (byte) 2;
            entity.rating = (short) 7;
            entity.grade = 'A';
            entity.quantity = 15;
            entity.revenue = 1500L;
            entity.ratio = 1.25f;
            entity.cost = 29.99;
            entity.optionalActive = Boolean.TRUE;
            entity.optionalPriority = (byte) 3;
            entity.optionalRating = (short) 8;
            entity.optionalGrade = 'B';
            entity.optionalQuantity = 16;
            entity.optionalRevenue = 1600L;
            entity.optionalRatio = 1.5f;
            entity.optionalCost = 31.25;
            entity.amount = new BigDecimal("12.34");
            entity.bigCount = new BigInteger("1234");
            entity.eventTime = Instant.parse("2025-08-01T00:00:00Z");
            entity.shipDate = LocalDate.of(2025, 8, 1);
            entity.processedAt = LocalDateTime.of(2025, 8, 1, 12, 30);
            entity.legacyDate = new Date(1754006400000L);
            entity.window = new ShippingWindow(LocalDate.of(2025, 8, 1), LocalDate.of(2025, 8, 3));
            entity.audit = new AuditTrail("system", 99L);
            return entity;
        }

        public static WideEntity partial(String title) {
            WideEntity entity = new WideEntity();
            entity.title = title;
            entity.active = false;
            entity.priority = (byte) 1;
            entity.rating = (short) 1;
            entity.grade = 'C';
            entity.quantity = 1;
            entity.revenue = 10L;
            entity.ratio = 0.1f;
            entity.cost = 1.0;
            entity.optionalActive = null;
            entity.optionalPriority = null;
            entity.optionalRating = null;
            entity.optionalGrade = null;
            entity.optionalQuantity = null;
            entity.optionalRevenue = null;
            entity.optionalRatio = null;
            entity.optionalCost = null;
            entity.amount = null;
            entity.bigCount = null;
            entity.eventTime = null;
            entity.shipDate = null;
            entity.processedAt = null;
            entity.legacyDate = null;
            entity.window = null;
            entity.audit = null;
            return entity;
        }
    }

    public interface WideEntityRepository extends MemrisRepository<WideEntity> {
        WideEntity save(WideEntity entity);

        Optional<WideEntity> findById(Long id);

        List<WideEntity> findByTitle(String title);

        List<WideEntity> findByLegacyDateIsNull();
    }

    @Entity
    public static class IndexedTicket {
        @Id
        public Long id;

        @Index(type = Index.IndexType.HASH)
        public String status;

        @Index(type = Index.IndexType.BTREE)
        public long amount;

        @Index(type = Index.IndexType.HASH)
        public String tag;

        public int priority;

        public IndexedTicket() {
        }

        public IndexedTicket(String status, long amount, String tag, int priority) {
            this.status = status;
            this.amount = amount;
            this.tag = tag;
            this.priority = priority;
        }
    }

    public interface IndexedTicketRepository extends MemrisRepository<IndexedTicket> {
        IndexedTicket save(IndexedTicket ticket);

        List<IndexedTicket> findByStatus(String status);

        List<IndexedTicket> findByAmountGreaterThan(long min);

        List<IndexedTicket> findByAmountBetween(long min, long max);

        List<IndexedTicket> findByStatusIn(List<String> statuses);

        List<IndexedTicket> findByTagIsNull();

        List<IndexedTicket> findByTagNotNull();

        List<IndexedTicket> findTop5ByStatusOrderByAmountDesc(String status);
    }

    @Entity
    public static class ChurnOrder {
        @Id
        public Long id;

        @Index(type = Index.IndexType.HASH)
        public String status;

        @Index(type = Index.IndexType.BTREE)
        public long total;

        @Index(type = Index.IndexType.HASH)
        public String customerKey;

        public int version;

        public ChurnOrder() {
        }

        public ChurnOrder(String status, long total, String customerKey, int version) {
            this.status = status;
            this.total = total;
            this.customerKey = customerKey;
            this.version = version;
        }
    }

    public interface ChurnOrderRepository extends MemrisRepository<ChurnOrder> {
        ChurnOrder save(ChurnOrder order);

        Optional<ChurnOrder> findById(Long id);

        List<ChurnOrder> findAll();

        List<ChurnOrder> findByStatus(String status);

        List<ChurnOrder> findByCustomerKey(String customerKey);

        List<ChurnOrder> findTop5ByStatusOrderByTotalDesc(String status);

        long countByStatus(String status);

        void deleteById(Long id);
    }
}
