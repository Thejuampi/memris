package io.memris.realworld;

import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.runtime.DepartmentAgeKey;
import io.memris.runtime.IndexedEntity;
import io.memris.runtime.IndexedEntityRepository;
import io.memris.runtime.TestEntity;
import io.memris.runtime.TestEntityRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Employee-directory workflows exercise CRUD, staffing searches, and workforce reports together. */
class RepositoryCrudAndGroupingRealWorldTest {
    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory(RealWorldSupport.configuration().build());
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() {
        factory.close();
    }

    @Test
    void shouldOnboardUpdateAndRemoveEmployeesIndividuallyAndInBatches() {
        var employees = arena.createRepository(TestEntityRepository.class);
        assertThat(employees.count()).isZero();
        assertThat(employees.findAll()).isEmpty();
        assertThat(employees.findById(Long.MAX_VALUE)).isEmpty();
        assertThat(employees.existsById(Long.MAX_VALUE)).isFalse();

        var alice = employees.save(new TestEntity(null, "Alice", 25, "Engineering"));
        var batch = employees.saveAll(List.of(new TestEntity(null, "Bob", 30, "Sales"),
                new TestEntity(null, "Cara", 35, "HR")));
        var draft = employees.save(new TestEntity(null, null, 25));
        var bob = batch.get(0);
        var cara = batch.get(1);
        assertThat(List.of(alice, bob, cara, draft)).extracting(row -> row.id).doesNotContainNull().doesNotHaveDuplicates();
        assertThat(employees.count()).isEqualTo(4);
        assertThat(employees.findAll()).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, bob.id, cara.id, draft.id);
        assertThat(employees.findById(alice.id)).get().usingRecursiveComparison().isEqualTo(alice);
        assertThat(employees.findById(draft.id)).get().extracting(row -> row.name).isNull();
        assertThat(employees.existsById(alice.id)).isTrue();
        assertThat(employees.findByIdIn(Set.of(alice.id, bob.id))).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, bob.id);
        assertThat(employees.findByIdIn(alice.id, bob.id)).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, bob.id);

        alice.name = "Alice Updated";
        alice.age = 26;
        assertThat(employees.save(alice).id).isEqualTo(alice.id);
        assertThat(employees.findById(alice.id)).get().usingRecursiveComparison().isEqualTo(alice);
        assertThat(employees.count()).isEqualTo(4);

        employees.deleteById(bob.id);
        employees.delete(cara);
        assertThat(employees.findById(bob.id)).isEmpty();
        assertThat(employees.existsById(cara.id)).isFalse();
        assertThat(employees.deleteAllById(List.of(alice.id, Long.MAX_VALUE))).isEqualTo(1);
        assertThat(employees.findAll()).extracting(row -> row.id).containsExactly(draft.id);
        employees.deleteAll();
        assertThat(employees.count()).isZero();
        assertThat(employees.findAll()).isEmpty();
    }

    @Test
    void shouldFindAndRankCandidatesForAStaffingSearch() {
        var employees = arena.createRepository(TestEntityRepository.class);
        var alice = employees.save(new TestEntity(null, "Alice", 25));
        var seniorAlice = employees.save(new TestEntity(null, "Alice", 35));
        var bob = employees.save(new TestEntity(null, "Bob", 30));
        var charlie = employees.save(new TestEntity(null, "Charlie", 40));
        var dora = employees.save(new TestEntity(null, "Dora", 45));

        assertThat(employees.findByName("Alice")).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, seniorAlice.id);
        assertThat(employees.findByNameIn(List.of("Alice", "Charlie"))).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, seniorAlice.id, charlie.id);
        assertThat(employees.findByNameIn(new String[] { "Bob" })).extracting(row -> row.id).containsExactly(bob.id);
        assertThat(employees.findByAgeGreaterThan(30)).extracting(row -> row.id)
                .containsExactlyInAnyOrder(seniorAlice.id, charlie.id, dora.id);
        assertThat(employees.findByAgeBetween(25, 35)).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, bob.id, seniorAlice.id);
        assertThat(employees.findByAgeIn(new int[] { 25, 40 })).extracting(row -> row.id)
                .containsExactlyInAnyOrder(alice.id, charlie.id);
        assertThat(employees.countByName("Alice")).isEqualTo(2);
        assertThat(employees.existsByName("Alice")).isTrue();
        assertThat(employees.findByNameAndAge("Alice", 35)).extracting(row -> row.id).containsExactly(seniorAlice.id);
        assertThat(employees.findByNameOrAge("Bob", 40)).extracting(row -> row.id)
                .containsExactlyInAnyOrder(bob.id, charlie.id);
        assertThat(employees.findByOrderByAgeAsc()).extracting(row -> row.id)
                .containsExactly(alice.id, bob.id, seniorAlice.id, charlie.id, dora.id);
        assertThat(employees.findTop2ByOrderByAgeAsc()).extracting(row -> row.id).containsExactly(alice.id, bob.id);
        assertThat(employees.findByName("Missing")).isEmpty();
        assertThat(employees.countByName("Missing")).isZero();
        assertThat(employees.existsByName("Missing")).isFalse();
    }

    @Test
    void shouldKeepAnIndexedDirectoryConsistentAcrossTeamTransfersAndDepartures() {
        var directory = arena.createRepository(IndexedEntityRepository.class);
        var transferred = directory.save(new IndexedEntity(null, "Engineering", 10));
        var departing = directory.save(new IndexedEntity(null, "Sales", 20));
        assertThat(directory.countByCategory("Engineering")).isEqualTo(1);
        assertThat(directory.findByCategory("Sales")).extracting(row -> row.id).containsExactly(departing.id);

        transferred.category = "Sales";
        directory.save(transferred);
        assertThat(directory.countByCategory("Engineering")).isZero();
        assertThat(directory.countByCategory("Sales")).isEqualTo(2);
        assertThat(directory.findByCategory("Sales")).extracting(row -> row.id)
                .containsExactlyInAnyOrder(transferred.id, departing.id);
        directory.delete(departing);
        assertThat(directory.findByCategory("Sales")).extracting(row -> row.id).containsExactly(transferred.id);
        assertThat(directory.countByCategory("Sales")).isEqualTo(1);
    }

    @Test
    void shouldBuildDepartmentAndAgeReportsWithDerivedQueriesJpqlAndHaving() {
        var employees = arena.createRepository(TestEntityRepository.class);
        var alice = employees.save(new TestEntity(null, "Alice", 25, "Engineering"));
        var seniorAlice = employees.save(new TestEntity(null, "Alice", 30, "Engineering"));
        var bob = employees.save(new TestEntity(null, "Bob", 25, "Engineering"));
        var charlie = employees.save(new TestEntity(null, "Charlie", 35, "Sales"));
        var david = employees.save(new TestEntity(null, "David", 35, "Sales"));
        var eve = employees.save(new TestEntity(null, "Eve", 45, "HR"));

        var departmentIds = Map.of("Engineering", Set.of(alice.id, seniorAlice.id, bob.id),
                "Sales", Set.of(charlie.id, david.id), "HR", Set.of(eve.id));
        assertThat(idsByGroup(employees.findAllGroupingByDepartment())).isEqualTo(departmentIds);
        assertThat(idsByGroup(employees.findAllGroupingByDepartmentAsSet())).isEqualTo(departmentIds);
        assertThat(employees.countByDepartment()).isEqualTo(Map.of("Engineering", 3L, "Sales", 2L, "HR", 1L));

        var engineering25 = new DepartmentAgeKey("Engineering", 25);
        var engineering30 = new DepartmentAgeKey("Engineering", 30);
        var sales35 = new DepartmentAgeKey("Sales", 35);
        var hr45 = new DepartmentAgeKey("HR", 45);
        var ageIds = Map.of(engineering25, Set.of(alice.id, bob.id), engineering30, Set.of(seniorAlice.id),
                sales35, Set.of(charlie.id, david.id), hr45, Set.of(eve.id));
        assertThat(idsByGroup(employees.findAllGroupingByDepartmentAndAge())).isEqualTo(ageIds);
        assertThat(idsByGroup(employees.findAllGroupedByDepartmentAndAgeJpql())).isEqualTo(ageIds);
        assertThat(employees.countByDepartmentAndAge())
                .isEqualTo(Map.of(engineering25, 2L, engineering30, 1L, sales35, 2L, hr45, 1L));
        assertThat(employees.countByNameGroupingByDepartmentAndAge("Alice"))
                .isEqualTo(Map.of(engineering25, 1L, engineering30, 1L));
        assertThat(employees.countByDepartmentAndAgeHavingMin(1)).isEqualTo(Map.of(engineering25, 2L, sales35, 2L));
        assertThat(employees.countByDepartmentAndAgeHavingMin(2)).isEmpty();

        seniorAlice.department = "Sales";
        employees.save(seniorAlice);
        assertThat(employees.countByDepartment()).isEqualTo(Map.of("Engineering", 2L, "Sales", 3L, "HR", 1L));
        employees.deleteById(eve.id);
        assertThat(employees.countByDepartment()).isEqualTo(Map.of("Engineering", 2L, "Sales", 3L));
    }

    private static <K> Map<K, Set<Long>> idsByGroup(Map<K, ? extends Collection<TestEntity>> groups) {
        return groups.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey,
                entry -> entry.getValue().stream().map(row -> row.id).collect(Collectors.toSet())));
    }
}
