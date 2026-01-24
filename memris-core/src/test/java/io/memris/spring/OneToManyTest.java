package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class OneToManyTest {

    @Test
    void onetomany_cascade_save_children() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            DepartmentRepository departments = factory.createJPARepository(DepartmentRepository.class);
            EmployeeRepository employees = factory.createJPARepository(EmployeeRepository.class);

            Department dept = new Department("Engineering");
            dept.employees.add(new Employee("Alice"));
            dept.employees.add(new Employee("Bob"));
            dept.employees.add(new Employee("Carol"));

            departments.save(dept);

            assertThat(employees.count()).isEqualTo(3);
            assertThat(employees.findAll()).hasSize(3);

            List<Employee> engEmployees = employees.findByDepartmentName("Engineering");
            assertThat(engEmployees).hasSize(3);

            factory.close();
        }
    }

    @Test
    void onetomany_find_children_by_parent() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            DepartmentRepository departments = factory.createJPARepository(DepartmentRepository.class);
            EmployeeRepository employees = factory.createJPARepository(EmployeeRepository.class);

            Department eng = new Department("Engineering");
            eng.employees.add(new Employee("Alice"));
            eng.employees.add(new Employee("Bob"));

            Department sales = new Department("Sales");
            sales.employees.add(new Employee("Carol"));

            departments.save(eng);
            departments.save(sales);

            assertThat(employees.count()).isEqualTo(3);

            List<Employee> engEmployees = employees.findByDepartmentName("Engineering");
            assertThat(engEmployees).extracting(e -> e.name)
                    .containsExactlyInAnyOrder("Alice", "Bob");

            List<Employee> salesEmployees = employees.findByDepartmentName("Sales");
            assertThat(salesEmployees).extracting(e -> e.name)
                    .containsExactlyInAnyOrder("Carol");

            factory.close();
        }
    }

    @Test
    void onetomany_empty_list_handled() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            DepartmentRepository departments = factory.createJPARepository(DepartmentRepository.class);
            EmployeeRepository employees = factory.createJPARepository(EmployeeRepository.class);

            Department emptyDept = new Department("Empty");
            departments.save(emptyDept);

            assertThat(employees.count()).isEqualTo(0);

            factory.close();
        }
    }

    @Test
    void onetomany_multiple_departments() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            DepartmentRepository departments = factory.createJPARepository(DepartmentRepository.class);
            EmployeeRepository employees = factory.createJPARepository(EmployeeRepository.class);

            Department dept1 = new Department("Dept1");
            dept1.employees.add(new Employee("A"));
            dept1.employees.add(new Employee("B"));

            Department dept2 = new Department("Dept2");
            dept2.employees.add(new Employee("C"));

            departments.save(dept1);
            departments.save(dept2);

            assertThat(employees.count()).isEqualTo(3);
            assertThat(departments.count()).isEqualTo(2);

            factory.close();
        }
    }

    @Entity
    static final class Department {
        @Id
        int id;
        String name;
        @OneToMany
        List<Employee> employees = new java.util.ArrayList<>();

        Department() {}

        Department(String name) {
            this.name = name;
        }
    }

    interface DepartmentRepository extends MemrisRepository<Department> {
        void save(Department d);
        long count();
    }

    @Entity
    static final class Employee {
        @Id
        int id;
        String name;
        String departmentName;

        Employee() {}

        Employee(String name) {
            this.name = name;
        }
    }

    interface EmployeeRepository extends MemrisRepository<Employee> {
        long count();
        java.util.List<Employee> findAll();
        java.util.List<Employee> findByDepartmentName(String departmentName);
    }
}
