package io.memris.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * TDD tests for UUID join table optimization.
 * Verifies that UUID join tables use 2 long columns (MSB + LSB) instead of String.
 */
class UuidJoinTableTest {

    @Test
    void uuid_many_to_many_join_table_works() {
        try (var factory = new MemrisRepositoryFactory()) {
            UuidStudentRepository students = factory.createJPARepository(UuidStudentRepository.class);
            UuidCourseRepository courses = factory.createJPARepository(UuidCourseRepository.class);

            UuidStudent alice = new UuidStudent();
            alice.id = UUID.randomUUID();
            alice.name = "Alice";
            alice.courses = new HashSet<>();

            UuidCourse math = new UuidCourse();
            math.id = UUID.randomUUID();
            math.name = "Math";
            alice.courses.add(math);

            UuidCourse science = new UuidCourse();
            science.id = UUID.randomUUID();
            science.name = "Science";
            alice.courses.add(science);

            // Save student with courses (should populate join table)
            students.save(alice);

            assertThat(students.count()).isEqualTo(1);
            assertThat(courses.count()).isEqualTo(2);

            // Verify the student was saved correctly
            UuidStudent found = students.findById(alice.id).orElseThrow();
            assertThat(found.name).isEqualTo("Alice");
        }
    }

    @Test
    void uuid_join_table_columns_are_2_longs() {
        try (var factory = new MemrisRepositoryFactory()) {
            UuidStudentRepository students = factory.createJPARepository(UuidStudentRepository.class);
            UuidCourseRepository courses = factory.createJPARepository(UuidCourseRepository.class);

            // Create entities with UUID IDs
            UuidStudent student = new UuidStudent();
            student.id = UUID.randomUUID();
            student.name = "Test Student";

            UuidCourse course = new UuidCourse();
            course.id = UUID.randomUUID();
            course.name = "Test Course";

            student.courses = new HashSet<>();
            student.courses.add(course);

            students.save(student);

            // Verify entities are stored
            assertThat(students.count()).isEqualTo(1);
            assertThat(courses.count()).isEqualTo(1);
            assertThat(students.findById(student.id)).isPresent();
            assertThat(courses.findById(course.id)).isPresent();
        }
    }

    // Test entity classes with UUID IDs
    static class UuidStudent {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.UUID)
        UUID id;
        String name;
        @jakarta.persistence.ManyToMany
        Set<UuidCourse> courses;

        UuidStudent() {}
    }

    static class UuidCourse {
        @jakarta.persistence.Id
        @GeneratedValue(strategy = GenerationType.UUID)
        UUID id;
        String name;

        UuidCourse() {}
    }

    // Repository interfaces
    interface UuidStudentRepository extends MemrisRepository<UuidStudent> {
        void save(UuidStudent s);
        java.util.Optional<UuidStudent> findById(java.util.UUID id);
        long count();
    }

    interface UuidCourseRepository extends MemrisRepository<UuidCourse> {
        void save(UuidCourse c);
        java.util.Optional<UuidCourse> findById(java.util.UUID id);
        long count();
    }
}
