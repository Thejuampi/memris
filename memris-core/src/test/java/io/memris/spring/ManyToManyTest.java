package io.memris.spring;

import io.memris.spring.MemrisRepository;
import io.memris.spring.MemrisRepositoryFactory;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.ManyToMany;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ManyToManyTest {

    @Test
    void manytomany_set_add_remove() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            StudentRepository students = factory.createJPARepository(StudentRepository.class);
            CourseRepository courses = factory.createJPARepository(CourseRepository.class);

            Student alice = new Student("Alice");
            alice.courses.add(new Course("Math"));
            alice.courses.add(new Course("Science"));

            Student bob = new Student("Bob");
            bob.courses.add(new Course("Science"));
            bob.courses.add(new Course("Art"));

            students.save(alice);
            students.save(bob);

            assertThat(students.count()).isEqualTo(2);
            assertThat(courses.count()).isEqualTo(4);

            factory.close();
        }
    }

    @Test
    void manytomany_join_table_populated() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            MemberRepository members = factory.createJPARepository(MemberRepository.class);
            GroupRepository groups = factory.createJPARepository(GroupRepository.class);

            Member alice = new Member("Alice");
            alice.groups.add(new Group("Admins"));
            alice.groups.add(new Group("Users"));

            Member bob = new Member("Bob");
            bob.groups.add(new Group("Users"));

            members.save(alice);
            members.save(bob);

            assertThat(members.count()).isEqualTo(2);
            assertThat(groups.count()).isEqualTo(3);

            factory.close();
        }
    }

    @Test
    void manytomany_bidirectional_sync() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            AuthorRepository authors = factory.createJPARepository(AuthorRepository.class);
            BookRepository books = factory.createJPARepository(BookRepository.class);

            Author author = new Author("Author1");
            author.books.add(new Book("Book1"));
            author.books.add(new Book("Book2"));

            authors.save(author);

            assertThat(authors.count()).isEqualTo(1);
            assertThat(books.count()).isEqualTo(2);

            factory.close();
        }
    }

    @Test
    void manytomany_empty_collections() {
        try (Arena arena = Arena.ofConfined()) {
            MemrisRepositoryFactory factory = new MemrisRepositoryFactory();
            PersonRepository persons = factory.createJPARepository(PersonRepository.class);
            EventRepository events = factory.createJPARepository(EventRepository.class);

            Person lonely = new Person("Lonely");
            Event unused = new Event("Unused");

            persons.save(lonely);
            events.save(unused);

            assertThat(persons.count()).isEqualTo(1);
            assertThat(events.count()).isEqualTo(1);

            factory.close();
        }
    }

    @Entity
    static final class Student {
        @Id
        int id;
        String name;
        @ManyToMany
        Set<Course> courses = new HashSet<>();

        Student() {}

        Student(String name) {
            this.name = name;
        }
    }

    interface StudentRepository extends MemrisRepository<Student> {
        void save(Student s);
        long count();
    }

    @Entity
    static final class Course {
        @Id
        int id;
        String name;

        Course() {}

        Course(String name) {
            this.name = name;
        }
    }

    interface CourseRepository extends MemrisRepository<Course> {
        void save(Course c);
        long count();
    }

    @Entity
    static final class Member {
        @Id
        int id;
        String name;
        @ManyToMany
        Set<Group> groups = new HashSet<>();

        Member() {}

        Member(String name) {
            this.name = name;
        }
    }

    interface MemberRepository extends MemrisRepository<Member> {
        void save(Member m);
        long count();
    }

    @Entity
    static final class Group {
        @Id
        int id;
        String name;

        Group() {}

        Group(String name) {
            this.name = name;
        }
    }

    interface GroupRepository extends MemrisRepository<Group> {
        void save(Group g);
        long count();
    }

    @Entity
    static final class Author {
        @Id
        int id;
        String name;
        @ManyToMany
        Set<Book> books = new HashSet<>();

        Author() {}

        Author(String name) {
            this.name = name;
        }
    }

    interface AuthorRepository extends MemrisRepository<Author> {
        void save(Author a);
        long count();
    }

    @Entity
    static final class Book {
        @Id
        int id;
        String name;

        Book() {}

        Book(String name) {
            this.name = name;
        }
    }

    interface BookRepository extends MemrisRepository<Book> {
        void save(Book b);
        long count();
    }

    @Entity
    static final class Person {
        @Id
        int id;
        String name;
        @ManyToMany
        Set<Event> events = new HashSet<>();

        Person() {}

        Person(String name) {
            this.name = name;
        }
    }

    interface PersonRepository extends MemrisRepository<Person> {
        void save(Person p);
        long count();
    }

    @Entity
    static final class Event {
        @Id
        int id;
        String name;

        Event() {}

        Event(String name) {
            this.name = name;
        }
    }

    interface EventRepository extends MemrisRepository<Event> {
        void save(Event e);
        long count();
    }
}
