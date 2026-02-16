package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.Id;
import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class EmbeddedEntityRuntimeTest {

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
    void shouldPersistAndLoadEmbeddedGraph() {
        EmbeddedUserRepository repo = arena.createRepository(EmbeddedUserRepository.class);
        EmbeddedUser saved = repo.save(new EmbeddedUser(
                null,
                "juan",
                new Profile("Juan", "juan@memris.io", new Address("Madrid", "28001"))));

        Optional<EmbeddedUser> loaded = repo.findById(saved.id);

        assertThat(loaded).isPresent();
        assertThat(loaded.orElseThrow().profile).isNotNull();
        assertThat(loaded.orElseThrow().profile.firstName).isEqualTo("Juan");
        assertThat(loaded.orElseThrow().profile.email).isEqualTo("juan@memris.io");
        assertThat(loaded.orElseThrow().profile.address).isNotNull();
        assertThat(loaded.orElseThrow().profile.address.city).isEqualTo("Madrid");
        assertThat(loaded.orElseThrow().profile.address.postalCode).isEqualTo("28001");
    }

    @Test
    void shouldQueryByEmbeddedAndDeepEmbeddedProperties() {
        EmbeddedUserRepository repo = arena.createRepository(EmbeddedUserRepository.class);
        repo.save(new EmbeddedUser(null, "juan", new Profile("Juan", "juan@memris.io", new Address("Madrid", "28001"))));
        repo.save(new EmbeddedUser(null, "ana", new Profile("Ana", "ana@memris.io", new Address("Bogota", "110111"))));
        repo.save(new EmbeddedUser(null, "maria", new Profile("Maria", "maria@memris.io", new Address("Madrid", "28002"))));

        List<EmbeddedUser> byFirstName = repo.findByProfileFirstName("Ana");
        List<EmbeddedUser> byCity = repo.findByProfileAddressCity("Madrid");
        List<EmbeddedUser> byEmailAndCity = repo.findByProfileEmailAndProfileAddressCity("juan@memris.io", "Madrid");

        assertThat(byFirstName).extracting(user -> user.username).containsExactly("ana");
        assertThat(byCity).extracting(user -> user.username).containsExactlyInAnyOrder("juan", "maria");
        assertThat(byEmailAndCity).extracting(user -> user.username).containsExactly("juan");
    }

    @Test
    void shouldHandleNullEmbeddedValuesInQueries() {
        EmbeddedUserRepository repo = arena.createRepository(EmbeddedUserRepository.class);
        repo.save(new EmbeddedUser(null, "empty", null));
        repo.save(new EmbeddedUser(null, "partial", new Profile("Partial", "partial@memris.io", null)));
        repo.save(new EmbeddedUser(null, "complete", new Profile("Complete", "complete@memris.io", new Address("Quito", "170101"))));

        List<EmbeddedUser> byCity = repo.findByProfileAddressCity("Quito");
        List<EmbeddedUser> byFirstName = repo.findByProfileFirstName("Partial");

        assertThat(byCity).extracting(user -> user.username).containsExactly("complete");
        assertThat(byFirstName).extracting(user -> user.username).containsExactly("partial");
    }

    @Test
    void shouldUpdateEmbeddedValuesOnResave() {
        EmbeddedUserRepository repo = arena.createRepository(EmbeddedUserRepository.class);
        EmbeddedUser saved = repo.save(new EmbeddedUser(
                null,
                "juan",
                new Profile("Juan", "juan@memris.io", new Address("Madrid", "28001"))));

        saved.profile.address.city = "Bogota";
        saved.profile.email = "juan@bogota.io";
        repo.save(saved);

        List<EmbeddedUser> updated = repo.findByProfileEmailAndProfileAddressCity("juan@bogota.io", "Bogota");
        List<EmbeddedUser> old = repo.findByProfileAddressCity("Madrid");

        assertThat(updated).extracting(user -> user.username).containsExactly("juan");
        assertThat(old).isEmpty();
    }

    public interface EmbeddedUserRepository extends MemrisRepository<EmbeddedUser> {
        EmbeddedUser save(EmbeddedUser entity);

        Optional<EmbeddedUser> findById(Long id);

        List<EmbeddedUser> findByProfileFirstName(String firstName);

        List<EmbeddedUser> findByProfileAddressCity(String city);

        List<EmbeddedUser> findByProfileEmailAndProfileAddressCity(String email, String city);
    }

    @Entity
    public static class EmbeddedUser {
        @Id
        @GeneratedValue
        public Long id;
        public String username;
        public Profile profile;

        public EmbeddedUser() {
        }

        public EmbeddedUser(Long id, String username, Profile profile) {
            this.id = id;
            this.username = username;
            this.profile = profile;
        }
    }

    public static class Profile {
        public String firstName;
        public String email;
        public Address address;

        public Profile() {
        }

        public Profile(String firstName, String email, Address address) {
            this.firstName = firstName;
            this.email = email;
            this.address = address;
        }
    }

    public static class Address {
        public String city;
        public String postalCode;

        public Address() {
        }

        public Address(String city, String postalCode) {
            this.city = city;
            this.postalCode = postalCode;
        }
    }
}
