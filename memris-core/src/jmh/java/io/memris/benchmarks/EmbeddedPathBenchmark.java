package io.memris.benchmarks;

import io.memris.core.Entity;
import io.memris.core.Id;
import io.memris.core.MemrisArena;
import io.memris.repository.MemrisRepository;
import io.memris.repository.MemrisRepositoryFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 2, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
public class EmbeddedPathBenchmark {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private MemrisRepositoryFactory factory;
        private MemrisArena arena;
        private FlatUserRepository flatRepository;
        private EmbeddedUserRepository embeddedRepository;
        private long flatId;
        private long embeddedId;
        private long saveRingSize;
        private long sequence;

        @Setup(Level.Trial)
        public void setup() {
            factory = new MemrisRepositoryFactory();
            arena = factory.createArena();
            flatRepository = arena.createRepository(FlatUserRepository.class);
            embeddedRepository = arena.createRepository(EmbeddedUserRepository.class);
            saveRingSize = 8_192L;

            flatId = flatRepository.save(new FlatUser(null, "flat-seed", "seed@memris.io")).id;
            embeddedId = embeddedRepository.save(
                    new EmbeddedUser(null, "embedded-seed", new Profile("seed@memris.io", new Address("Madrid"))))
                    .id;
            for (long id = 1L; id <= saveRingSize; id++) {
                flatRepository.save(new FlatUser(id, "flat-init-" + id, "flat-init-" + id + "@memris.io"));
                embeddedRepository.save(new EmbeddedUser(
                        id,
                        "embedded-init-" + id,
                        new Profile("embedded-init-" + id + "@memris.io", new Address("Init-" + (id % 100)))));
            }
            sequence = 1L;
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (factory != null) {
                factory.close();
            }
        }
    }

    @Benchmark
    public FlatUser flat_save(BenchmarkState state) {
        long seq = state.sequence++;
        long id = (seq % state.saveRingSize) + 1L;
        return state.flatRepository.save(new FlatUser(id, "flat-" + seq, "flat-" + seq + "@memris.io"));
    }

    @Benchmark
    public EmbeddedUser embedded_save(BenchmarkState state) {
        long seq = state.sequence++;
        long id = (seq % state.saveRingSize) + 1L;
        return state.embeddedRepository.save(new EmbeddedUser(
                id,
                "embedded-" + seq,
                new Profile("embedded-" + seq + "@memris.io", new Address("City-" + (seq % 100)))));
    }

    @Benchmark
    public Optional<FlatUser> flat_findById(BenchmarkState state) {
        return state.flatRepository.findById(state.flatId);
    }

    @Benchmark
    public Optional<EmbeddedUser> embedded_findById(BenchmarkState state) {
        return state.embeddedRepository.findById(state.embeddedId);
    }

    @Benchmark
    public FlatUser flat_update(BenchmarkState state) {
        long seq = state.sequence++;
        return state.flatRepository.save(new FlatUser(state.flatId, "flat-update-" + seq, "u" + seq + "@memris.io"));
    }

    @Benchmark
    public EmbeddedUser embedded_update(BenchmarkState state) {
        long seq = state.sequence++;
        return state.embeddedRepository.save(new EmbeddedUser(
                state.embeddedId,
                "embedded-update-" + seq,
                new Profile("u" + seq + "@memris.io", new Address("Update-" + (seq % 100)))));
    }

    public interface FlatUserRepository extends MemrisRepository<FlatUser> {
        FlatUser save(FlatUser entity);

        Optional<FlatUser> findById(Long id);
    }

    public interface EmbeddedUserRepository extends MemrisRepository<EmbeddedUser> {
        EmbeddedUser save(EmbeddedUser entity);

        Optional<EmbeddedUser> findById(Long id);
    }

    @Entity
    public static class FlatUser {
        @Id
        public Long id;
        public String username;
        public String email;

        public FlatUser() {
        }

        public FlatUser(Long id, String username, String email) {
            this.id = id;
            this.username = username;
            this.email = email;
        }
    }

    @Entity
    public static class EmbeddedUser {
        @Id
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
        public String email;
        public Address address;

        public Profile() {
        }

        public Profile(String email, Address address) {
            this.email = email;
            this.address = address;
        }
    }

    public static class Address {
        public String city;

        public Address() {
        }

        public Address(String city) {
            this.city = city;
        }
    }
}
