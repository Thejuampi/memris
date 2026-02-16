package io.memris.runtime;

import io.memris.repository.MemrisArena;
import io.memris.repository.MemrisRepositoryFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Comprehensive tests for HeapRuntimeKernel BETWEEN operations.
 * Tests all primitive types: Float, Double, Byte, Short, Char.
 */
class HeapRuntimeKernelTest {

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

    // ==================== FLOAT BETWEEN TESTS ====================

    @Test
    void floatBetween_basicRange() {
        FloatTestRepository repo = arena.createRepository(FloatTestRepository.class);

        repo.save(new FloatTestEntity(1L, 1.5f));
        repo.save(new FloatTestEntity(2L, 2.5f));
        repo.save(new FloatTestEntity(3L, 3.5f));
        repo.save(new FloatTestEntity(4L, 5.5f));
        repo.save(new FloatTestEntity(5L, 10.5f));

        List<FloatTestEntity> results = repo.findByValueBetween(2.0f, 6.0f);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new FloatTestEntity(null, 2.5f),
                new FloatTestEntity(null, 3.5f),
                new FloatTestEntity(null, 5.5f)
        );
    }

    @Test
    void floatBetween_negativeValues() {
        // Now works correctly with sortable encoding
        FloatTestRepository repo = arena.createRepository(FloatTestRepository.class);

        repo.save(new FloatTestEntity(1L, -10.5f));
        repo.save(new FloatTestEntity(2L, -5.5f));
        repo.save(new FloatTestEntity(3L, -2.5f));
        repo.save(new FloatTestEntity(4L, 0.0f));
        repo.save(new FloatTestEntity(5L, 5.5f));

        List<FloatTestEntity> results = repo.findByValueBetween(-6.0f, -2.0f);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new FloatTestEntity(null, -5.5f),
                new FloatTestEntity(null, -2.5f)
        );
    }

    @Test
    void floatBetween_boundaryInclusive() {
        FloatTestRepository repo = arena.createRepository(FloatTestRepository.class);

        repo.save(new FloatTestEntity(1L, 1.0f));
        repo.save(new FloatTestEntity(2L, 5.0f));
        repo.save(new FloatTestEntity(3L, 10.0f));

        List<FloatTestEntity> results = repo.findByValueBetween(1.0f, 10.0f);

        assertThat(results).hasSize(3);
    }

    @Test
    void floatBetween_noMatches() {
        FloatTestRepository repo = arena.createRepository(FloatTestRepository.class);

        repo.save(new FloatTestEntity(1L, 1.0f));
        repo.save(new FloatTestEntity(2L, 100.0f));

        List<FloatTestEntity> results = repo.findByValueBetween(50.0f, 75.0f);

        assertThat(results).isEmpty();
    }

    @Test
    void floatBetween_crossZeroRange() {
        FloatTestRepository repo = arena.createRepository(FloatTestRepository.class);

        repo.save(new FloatTestEntity(1L, -5.5f));
        repo.save(new FloatTestEntity(2L, -1.0f));
        repo.save(new FloatTestEntity(3L, 0.0f));
        repo.save(new FloatTestEntity(4L, 2.5f));
        repo.save(new FloatTestEntity(5L, 7.5f));

        List<FloatTestEntity> results = repo.findByValueBetween(-2.0f, 3.0f);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new FloatTestEntity(null, -1.0f),
                new FloatTestEntity(null, 0.0f),
                new FloatTestEntity(null, 2.5f)
        );
    }

    // ==================== DOUBLE BETWEEN TESTS ====================

    @Test
    void doubleBetween_basicRange() {
        DoubleTestRepository repo = arena.createRepository(DoubleTestRepository.class);

        repo.save(new DoubleTestEntity(1L, 1.5));
        repo.save(new DoubleTestEntity(2L, 2.5));
        repo.save(new DoubleTestEntity(3L, 3.5));
        repo.save(new DoubleTestEntity(4L, 5.5));
        repo.save(new DoubleTestEntity(5L, 10.5));

        List<DoubleTestEntity> results = repo.findByValueBetween(2.0, 6.0);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new DoubleTestEntity(null, 2.5),
                new DoubleTestEntity(null, 3.5),
                new DoubleTestEntity(null, 5.5)
        );
    }

    @Test
    void doubleBetween_largeValues() {
        DoubleTestRepository repo = arena.createRepository(DoubleTestRepository.class);

        repo.save(new DoubleTestEntity(1L, 1e100));
        repo.save(new DoubleTestEntity(2L, 1e150));
        repo.save(new DoubleTestEntity(3L, 1e200));

        List<DoubleTestEntity> results = repo.findByValueBetween(1e140, 1e160);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactly(new DoubleTestEntity(null, 1e150));
    }

    @Test
    void doubleBetween_negativeValues() {
        // Now works correctly with sortable encoding
        DoubleTestRepository repo = arena.createRepository(DoubleTestRepository.class);

        repo.save(new DoubleTestEntity(1L, -100.5));
        repo.save(new DoubleTestEntity(2L, -50.5));
        repo.save(new DoubleTestEntity(3L, -10.5));
        repo.save(new DoubleTestEntity(4L, 0.0));

        List<DoubleTestEntity> results = repo.findByValueBetween(-60.0, -40.0);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactly(new DoubleTestEntity(null, -50.5));
    }

    @Test
    void doubleBetween_crossZeroRange() {
        DoubleTestRepository repo = arena.createRepository(DoubleTestRepository.class);

        repo.save(new DoubleTestEntity(1L, -5.5));
        repo.save(new DoubleTestEntity(2L, -1.0));
        repo.save(new DoubleTestEntity(3L, 0.0));
        repo.save(new DoubleTestEntity(4L, 2.5));
        repo.save(new DoubleTestEntity(5L, 7.5));

        List<DoubleTestEntity> results = repo.findByValueBetween(-2.0, 3.0);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new DoubleTestEntity(null, -1.0),
                new DoubleTestEntity(null, 0.0),
                new DoubleTestEntity(null, 2.5)
        );
    }

    // ==================== BYTE BETWEEN TESTS ====================

    @Test
    void byteBetween_basicRange() {
        ByteTestRepository repo = arena.createRepository(ByteTestRepository.class);

        repo.save(new ByteTestEntity(1L, (byte) 10));
        repo.save(new ByteTestEntity(2L, (byte) 20));
        repo.save(new ByteTestEntity(3L, (byte) 30));
        repo.save(new ByteTestEntity(4L, (byte) 40));
        repo.save(new ByteTestEntity(5L, (byte) 50));

        List<ByteTestEntity> results = repo.findByValueBetween((byte) 15, (byte) 35);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new ByteTestEntity(null, (byte) 20),
                new ByteTestEntity(null, (byte) 30)
        );
    }

    @Test
    void byteBetween_fullRange() {
        ByteTestRepository repo = arena.createRepository(ByteTestRepository.class);

        repo.save(new ByteTestEntity(1L, Byte.MIN_VALUE));
        repo.save(new ByteTestEntity(2L, (byte) 0));
        repo.save(new ByteTestEntity(3L, Byte.MAX_VALUE));

        List<ByteTestEntity> results = repo.findByValueBetween(Byte.MIN_VALUE, Byte.MAX_VALUE);

        assertThat(results).hasSize(3);
    }

    @Test
    void byteBetween_negativeRange() {
        ByteTestRepository repo = arena.createRepository(ByteTestRepository.class);

        repo.save(new ByteTestEntity(1L, (byte) -100));
        repo.save(new ByteTestEntity(2L, (byte) -50));
        repo.save(new ByteTestEntity(3L, (byte) -10));
        repo.save(new ByteTestEntity(4L, (byte) 10));

        List<ByteTestEntity> results = repo.findByValueBetween((byte) -60, (byte) -40);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactly(new ByteTestEntity(null, (byte) -50));
    }

    // ==================== SHORT BETWEEN TESTS ====================

    @Test
    void shortBetween_basicRange() {
        ShortTestRepository repo = arena.createRepository(ShortTestRepository.class);

        repo.save(new ShortTestEntity(1L, (short) 100));
        repo.save(new ShortTestEntity(2L, (short) 200));
        repo.save(new ShortTestEntity(3L, (short) 300));
        repo.save(new ShortTestEntity(4L, (short) 400));
        repo.save(new ShortTestEntity(5L, (short) 500));

        List<ShortTestEntity> results = repo.findByValueBetween((short) 150, (short) 350);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new ShortTestEntity(null, (short) 200),
                new ShortTestEntity(null, (short) 300)
        );
    }

    @Test
    void shortBetween_negativeRange() {
        ShortTestRepository repo = arena.createRepository(ShortTestRepository.class);

        repo.save(new ShortTestEntity(1L, (short) -1000));
        repo.save(new ShortTestEntity(2L, (short) -500));
        repo.save(new ShortTestEntity(3L, (short) -100));
        repo.save(new ShortTestEntity(4L, (short) 100));

        List<ShortTestEntity> results = repo.findByValueBetween((short) -600, (short) -400);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id")
                .containsExactly(new ShortTestEntity(null, (short) -500));
    }

    @Test
    void shortBetween_largeValues() {
        ShortTestRepository repo = arena.createRepository(ShortTestRepository.class);

        repo.save(new ShortTestEntity(1L, (short) 30000));
        repo.save(new ShortTestEntity(2L, Short.MAX_VALUE));
        repo.save(new ShortTestEntity(3L, (short) 10000));

        List<ShortTestEntity> results = repo.findByValueBetween((short) 25000, Short.MAX_VALUE);

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new ShortTestEntity(null, (short) 30000),
                new ShortTestEntity(null, Short.MAX_VALUE)
        );
    }

    // ==================== CHAR BETWEEN TESTS ====================

    @Test
    void charBetween_alphabeticRange() {
        CharTestRepository repo = arena.createRepository(CharTestRepository.class);

        repo.save(new CharTestEntity(1L, 'a'));
        repo.save(new CharTestEntity(2L, 'd'));
        repo.save(new CharTestEntity(3L, 'g'));
        repo.save(new CharTestEntity(4L, 'z'));

        List<CharTestEntity> results = repo.findByValueBetween('c', 'h');

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new CharTestEntity(null, 'd'),
                new CharTestEntity(null, 'g')
        );
    }

    @Test
    void charBetween_numericRange() {
        CharTestRepository repo = arena.createRepository(CharTestRepository.class);

        repo.save(new CharTestEntity(1L, '0'));
        repo.save(new CharTestEntity(2L, '3'));
        repo.save(new CharTestEntity(3L, '5'));
        repo.save(new CharTestEntity(4L, '9'));

        List<CharTestEntity> results = repo.findByValueBetween('2', '6');

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new CharTestEntity(null, '3'),
                new CharTestEntity(null, '5')
        );
    }

    @Test
    void charBetween_unicodeRange() {
        CharTestRepository repo = arena.createRepository(CharTestRepository.class);

        repo.save(new CharTestEntity(1L, '\u0041')); // 'A'
        repo.save(new CharTestEntity(2L, '\u0061')); // 'a'
        repo.save(new CharTestEntity(3L, '\u03B1')); // Greek alpha
        repo.save(new CharTestEntity(4L, '\u4E00')); // CJK character

        List<CharTestEntity> results = repo.findByValueBetween('\u0060', '\u03C0');

        assertThat(results).usingRecursiveFieldByFieldElementComparatorIgnoringFields("id").containsExactlyInAnyOrder(
                new CharTestEntity(null, '\u0061'),
                new CharTestEntity(null, '\u03B1')
        );
    }

}
