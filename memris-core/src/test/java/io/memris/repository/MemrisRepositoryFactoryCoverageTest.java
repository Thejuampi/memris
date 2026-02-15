package io.memris.repository;

import io.memris.index.CompositeHashIndex;
import io.memris.index.CompositeKey;
import io.memris.index.CompositeRangeIndex;
import io.memris.index.HashIndex;
import io.memris.index.RangeIndex;
import io.memris.index.StringPrefixIndex;
import io.memris.index.StringSuffixIndex;
import io.memris.kernel.Predicate;
import io.memris.kernel.RowId;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MemrisRepositoryFactoryCoverageTest {

    private MemrisRepositoryFactory factory;

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void queryIndexShouldReturnNullWhenIndexOrTableMissing() {
        factory = new MemrisRepositoryFactory();

        assertThat(factory.queryIndex(DummyEntity.class, "name", Predicate.Operator.EQ, "x")).isNull();

        putIndex(DummyEntity.class, "name", new HashIndex<>());
        assertThat(factory.queryIndex(DummyEntity.class, "name", Predicate.Operator.EQ, "x")).isNull();
    }

    @Test
    void queryIndexShouldHandleHashRangePrefixSuffixAndCompositeIndexes() {
        factory = new MemrisRepositoryFactory();
        putTable(DummyEntity.class, alwaysLiveTable());

        HashIndex<Object> hash = new HashIndex<>();
        hash.add("alice", RowId.fromLong(1));
        putIndex(DummyEntity.class, "hash", hash);
        assertThat(factory.queryIndex(DummyEntity.class, "hash", Predicate.Operator.EQ, "alice"))
                .containsExactly(1);
        assertThat(factory.queryIndex(DummyEntity.class, "hash", Predicate.Operator.EQ, null)).isNull();

        RangeIndex<Integer> range = new RangeIndex<>();
        range.add(10, RowId.fromLong(2));
        range.add(20, RowId.fromLong(3));
        putIndex(DummyEntity.class, "range", range);
        assertThat(factory.queryIndex(DummyEntity.class, "range", Predicate.Operator.GT, 10))
                .containsExactly(3);
        assertThat(factory.queryIndex(DummyEntity.class, "range", Predicate.Operator.BETWEEN, new Object[] { 10, 20 }))
                .containsExactlyInAnyOrder(2, 3);
        assertThat(factory.queryIndex(DummyEntity.class, "range", Predicate.Operator.BETWEEN, new Object[] { 10 }))
                .isNull();
        assertThatThrownBy(() -> factory.queryIndex(
                DummyEntity.class,
                "range",
                Predicate.Operator.BETWEEN,
                new Object[] { "a", 20 }))
                        .isInstanceOf(ClassCastException.class);

        StringPrefixIndex prefix = new StringPrefixIndex();
        prefix.add("alice", RowId.fromLong(4));
        putIndex(DummyEntity.class, "prefix", prefix);
        assertThat(factory.queryIndex(DummyEntity.class, "prefix", Predicate.Operator.STARTING_WITH, "ali"))
                .containsExactly(4);
        assertThat(factory.queryIndex(DummyEntity.class, "prefix", Predicate.Operator.EQ, "ali"))
                .containsExactly(4);
        assertThat(factory.queryIndex(DummyEntity.class, "prefix", Predicate.Operator.ENDING_WITH, "ice"))
                .isNull();

        StringSuffixIndex suffix = new StringSuffixIndex();
        suffix.add("hello", RowId.fromLong(5));
        putIndex(DummyEntity.class, "suffix", suffix);
        assertThat(factory.queryIndex(DummyEntity.class, "suffix", Predicate.Operator.ENDING_WITH, "llo"))
                .containsExactly(5);
        assertThat(factory.queryIndex(DummyEntity.class, "suffix", Predicate.Operator.EQ, "hello")).isNull();

        CompositeHashIndex compositeHash = new CompositeHashIndex();
        CompositeKey keyA = CompositeKey.of(new Object[] { "d1", 11 });
        compositeHash.add(keyA, RowId.fromLong(6));
        putIndex(DummyEntity.class, "compHash", compositeHash);
        assertThat(factory.queryIndex(DummyEntity.class, "compHash", Predicate.Operator.EQ, keyA))
                .containsExactly(6);

        CompositeRangeIndex compositeRange = new CompositeRangeIndex();
        CompositeKey keyB = CompositeKey.of(new Object[] { "d1", 10 });
        CompositeKey keyC = CompositeKey.of(new Object[] { "d1", 20 });
        compositeRange.add(keyB, RowId.fromLong(7));
        compositeRange.add(keyC, RowId.fromLong(8));
        putIndex(DummyEntity.class, "compRange", compositeRange);
        assertThat(factory.queryIndex(DummyEntity.class, "compRange", Predicate.Operator.GTE, keyB))
                .containsExactlyInAnyOrder(7, 8);
        assertThat(factory.queryIndex(
                DummyEntity.class,
                "compRange",
                Predicate.Operator.BETWEEN,
                new Object[] { keyB, keyC })).containsExactlyInAnyOrder(7, 8);
    }

    @Test
    void addRemoveAndClearIndexEntryShouldHandleAllIndexFamilies() {
        factory = new MemrisRepositoryFactory();

        HashIndex<Object> hash = new HashIndex<>();
        RangeIndex<Integer> range = new RangeIndex<>();
        StringPrefixIndex prefix = new StringPrefixIndex();
        StringSuffixIndex suffix = new StringSuffixIndex();
        CompositeHashIndex compositeHash = new CompositeHashIndex();
        CompositeRangeIndex compositeRange = new CompositeRangeIndex();

        putIndex(DummyEntity.class, "hash", hash);
        putIndex(DummyEntity.class, "range", range);
        putIndex(DummyEntity.class, "prefix", prefix);
        putIndex(DummyEntity.class, "suffix", suffix);
        putIndex(DummyEntity.class, "compHash", compositeHash);
        putIndex(DummyEntity.class, "compRange", compositeRange);

        CompositeKey compositeKey = CompositeKey.of(new Object[] { "x", 1 });

        factory.addIndexEntry(DummyEntity.class, "hash", "v", 10);
        factory.addIndexEntry(DummyEntity.class, "range", 42, 11);
        factory.addIndexEntry(DummyEntity.class, "prefix", "alpha", 12);
        factory.addIndexEntry(DummyEntity.class, "suffix", "omega", 13);
        factory.addIndexEntry(DummyEntity.class, "compHash", compositeKey, 14);
        factory.addIndexEntry(DummyEntity.class, "compRange", compositeKey, 15);
        factory.addIndexEntry(DummyEntity.class, "hash", null, 99);

        assertThat(hash.lookup("v", null).toLongArray()).containsExactly(10);
        assertThat(range.lookup(42, null).toLongArray()).containsExactly(11);
        assertThat(prefix.startsWith("alp").toLongArray()).containsExactly(12);
        assertThat(suffix.endsWith("ega").toLongArray()).containsExactly(13);
        assertThat(compositeHash.lookup(compositeKey, null).toLongArray()).containsExactly(14);
        assertThat(compositeRange.lookup(compositeKey, null).toLongArray()).containsExactly(15);

        factory.removeIndexEntry(DummyEntity.class, "hash", "v", 10);
        factory.removeIndexEntry(DummyEntity.class, "range", 42, 11);
        factory.removeIndexEntry(DummyEntity.class, "prefix", "alpha", 12);
        factory.removeIndexEntry(DummyEntity.class, "suffix", "omega", 13);
        factory.removeIndexEntry(DummyEntity.class, "compHash", compositeKey, 14);
        factory.removeIndexEntry(DummyEntity.class, "compRange", compositeKey, 15);
        factory.removeIndexEntry(DummyEntity.class, "hash", null, 10);

        assertThat(hash.lookup("v", null).size()).isZero();
        assertThat(range.lookup(42, null).size()).isZero();
        assertThat(prefix.startsWith("alp").size()).isZero();
        assertThat(suffix.endsWith("ega").size()).isZero();
        assertThat(compositeHash.lookup(compositeKey, null).size()).isZero();
        assertThat(compositeRange.lookup(compositeKey, null).size()).isZero();

        hash.add("z", RowId.fromLong(1));
        range.add(1, RowId.fromLong(1));
        prefix.add("abc", RowId.fromLong(1));
        suffix.add("abc", RowId.fromLong(1));
        compositeHash.add(compositeKey, RowId.fromLong(1));
        compositeRange.add(compositeKey, RowId.fromLong(1));

        factory.clearIndexes(DummyEntity.class);

        assertThat(hash.lookup("z", null).size()).isZero();
        assertThat(range.lookup(1, null).size()).isZero();
        assertThat(prefix.size()).isZero();
        assertThat(suffix.size()).isZero();
        assertThat(compositeHash.lookup(compositeKey, null).size()).isZero();
        assertThat(compositeRange.lookup(compositeKey, null).size()).isZero();

        // Exercise default/no-op branch when unknown index object is configured.
        putIndex(DummyEntity.class, "unknown", new Object());
        factory.addIndexEntry(DummyEntity.class, "unknown", "v", 1);
        factory.removeIndexEntry(DummyEntity.class, "unknown", "v", 1);
    }

    @SuppressWarnings("unchecked")
    private void putIndex(Class<?> entityClass, String fieldName, Object index) {
        try {
            Field field = MemrisRepositoryFactory.class.getDeclaredField("indexes");
            field.setAccessible(true);
            Map<Class<?>, Map<String, Object>> indexes = (Map<Class<?>, Map<String, Object>>) field.get(factory);
            indexes.computeIfAbsent(entityClass, ignored -> new HashMap<>()).put(fieldName, index);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private void putTable(Class<?> entityClass, GeneratedTable table) {
        try {
            Field field = MemrisRepositoryFactory.class.getDeclaredField("tables");
            field.setAccessible(true);
            Map<Class<?>, GeneratedTable> tables = (Map<Class<?>, GeneratedTable>) field.get(factory);
            tables.put(entityClass, table);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private static GeneratedTable alwaysLiveTable() {
        return (GeneratedTable) Proxy.newProxyInstance(
                GeneratedTable.class.getClassLoader(),
                new Class<?>[] { GeneratedTable.class },
                (proxy, method, args) -> {
                    return switch (method.getName()) {
                        case "rowGeneration" -> 1L;
                        case "isLive" -> true;
                        case "scanAll", "scanEqualsInt", "scanEqualsLong", "scanEqualsString", "scanEqualsStringIgnoreCase",
                                "scanBetweenInt", "scanBetweenLong", "scanInInt", "scanInLong", "scanInString" -> new int[0];
                        case "lookupById", "lookupByIdString", "insertFrom", "currentGeneration", "allocatedCount", "liveCount" ->
                            0L;
                        case "columnCount", "readInt" -> 0;
                        case "typeCodeAt" -> (byte) 0;
                        case "readLong" -> 0L;
                        case "readString" -> null;
                        case "isPresent" -> true;
                        case "readWithSeqLock" -> null;
                        default -> null;
                    };
                });
    }

    private static final class DummyEntity {
    }
}
