package io.memris.runtime;

import io.memris.core.Entity;
import io.memris.core.GeneratedValue;
import io.memris.core.Id;
import io.memris.core.Index;
import io.memris.core.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.core.MetadataExtractor;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.index.HashIndex;
import io.memris.query.CompiledQuery;
import io.memris.query.QueryCompiler;
import io.memris.query.QueryPlanner;
import io.memris.repository.EntitySaverGenerator;
import io.memris.repository.MemrisRepositoryFactory;
import io.memris.storage.GeneratedTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class RepositoryRuntimeCompiledConditionProgramTest {

    private MemrisRepositoryFactory factory;
    private MemrisArena arena;

    @BeforeEach
    void setUp() {
        factory = new MemrisRepositoryFactory(MemrisConfiguration.builder().build());
        arena = factory.createArena();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (factory != null) {
            factory.close();
        }
    }

    @Test
    void shouldUseIndexedLookupThenRowFilterForIndexedAndNonIndexedConditions() throws Exception {
        var harness = createHarness("findByNameAndDateOfBirth", String.class, LocalDate.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("juan", LocalDate.of(1991, 1, 1), "Bogota"));
        runtime.saveOne(new Person("maria", LocalDate.of(1990, 1, 1), "Madrid"));

        harness.table().resetCounters();

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { "juan", LocalDate.of(1990, 1, 1) });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isTrue();
        assertThat(result).extracting(person -> person.city).containsExactly("Madrid");
        assertThat(harness.table().scanEqualsLongCalls()).isZero();
        assertThat(harness.table().readLongCalls()).isGreaterThan(0);
    }

    @Test
    void shouldFallbackToDefaultExecutionWhenNoIndexedDriverConditionExists() throws Exception {
        var harness = createHarness("findByDateOfBirth", LocalDate.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("maria", LocalDate.of(1990, 1, 1), "Bogota"));

        harness.table().resetCounters();

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { LocalDate.of(1990, 1, 1) });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isFalse();
        assertThat(result).hasSize(2);
        assertThat(harness.table().scanEqualsLongCalls()).isGreaterThan(0);
    }

    @Test
    void shouldFallbackToDefaultExecutionWhenResidualConditionIsUnsupported() throws Exception {
        var harness = createHarness("findByNameAndCityContaining", String.class, String.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Bogota"));

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { "juan", "rid" });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isFalse();
        assertThat(result).extracting(person -> person.city).containsExactly("Madrid");
    }

    @Test
    void shouldCompileSingleIndexedConditionQuery() throws Exception {
        var harness = createHarness("findByName", String.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("maria", LocalDate.of(1990, 1, 1), "Bogota"));

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { "juan" });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isTrue();
        assertThat(result).extracting(person -> person.name).containsExactly("juan");
    }

    @Test
    void shouldCompileOrGroupsWhenEachGroupHasIndexedDriver() throws Exception {
        var harness = createHarness("findByNameOrCity", String.class, String.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("maria", LocalDate.of(1990, 1, 1), "Bogota"));
        runtime.saveOne(new Person("ana", LocalDate.of(1990, 1, 1), "Quito"));

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { "juan", "Bogota" });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isTrue();
        assertThat(result).extracting(person -> person.name).containsExactlyInAnyOrder("juan", "maria");
    }

    @Test
    void shouldCompileResidualNotEqualsCondition() throws Exception {
        var harness = createHarness("findByNameAndDateOfBirthNot", String.class, LocalDate.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("juan", LocalDate.of(1991, 1, 1), "Bogota"));
        runtime.saveOne(new Person("maria", LocalDate.of(1991, 1, 1), "Quito"));

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { "juan", LocalDate.of(1990, 1, 1) });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isTrue();
        assertThat(result).extracting(person -> person.city).containsExactly("Bogota");
    }

    @Test
    void shouldCompileResidualInCondition() throws Exception {
        var harness = createHarness("findByNameAndDateOfBirthIn", String.class, Iterable.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("juan", LocalDate.of(1991, 1, 1), "Bogota"));
        runtime.saveOne(new Person("juan", LocalDate.of(1992, 1, 1), "Quito"));

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(),
                new Object[] { "juan", Set.of(LocalDate.of(1990, 1, 1), LocalDate.of(1992, 1, 1)) });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isTrue();
        assertThat(result).extracting(person -> person.city).containsExactlyInAnyOrder("Madrid", "Quito");
    }

    @Test
    void shouldCompileInDriverConditionWithoutRuntimeFallbackDispatch() throws Exception {
        var harness = createHarness("findByNameIn", Iterable.class);
        var runtime = harness.runtime();

        runtime.saveOne(new Person("juan", LocalDate.of(1990, 1, 1), "Madrid"));
        runtime.saveOne(new Person("maria", LocalDate.of(1991, 1, 1), "Bogota"));
        runtime.saveOne(new Person("ana", LocalDate.of(1992, 1, 1), "Quito"));

        harness.table().resetCounters();

        @SuppressWarnings("unchecked")
        List<Person> result = (List<Person>) runtime.find(harness.query(), new Object[] { List.of("juan", "ana") });

        assertThat(runtime.hasCompiledConditionProgram(harness.query())).isTrue();
        assertThat(result).extracting(person -> person.name).containsExactlyInAnyOrder("juan", "ana");
        assertThat(harness.table().scanEqualsLongCalls()).isZero();
    }

    private Harness createHarness(String methodName, Class<?>... parameterTypes) throws Exception {
        var metadata = MetadataExtractor.extractEntityMetadata(Person.class);
        var baseTable = arena.getOrCreateTable(Person.class);
        var table = new CountingGeneratedTable(baseTable);
        seedIndexes(metadata);

        Method method = PersonRepository.class.getMethod(methodName, parameterTypes);
        var logical = QueryPlanner.parse(method, Person.class);
        var compiled = new QueryCompiler(metadata).compile(logical);
        var queries = new CompiledQuery[] { compiled };
        var bindings = RepositoryMethodBinding.fromQueries(queries);
        var fields = metadata.fields().stream()
                .filter(field -> field.columnPosition() >= 0)
                .sorted(Comparator.comparingInt(io.memris.core.EntityMetadata.FieldMapping::columnPosition))
                .toList();
        var columnNames = fields.stream().map(io.memris.core.EntityMetadata.FieldMapping::name).toArray(String[]::new);
        var typeCodes = new byte[fields.size()];
        var primitiveNonNull = new boolean[fields.size()];
        var converters = new TypeConverter<?, ?>[fields.size()];
        var setters = new MethodHandle[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            var field = fields.get(i);
            typeCodes[i] = field.typeCode();
            primitiveNonNull[i] = field.javaType().isPrimitive();
            converters[i] = metadata.converters().get(field.name());
            setters[i] = metadata.fieldSetters().get(field.name());
        }

        var conditionExecutors = RepositoryRuntime.buildConditionExecutors(
                queries,
                columnNames,
                primitiveNonNull,
                Person.class,
                true);
        var orderExecutors = RepositoryRuntime.buildOrderExecutors(queries, table, primitiveNonNull);
        var projectionExecutors = RepositoryRuntime.buildProjectionExecutors(queries, MetadataExtractor::extractEntityMetadata);
        var entitySaver = new EntitySaverGenerator().generate(Person.class, metadata);

        MethodHandle entityConstructor;
        try {
            entityConstructor = MethodHandles.lookup().unreflectConstructor(metadata.entityConstructor());
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }

        int idColumnIndex = metadata.resolveColumnPosition(metadata.idColumnName());
        byte idTypeCode = idColumnIndex >= 0 && idColumnIndex < typeCodes.length
                ? typeCodes[idColumnIndex]
                : TypeCodes.TYPE_LONG;

        var plan = RepositoryPlan.fromGeneratedTable(
                table,
                Person.class,
                metadata.idColumnName(),
                queries,
                bindings,
                new RepositoryMethodExecutor[] { (runtime, args) -> null },
                conditionExecutors,
                orderExecutors,
                projectionExecutors,
                entityConstructor,
                columnNames,
                typeCodes,
                converters,
                setters,
                Map.of(Person.class, table),
                Map.of(Person.class, new HeapRuntimeKernel(table)),
                Map.of(),
                Map.of(),
                entitySaver,
                IdLookup.forTypeCode(idTypeCode));

        var runtime = new RepositoryRuntime<>(plan, factory, arena, metadata);
        return new Harness(runtime, compiled, table);
    }

    private void seedIndexes(io.memris.core.EntityMetadata<Person> metadata) {
        var indexes = arena.getOrCreateIndexes(Person.class);
        indexes.put(metadata.idColumnName(), new HashIndex<>());
        indexes.put("name", new HashIndex<>());
        indexes.put("city", new HashIndex<>());
    }

    private record Harness(RepositoryRuntime<Person> runtime, CompiledQuery query, CountingGeneratedTable table) {
    }

    @Entity
    public static class Person {
        @Id
        @GeneratedValue
        public Long id;
        @Index(type = Index.IndexType.HASH)
        public String name;
        public LocalDate dateOfBirth;
        @Index(type = Index.IndexType.HASH)
        public String city;

        public Person() {
        }

        public Person(String name, LocalDate dateOfBirth, String city) {
            this.name = name;
            this.dateOfBirth = dateOfBirth;
            this.city = city;
        }
    }

    interface PersonRepository {
        List<Person> findByNameAndDateOfBirth(String name, LocalDate dateOfBirth);

        List<Person> findByDateOfBirth(LocalDate dateOfBirth);

        List<Person> findByNameAndCityContaining(String name, String cityPart);

        List<Person> findByName(String name);

        List<Person> findByNameOrCity(String name, String city);

        List<Person> findByNameAndDateOfBirthNot(String name, LocalDate dateOfBirth);

        List<Person> findByNameAndDateOfBirthIn(String name, Iterable<LocalDate> dateOfBirth);

        List<Person> findByNameIn(Iterable<String> names);
    }

    private static final class CountingGeneratedTable implements GeneratedTable {
        private final GeneratedTable delegate;
        private int scanEqualsLongCalls;
        private int readLongCalls;

        private CountingGeneratedTable(GeneratedTable delegate) {
            this.delegate = delegate;
        }

        void resetCounters() {
            scanEqualsLongCalls = 0;
            readLongCalls = 0;
        }

        int scanEqualsLongCalls() {
            return scanEqualsLongCalls;
        }

        int readLongCalls() {
            return readLongCalls;
        }

        @Override
        public int columnCount() {
            return delegate.columnCount();
        }

        @Override
        public byte typeCodeAt(int columnIndex) {
            return delegate.typeCodeAt(columnIndex);
        }

        @Override
        public long allocatedCount() {
            return delegate.allocatedCount();
        }

        @Override
        public long liveCount() {
            return delegate.liveCount();
        }

        @Override
        public <T> T readWithSeqLock(int rowIndex, java.util.function.Supplier<T> reader) {
            return delegate.readWithSeqLock(rowIndex, reader);
        }

        @Override
        public long lookupById(long id) {
            return delegate.lookupById(id);
        }

        @Override
        public long lookupByIdString(String id) {
            return delegate.lookupByIdString(id);
        }

        @Override
        public void removeById(long id) {
            delegate.removeById(id);
        }

        @Override
        public long insertFrom(Object[] values) {
            return delegate.insertFrom(values);
        }

        @Override
        public void tombstone(long ref) {
            delegate.tombstone(ref);
        }

        @Override
        public boolean isLive(long ref) {
            return delegate.isLive(ref);
        }

        @Override
        public long currentGeneration() {
            return delegate.currentGeneration();
        }

        @Override
        public long rowGeneration(int rowIndex) {
            return delegate.rowGeneration(rowIndex);
        }

        @Override
        public int[] scanEqualsLong(int columnIndex, long value) {
            scanEqualsLongCalls++;
            return delegate.scanEqualsLong(columnIndex, value);
        }

        @Override
        public int[] scanEqualsInt(int columnIndex, int value) {
            return delegate.scanEqualsInt(columnIndex, value);
        }

        @Override
        public int[] scanEqualsString(int columnIndex, String value) {
            return delegate.scanEqualsString(columnIndex, value);
        }

        @Override
        public int[] scanEqualsStringIgnoreCase(int columnIndex, String value) {
            return delegate.scanEqualsStringIgnoreCase(columnIndex, value);
        }

        @Override
        public int[] scanBetweenInt(int columnIndex, int min, int max) {
            return delegate.scanBetweenInt(columnIndex, min, max);
        }

        @Override
        public int[] scanBetweenLong(int columnIndex, long min, long max) {
            return delegate.scanBetweenLong(columnIndex, min, max);
        }

        @Override
        public int[] scanInLong(int columnIndex, long[] values) {
            return delegate.scanInLong(columnIndex, values);
        }

        @Override
        public int[] scanInInt(int columnIndex, int[] values) {
            return delegate.scanInInt(columnIndex, values);
        }

        @Override
        public int[] scanInString(int columnIndex, String[] values) {
            return delegate.scanInString(columnIndex, values);
        }

        @Override
        public int[] scanAll() {
            return delegate.scanAll();
        }

        @Override
        public long readLong(int columnIndex, int rowIndex) {
            readLongCalls++;
            return delegate.readLong(columnIndex, rowIndex);
        }

        @Override
        public int readInt(int columnIndex, int rowIndex) {
            return delegate.readInt(columnIndex, rowIndex);
        }

        @Override
        public String readString(int columnIndex, int rowIndex) {
            return delegate.readString(columnIndex, rowIndex);
        }

        @Override
        public boolean isPresent(int columnIndex, int rowIndex) {
            return delegate.isPresent(columnIndex, rowIndex);
        }
    }
}

