package io.memris.query;

import io.memris.core.TypeCodes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryModelCoverageTest {

    @Test
    void compiledQueryFactoryOverloadsAndJoinValidation() {
        var conditions = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.EQ, 0)
        };
        var joins = new CompiledQuery.CompiledJoin[] {
                new CompiledQuery.CompiledJoin(
                        "customer",
                        String.class,
                        String.class,
                        0,
                        0,
                        true,
                        TypeCodes.TYPE_LONG,
                        LogicalQuery.Join.JoinType.INNER,
                        "customer",
                        new CompiledQuery.CompiledJoinPredicate[0])
        };
        var order = new CompiledQuery.CompiledOrderBy[] {
                new CompiledQuery.CompiledOrderBy(0, true)
        };
        var assignments = new CompiledQuery.CompiledUpdateAssignment[] {
                new CompiledQuery.CompiledUpdateAssignment(0, 0)
        };
        var having = new CompiledQuery.CompiledCondition[] {
                CompiledQuery.CompiledCondition.of(0, TypeCodes.TYPE_INT, LogicalQuery.Operator.GT, 0)
        };

        var fromSimpleOverload = CompiledQuery.of(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                conditions,
                joins,
                order,
                10,
                1);
        assertThat(fromSimpleOverload.limit()).isEqualTo(10);
        assertThat(fromSimpleOverload.joins()).hasSize(1);

        var fromFullOverload = CompiledQuery.of(
                OpCode.UPDATE_QUERY,
                LogicalQuery.ReturnKind.MODIFYING_INT,
                conditions,
                assignments,
                null,
                joins,
                order,
                null,
                2,
                true,
                new Object[] { "bound" },
                new int[] { 0 },
                1,
                having);
        assertThat(fromFullOverload.havingConditions()).hasSize(1);
        assertThat(fromFullOverload.distinct()).isTrue();

        var withoutHaving = CompiledQuery.of(
                OpCode.UPDATE_QUERY,
                LogicalQuery.ReturnKind.MODIFYING_INT,
                conditions,
                assignments,
                null,
                joins,
                order,
                null,
                2,
                false,
                new Object[0],
                new int[0],
                1);
        assertThat(withoutHaving.havingConditions()).isNull();

        var replacedJoins = fromSimpleOverload.withJoins(new CompiledQuery.CompiledJoin[0]);
        assertThat(replacedJoins.joins()).isEmpty();

        assertThatThrownBy(() -> new CompiledQuery.CompiledJoin(
                " ",
                String.class,
                String.class,
                0,
                0,
                true,
                TypeCodes.TYPE_LONG,
                LogicalQuery.Join.JoinType.INNER,
                "x",
                new CompiledQuery.CompiledJoinPredicate[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("joinPath required");
    }

    @Test
    void logicalQueryFactoriesHashCodeAndOrderByHelpers() {
        var conditions = new LogicalQuery.Condition[] {
                LogicalQuery.Condition.of("age", LogicalQuery.Operator.GTE, 0)
        };
        var joins = new LogicalQuery.Join[] {
                LogicalQuery.Join.left("customer", String.class, "customerId")
        };
        var orderBy = new LogicalQuery.OrderBy[] {
                LogicalQuery.OrderBy.asc("name")
        };

        var queryA = LogicalQuery.of(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                conditions,
                joins,
                orderBy,
                1);
        var queryB = LogicalQuery.of(
                OpCode.FIND,
                LogicalQuery.ReturnKind.MANY_LIST,
                conditions,
                joins,
                orderBy,
                1);

        assertThat(queryA.hashCode()).isEqualTo(queryB.hashCode());
        assertThat(LogicalQuery.OrderBy.asc("createdAt").ascending()).isTrue();
        assertThat(LogicalQuery.OrderBy.desc("createdAt").ascending()).isFalse();
    }

    @Test
    void methodKeyMatchesOverloadsAndIdMarker() throws Exception {
        Method byString = SampleRepository.class.getMethod("findById", String.class);
        Method byInt = SampleRepository.class.getMethod("findById", int.class);
        Method save = SampleRepository.class.getMethod("save", Object.class);

        var fromMethod = MethodKey.of(byString);
        assertThat(fromMethod.methodName()).isEqualTo("findById");
        assertThat(fromMethod.matches(byString)).isTrue();

        var idKey = new MethodKey("findById", List.of(IdParam.class));
        assertThat(idKey.matches(byString)).isTrue();
        assertThat(idKey.matches(byInt)).isFalse();

        var saveKey = new MethodKey("save", List.of(Object.class));
        assertThat(saveKey.matches(save)).isTrue();
        assertThat(saveKey.matches(byString)).isFalse();
    }

    @Test
    void queryMethodLexerEntityMetadataHelpers() throws Exception {
        Method extractor = QueryMethodLexer.class.getDeclaredMethod("extractEntityMetadata", Class.class);
        extractor.setAccessible(true);
        Object metadata = extractor.invoke(null, LexerEntity.class);

        Method getField = metadata.getClass().getDeclaredMethod("getField", String.class);
        Method hasField = metadata.getClass().getDeclaredMethod("hasField", String.class);
        getField.setAccessible(true);
        hasField.setAccessible(true);

        Object field = getField.invoke(metadata, "FIRSTNAME");
        assertThat(field).isNotNull();
        assertThat(((java.lang.reflect.Field) field).getName()).isEqualTo("firstName");
        assertThat((boolean) hasField.invoke(metadata, "firstName")).isTrue();
        assertThat((boolean) hasField.invoke(metadata, "missing")).isFalse();
    }

    interface SampleRepository {
        void findById(String id);
        void findById(int id);
        void save(Object entity);
    }

    static final class LexerEntity {
        String firstName;
    }
}
