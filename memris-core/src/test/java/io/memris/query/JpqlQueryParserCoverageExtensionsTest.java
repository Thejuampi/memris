package io.memris.query;

import io.memris.core.Modifying;
import io.memris.core.Param;
import io.memris.core.Query;
import io.memris.core.plan.entities.SimpleEntity;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JpqlQueryParserCoverageExtensionsTest {

    record NameAge(String name, int age) {
    }

    interface CoverageRepository {
        @Query("select count(e) from SimpleEntity e group by e.name having not (count(e) > :min or count(e) < :max)")
        Map<String, Long> countByNameHavingNotRange(@Param("min") long min, @Param("max") long max);

        @Query("select e.name as name, e.age as age from SimpleEntity e")
        List<NameAge> findProjected();

        @Query("select e.name, e.age as age from SimpleEntity e")
        List<NameAge> invalidProjectionMissingAlias();

        @Query("select e.name as name from SimpleEntity e")
        List<SimpleEntity> invalidAliasWithoutProjection();

        @Query("select e.name from SimpleEntity e group by e.name")
        Map<String, Long> invalidGroupingSelect();

        @Query("select e.name as name, e.age as age from SimpleEntity e")
        NameAge invalidDirectRecordProjection();

        @Modifying
        @Query("update SimpleEntity e set e.id = ?1 where e.id = ?2")
        int invalidUpdateId(long id, long current);

        @Query("update SimpleEntity e set e.name = ?1")
        int updateWithoutModifying(String name);
    }

    @Test
    void shouldRejectUnsupportedHavingNegationOnCountPredicates() throws Exception {
        Method method = CoverageRepository.class.getMethod("countByNameHavingNotRange", long.class, long.class);
        assertThatThrownBy(() -> JpqlQueryParser.parse(method, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("HAVING count requires first parameter slot");
    }

    @Test
    void shouldParseRecordProjectionAndRejectInvalidProjectionShapes() throws Exception {
        Method valid = CoverageRepository.class.getMethod("findProjected");
        LogicalQuery projected = JpqlQueryParser.parse(valid, SimpleEntity.class);
        assertThat(projected.projection()).isNotNull();
        assertThat(projected.projection().items()).hasSize(2);

        Method missingAlias = CoverageRepository.class.getMethod("invalidProjectionMissingAlias");
        assertThatThrownBy(() -> JpqlQueryParser.parse(missingAlias, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires aliases");

        Method aliasWithoutProjection = CoverageRepository.class.getMethod("invalidAliasWithoutProjection");
        assertThatThrownBy(() -> JpqlQueryParser.parse(aliasWithoutProjection, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("requires a projection");

        Method directRecord = CoverageRepository.class.getMethod("invalidDirectRecordProjection");
        assertThatThrownBy(() -> JpqlQueryParser.parse(directRecord, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Record projections must be returned as List, Set, or Optional");
    }

    @Test
    void shouldRejectInvalidGroupingAndModifyingConstraints() throws Exception {
        Method invalidGrouping = CoverageRepository.class.getMethod("invalidGroupingSelect");
        assertThatThrownBy(() -> JpqlQueryParser.parse(invalidGrouping, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("select list requires a projection return type");

        Method invalidUpdateId = CoverageRepository.class.getMethod("invalidUpdateId", long.class, long.class);
        assertThatThrownBy(() -> JpqlQueryParser.parse(invalidUpdateId, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot modify ID column");

        Method updateWithoutModifying = CoverageRepository.class.getMethod("updateWithoutModifying", String.class);
        assertThatThrownBy(() -> JpqlQueryParser.parse(updateWithoutModifying, SimpleEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Query update/delete requires @Modifying");
    }
}
