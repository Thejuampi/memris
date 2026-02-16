package io.memris.query;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogicalQueryJoinTest {

    @Test
    void join_helpers() {
        var j = LogicalQuery.Join.inner("customer", String.class, "customer_id");

        assertThat(j.propertyPath()).isEqualTo("customer");
        assertThat(j.targetEntity()).isEqualTo(String.class);
        assertThat(j.joinColumn()).isEqualTo("customer_id");
        assertThat(j.referencedColumn()).isEqualTo("id");
        assertThat(j.joinType()).isEqualTo(LogicalQuery.Join.JoinType.INNER);

        var l = LogicalQuery.Join.left("x", Integer.class, "col");
        assertThat(l.joinType()).isEqualTo(LogicalQuery.Join.JoinType.LEFT);
    }
}
