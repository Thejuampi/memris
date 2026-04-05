package io.memris.index;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CompositeKeyMutationCoverageTest {

    @Test
    void minSentinelIsNotNull() {
        assertThat(CompositeKey.minSentinel()).isNotNull();
    }

    @Test
    void maxSentinelIsNotNull() {
        assertThat(CompositeKey.maxSentinel()).isNotNull();
    }

    @Test
    void minAndMaxSentinelsAreDistinct() {
        assertThat(CompositeKey.minSentinel()).isNotSameAs(CompositeKey.maxSentinel());
    }

    @Test
    void compareToReturnsNonZeroForDifferentLengths() {
        var shorter = CompositeKey.of(new Object[]{1});
        var longer = CompositeKey.of(new Object[]{1, 2});
        assertThat(shorter.compareTo(longer)).isNegative();
        assertThat(longer.compareTo(shorter)).isPositive();
    }

    @Test
    void compareToReturnsZeroForIdenticalValues() {
        var a = CompositeKey.of(new Object[]{1, "x"});
        var b = CompositeKey.of(new Object[]{1, "x"});
        assertThat(a.compareTo(b)).isZero();
    }

    @Test
    void equalsReturnsFalseForNull() {
        var key = CompositeKey.of(new Object[]{1});
        assertThat(key.equals(null)).isFalse();
    }

    @Test
    void equalsReturnsFalseForDifferentValues() {
        var a = CompositeKey.of(new Object[]{1});
        var b = CompositeKey.of(new Object[]{2});
        assertThat(a.equals(b)).isFalse();
    }

    @Test
    void hashCodeDiffersForDifferentValues() {
        var a = CompositeKey.of(new Object[]{1});
        var b = CompositeKey.of(new Object[]{2});
        assertThat(a.hashCode()).isNotEqualTo(b.hashCode());
    }

    @Test
    void hashCodeConsistentWithEquals() {
        var a = CompositeKey.of(new Object[]{1, "hello"});
        var b = CompositeKey.of(new Object[]{1, "hello"});
        assertThat(a.equals(b)).isTrue();
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
    }

    @Test
    void minSentinelCompareToLowerThanNormalValue() {
        var min = CompositeKey.of(new Object[]{CompositeKey.minSentinel()});
        var normal = CompositeKey.of(new Object[]{42});
        assertThat(min.compareTo(normal)).isNegative();
    }

    @Test
    void maxSentinelComparesHigherThanNormalValue() {
        var max = CompositeKey.of(new Object[]{CompositeKey.maxSentinel()});
        var normal = CompositeKey.of(new Object[]{42});
        assertThat(max.compareTo(normal)).isPositive();
    }

    @Test
    void sameRefCompareToReturnsZero() {
        var obj = new Object();
        var key = CompositeKey.of(new Object[]{obj});
        assertThat(key.compareTo(key)).isZero();
    }

    @Test
    void equalsReflexive() {
        var key = CompositeKey.of(new Object[]{1});
        assertThat(key.equals(key)).isTrue();
    }
}
