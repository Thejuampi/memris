package io.memris.runtime.dispatch;

import io.memris.index.CompositeKey;
import io.memris.kernel.Predicate;

public record CompositeRangeProbe(
        Predicate.Operator operator,
        CompositeKey lower,
        CompositeKey upper,
        boolean[] consumed) {
}
